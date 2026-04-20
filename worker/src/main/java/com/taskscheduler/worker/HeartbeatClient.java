package com.taskscheduler.worker;

import com.taskscheduler.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.DoubleSupplier;

/**
 * Sends periodic heartbeats and registers the worker with the scheduler.
 *
 * Registration is retried with exponential backoff until it succeeds.
 * Heartbeats are sent every 5 seconds (well within the scheduler's 15s timeout).
 */
public class HeartbeatClient {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatClient.class);

    private static final int HEARTBEAT_INTERVAL_SEC = 5;
    private static final int MAX_REGISTER_RETRIES   = 10;

    private final String        workerId;
    private final String        workerHost;
    private final int           workerPort;
    private final int           capacity;
    private final IntSupplier   activeTasksSupplier;
    private final DoubleSupplier cpuLoadSupplier;

    private final ManagedChannel                         channel;
    private final HeartbeatServiceGrpc.HeartbeatServiceBlockingStub stub;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "heartbeat-client"); t.setDaemon(true); return t; }
    );

    public HeartbeatClient(String workerId, String workerHost, int workerPort, int capacity,
                            String schedulerHost, int schedulerPort,
                            IntSupplier activeTasksSupplier, DoubleSupplier cpuLoadSupplier) {
        this.workerId            = workerId;
        this.workerHost          = workerHost;
        this.workerPort          = workerPort;
        this.capacity            = capacity;
        this.activeTasksSupplier = activeTasksSupplier;
        this.cpuLoadSupplier     = cpuLoadSupplier;

        this.channel = NettyChannelBuilder
                .forAddress(new InetSocketAddress(schedulerHost, schedulerPort))
                .usePlaintext()
                .build();
        this.stub = HeartbeatServiceGrpc.newBlockingStub(channel);

        log.info("HeartbeatClient initialized -> {}:{}", schedulerHost, schedulerPort);
    }

    /** Register with scheduler (with retries), then start heartbeat loop. */
    public void registerAndStartBeating() {
        running.set(true);

        // Register in background with exponential backoff
        Thread registerThread = new Thread(this::registerWithRetry, "worker-register");
        registerThread.setDaemon(true);
        registerThread.start();
    }

    public void stop() {
        running.set(false);
        scheduler.shutdownNow();
        channel.shutdown();
        log.info("HeartbeatClient stopped for worker {}", workerId);
    }

    // ── Registration ──────────────────────────────────────────────────────

    private void registerWithRetry() {
        int attempt = 0;
        while (running.get() && attempt < MAX_REGISTER_RETRIES) {
            try {
                RegisterRequest req = RegisterRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setHost(workerHost)
                        .setPort(workerPort)
                        .setCapacity(capacity)
                        .build();

                RegisterResponse res = stub
                        .withDeadlineAfter(5, TimeUnit.SECONDS)
                        .register(req);

                if (res.getSuccess()) {
                    log.info("Worker {} registered with scheduler (id={})", workerId, res.getSchedulerId());
                    startHeartbeatLoop();
                    return;
                }

            } catch (StatusRuntimeException e) {
                attempt++;
                long backoffMs = (long) Math.min(1000 * Math.pow(2, attempt), 30_000);
                log.warn("Registration attempt {}/{} failed: {}. Retrying in {}ms",
                        attempt, MAX_REGISTER_RETRIES, e.getStatus().getCode(), backoffMs);
                try { TimeUnit.MILLISECONDS.sleep(backoffMs); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        log.error("Worker {} failed to register after {} attempts", workerId, MAX_REGISTER_RETRIES);
    }

    // ── Heartbeat loop ────────────────────────────────────────────────────

    private void startHeartbeatLoop() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("Heartbeat loop started (interval={}s)", HEARTBEAT_INTERVAL_SEC);
    }

    private void sendHeartbeat() {
        if (!running.get()) return;
        try {
            HeartbeatRequest req = HeartbeatRequest.newBuilder()
                    .setWorkerId(workerId)
                    .setActiveTasks(activeTasksSupplier.getAsInt())
                    .setCpuLoad(cpuLoadSupplier.getAsDouble())
                    .setMemoryUsage(getMemoryUsagePercent())
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            HeartbeatResponse res = stub
                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                    .sendHeartbeat(req);

            log.debug("Heartbeat sent | acknowledged={} schedulerIsLeader={}",
                    res.getAcknowledged(), res.getIsLeader());

        } catch (StatusRuntimeException e) {
            log.warn("Heartbeat failed ({}). Scheduler may have restarted.", e.getStatus().getCode());
        }
    }

    // ── System metrics ────────────────────────────────────────────────────

    private double getMemoryUsagePercent() {
        Runtime rt = Runtime.getRuntime();
        long used  = rt.totalMemory() - rt.freeMemory();
        return (double) used / rt.maxMemory() * 100.0;
    }
}
