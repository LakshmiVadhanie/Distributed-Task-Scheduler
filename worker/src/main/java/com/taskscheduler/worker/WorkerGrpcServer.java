package com.taskscheduler.worker;

import com.taskscheduler.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker's gRPC server.
 *
 * Exposes WorkerService (called by the scheduler):
 *   - ExecuteTask: run a task and return the result
 *   - GetWorkerStatus: return current capacity info
 */
public class WorkerGrpcServer {

    private static final Logger log = LoggerFactory.getLogger(WorkerGrpcServer.class);

    private final int          port;
    private final String       workerId;
    private final TaskExecutor taskExecutor;
    private final JedisPool    jedisPool;

    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private       Server        server;

    // Thread pool for non-blocking task execution inside the gRPC handler
    private final ExecutorService taskPool;

    public WorkerGrpcServer(int port, String workerId, TaskExecutor taskExecutor, JedisPool jedisPool) {
        this.port         = port;
        this.workerId     = workerId;
        this.taskExecutor = taskExecutor;
        this.jedisPool    = jedisPool;
        // Separate pool so gRPC Netty threads are not blocked
        this.taskPool     = Executors.newFixedThreadPool(
                Integer.parseInt(System.getenv().getOrDefault("CAPACITY", "10"))
        );
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new WorkerServiceImpl())
                .build()
                .start();
        log.info("Worker gRPC server started on port {}", port);
    }

    public void stop() {
        if (server != null) server.shutdown();
        taskPool.shutdown();
    }

    public int    getActiveTasks() { return activeTasks.get(); }
    public double getCpuLoad()     {
        return ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage() * 10;
    }

    // ── WorkerService implementation ──────────────────────────────────────

    private class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

        @Override
        public void executeTask(ExecuteTaskRequest request,
                                StreamObserver<ExecuteTaskResponse> responseObserver) {
            String taskId = request.getTaskId();
            log.info("Executing task {} type={}", taskId, request.getTaskType());

            activeTasks.incrementAndGet();
            long startTime = System.currentTimeMillis();

            // Run in task pool to avoid blocking gRPC I/O threads
            taskPool.submit(() -> {
                ExecuteTaskResponse.Builder builder = ExecuteTaskResponse.newBuilder()
                        .setTaskId(taskId);
                try {
                    String result = taskExecutor.execute(
                            request.getTaskType(),
                            request.getPayload(),
                            request.getTimeoutMs()
                    );
                    long elapsed = System.currentTimeMillis() - startTime;

                    builder.setStatus(TaskStatus.COMPLETED)
                           .setResult(result)
                           .setExecutionTimeMs(elapsed);

                    // Write result directly to Redis (Scheduler also polls this)
                    persistResult(taskId, "COMPLETED", result, null);
                    log.info("Task {} COMPLETED in {}ms", taskId, elapsed);

                } catch (Exception e) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    log.error("Task {} FAILED: {}", taskId, e.getMessage());

                    builder.setStatus(TaskStatus.FAILED)
                           .setErrorMessage(e.getMessage())
                           .setExecutionTimeMs(elapsed);

                    persistResult(taskId, "FAILED", null, e.getMessage());
                } finally {
                    activeTasks.decrementAndGet();
                }

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            });
        }

        @Override
        public void getWorkerStatus(WorkerStatusRequest request,
                                    StreamObserver<WorkerStatusResponse> responseObserver) {
            int capacity = Integer.parseInt(System.getenv().getOrDefault("CAPACITY", "10"));
            int active   = activeTasks.get();

            WorkerStatus status;
            if (active >= capacity)       status = WorkerStatus.OVERLOADED;
            else if (active > 0)          status = WorkerStatus.BUSY;
            else                          status = WorkerStatus.IDLE;

            WorkerStatusResponse response = WorkerStatusResponse.newBuilder()
                    .setWorkerId(workerId)
                    .setStatus(status)
                    .setActiveTasks(active)
                    .setCapacity(capacity)
                    .setCpuLoad(getCpuLoad())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    // ── Result persistence ────────────────────────────────────────────────

    private void persistResult(String taskId, String status, String result, String error) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields = new HashMap<>();
            fields.put("status",      status);
            fields.put("workerId",    workerId);
            fields.put("completedAt", String.valueOf(System.currentTimeMillis()));
            if (result != null) fields.put("result", result);
            if (error  != null) fields.put("errorMessage", error);
            jedis.hset("task:" + taskId, fields);
        } catch (Exception e) {
            log.error("Failed to persist result for task {} to Redis", taskId, e);
        }
    }
}
