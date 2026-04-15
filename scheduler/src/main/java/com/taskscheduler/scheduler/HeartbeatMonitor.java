package com.taskscheduler.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background thread that periodically checks worker heartbeat timestamps.
 * Workers that have not sent a heartbeat within the timeout window are marked
 * as OFFLINE and removed from the pool, preventing the scheduler from
 * dispatching tasks to dead nodes.
 *
 * Heartbeat timeout: 15 seconds
 * Check interval:    5 seconds
 */
public class HeartbeatMonitor {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatMonitor.class);

    public static final long HEARTBEAT_TIMEOUT_MS = 15_000L; // 15s
    private static final int CHECK_INTERVAL_SEC   = 5;

    private final WorkerRegistry workerRegistry;
    private final JedisPool      jedisPool;
    private final AtomicBoolean  running = new AtomicBoolean(false);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "heartbeat-monitor"); t.setDaemon(true); return t; }
    );

    public HeartbeatMonitor(WorkerRegistry workerRegistry, JedisPool jedisPool) {
        this.workerRegistry = workerRegistry;
        this.jedisPool       = jedisPool;
    }

    public void start() {
        running.set(true);
        executor.scheduleAtFixedRate(this::checkHeartbeats, CHECK_INTERVAL_SEC, CHECK_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("Heartbeat monitor started (timeout={}ms, interval={}s)", HEARTBEAT_TIMEOUT_MS, CHECK_INTERVAL_SEC);
    }

    public void stop() {
        running.set(false);
        executor.shutdownNow();
        log.info("Heartbeat monitor stopped");
    }

    // ── Core check ────────────────────────────────────────────────────────

    private void checkHeartbeats() {
        if (!running.get()) return;
        try {
            int before = workerRegistry.size();
            workerRegistry.evictStaleWorkers(HEARTBEAT_TIMEOUT_MS);
            int after = workerRegistry.size();

            if (before != after) {
                log.warn("Evicted {} stale worker(s). Active workers: {}", before - after, after);
                recordEvictionMetric(before - after);
            } else {
                log.debug("Heartbeat check OK. Active workers: {}", after);
            }
        } catch (Exception e) {
            log.error("Error during heartbeat check", e);
        }
    }

    /** Write eviction events to Redis for dashboard visibility. */
    private void recordEvictionMetric(int count) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.incrBy("metrics:worker_evictions", count);
        } catch (Exception ignored) {}
    }
}
