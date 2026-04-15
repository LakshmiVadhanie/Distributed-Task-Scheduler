package com.taskscheduler.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of all known worker nodes.
 * In-memory map is the source of truth during runtime;
 * Redis is used for cross-scheduler visibility (e.g., during failover).
 */
public class WorkerRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);

    private static final String REDIS_WORKER_PREFIX  = "worker:";
    private static final String REDIS_WORKERS_ACTIVE = "workers:active";

    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final JedisPool jedisPool;
    private final ObjectMapper mapper = new ObjectMapper();

    public WorkerRegistry(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // ── Register ──────────────────────────────────────────────────────────

    public void register(WorkerInfo info) {
        workers.put(info.getWorkerId(), info);
        persistToRedis(info);
        log.info("Worker registered: {}", info);
    }

    // ── Update heartbeat ──────────────────────────────────────────────────

    public void updateHeartbeat(String workerId, int activeTasks, double cpuLoad, double memoryUsage) {
        WorkerInfo info = workers.get(workerId);
        if (info == null) {
            log.warn("Heartbeat from unknown worker {}", workerId);
            return;
        }
        info.setActiveTasks(activeTasks);
        info.setCpuLoad(cpuLoad);
        info.setMemoryUsage(memoryUsage);
        info.setLastHeartbeat(System.currentTimeMillis());

        // Derive status from load
        if (activeTasks >= info.getCapacity()) {
            info.setStatus(WorkerInfo.Status.OVERLOADED);
        } else if (activeTasks > 0) {
            info.setStatus(WorkerInfo.Status.BUSY);
        } else {
            info.setStatus(WorkerInfo.Status.IDLE);
        }

        persistToRedis(info);
    }

    // ── Evict stale workers ───────────────────────────────────────────────

    public void evictStaleWorkers(long heartbeatTimeoutMs) {
        workers.values().removeIf(w -> {
            if (w.isStale(heartbeatTimeoutMs)) {
                log.warn("Evicting stale worker: {}", w.getWorkerId());
                w.setStatus(WorkerInfo.Status.OFFLINE);
                removeFromRedis(w.getWorkerId());
                return true;
            }
            return false;
        });
    }

    // ── Queries ───────────────────────────────────────────────────────────

    public WorkerInfo getWorker(String workerId) {
        return workers.get(workerId);
    }

    public Collection<WorkerInfo> getAllWorkers() {
        return workers.values();
    }

    public List<WorkerInfo> getAvailableWorkers() {
        List<WorkerInfo> available = new ArrayList<>();
        for (WorkerInfo w : workers.values()) {
            if (w.isAvailable()) available.add(w);
        }
        return available;
    }

    public int size() { return workers.size(); }

    // ── Task counters ─────────────────────────────────────────────────────

    public void incrementActiveTasks(String workerId) {
        WorkerInfo w = workers.get(workerId);
        if (w != null) w.setActiveTasks(w.getActiveTasks() + 1);
    }

    public void decrementActiveTasks(String workerId) {
        WorkerInfo w = workers.get(workerId);
        if (w != null) w.setActiveTasks(Math.max(0, w.getActiveTasks() - 1));
    }

    // ── Redis persistence ─────────────────────────────────────────────────

    private void persistToRedis(WorkerInfo info) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = mapper.writeValueAsString(info);
            jedis.set(REDIS_WORKER_PREFIX + info.getWorkerId(), json);
            jedis.sadd(REDIS_WORKERS_ACTIVE, info.getWorkerId());
        } catch (Exception e) {
            log.error("Failed to persist worker {} to Redis", info.getWorkerId(), e);
        }
    }

    private void removeFromRedis(String workerId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(REDIS_WORKER_PREFIX + workerId);
            jedis.srem(REDIS_WORKERS_ACTIVE, workerId);
        } catch (Exception e) {
            log.error("Failed to remove worker {} from Redis", workerId, e);
        }
    }
}
