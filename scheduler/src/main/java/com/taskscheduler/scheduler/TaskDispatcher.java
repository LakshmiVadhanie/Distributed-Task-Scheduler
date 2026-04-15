package com.taskscheduler.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskscheduler.proto.ExecuteTaskRequest;
import com.taskscheduler.proto.ExecuteTaskResponse;
import com.taskscheduler.proto.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.InetSocketAddress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core dispatcher: only runs when this scheduler instance is the leader.
 *
 * Loop:
 *  1. Pop a task ID from Redis priority queues (high -> normal -> low)
 *  2. Load task details from Redis hash
 *  3. Use LoadBalancer to pick a worker
 *  4. Call worker gRPC ExecuteTask (async)
 *  5. On completion: update task status and counters in Redis
 *  6. Retry failed tasks up to MAX_RETRIES times
 */
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    // Redis queue keys (LPUSH to enqueue, RPOP to dequeue for FIFO)
    static final String QUEUE_HIGH   = "tasks:queue:high";
    static final String QUEUE_NORMAL = "tasks:queue:normal";
    static final String QUEUE_LOW    = "tasks:queue:low";
    static final String TASK_PREFIX  = "task:";

    private static final int    MAX_RETRIES        = 3;
    private static final int    POLL_INTERVAL_MS   = 200;
    private static final int    DISPATCH_THREADS   = 8;

    private final JedisPool      jedisPool;
    private final LoadBalancer   loadBalancer;
    private final LeaderElection leaderElection;
    private final String         schedulerId;
    private final ObjectMapper   mapper      = new ObjectMapper();
    private final AtomicBoolean  running     = new AtomicBoolean(false);
    private final AtomicLong     dispatched  = new AtomicLong(0);
    private final AtomicLong     completed   = new AtomicLong(0);
    private final AtomicLong     failed      = new AtomicLong(0);

    private final ExecutorService dispatchPool = Executors.newFixedThreadPool(DISPATCH_THREADS);

    // gRPC channel cache: workerId -> ManagedChannel
    private final ConcurrentHashMap<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();

    private Thread pollerThread;

    public TaskDispatcher(JedisPool jedisPool, LoadBalancer loadBalancer,
                          LeaderElection leaderElection, String schedulerId) {
        this.jedisPool      = jedisPool;
        this.loadBalancer   = loadBalancer;
        this.leaderElection = leaderElection;
        this.schedulerId    = schedulerId;
    }

    public void start() {
        running.set(true);
        pollerThread = new Thread(this::pollLoop, "task-dispatcher");
        pollerThread.setDaemon(true);
        pollerThread.start();
        log.info("Task dispatcher started (threads={})", DISPATCH_THREADS);
    }

    public void stop() {
        running.set(false);
        if (pollerThread != null) pollerThread.interrupt();
        dispatchPool.shutdown();
        channelCache.values().forEach(ManagedChannel::shutdown);
        log.info("Task dispatcher stopped. dispatched={} completed={} failed={}",
                dispatched.get(), completed.get(), failed.get());
    }

    // ── Polling loop ──────────────────────────────────────────────────────

    private void pollLoop() {
        while (running.get()) {
            try {
                if (!leaderElection.isLeader()) {
                    Thread.sleep(1000); // Non-leader: stand by
                    continue;
                }

                String taskId = dequeueTask();
                if (taskId == null) {
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }

                Map<String, String> task = loadTask(taskId);
                if (task == null || task.isEmpty()) {
                    log.warn("Task {} not found in Redis, skipping", taskId);
                    continue;
                }

                dispatchPool.submit(() -> dispatchTask(task));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error in dispatch loop", e);
            }
        }
    }

    // ── Task dequeue (priority: high > normal > low) ──────────────────────

    private String dequeueTask() {
        try (Jedis jedis = jedisPool.getResource()) {
            // Try each queue in priority order
            for (String queue : List.of(QUEUE_HIGH, QUEUE_NORMAL, QUEUE_LOW)) {
                String taskId = jedis.rpop(queue);
                if (taskId != null) return taskId;
            }
        } catch (Exception e) {
            log.error("Error dequeueing task", e);
        }
        return null;
    }

    // ── Task dispatch ─────────────────────────────────────────────────────

    private void dispatchTask(Map<String, String> task) {
        String taskId   = task.get("taskId");
        String taskType = task.getOrDefault("taskType", "generic");
        String payload  = task.getOrDefault("payload", "{}");
        int    priority = Integer.parseInt(task.getOrDefault("priority", "5"));
        long   timeout  = Long.parseLong(task.getOrDefault("timeoutMs", "30000"));
        int    retries  = Integer.parseInt(task.getOrDefault("retryCount", "0"));

        Optional<WorkerInfo> workerOpt = loadBalancer.selectWorker();
        if (workerOpt.isEmpty()) {
            log.warn("No worker available for task {}, re-queuing", taskId);
            requeueTask(taskId, priority);
            return;
        }

        WorkerInfo worker = workerOpt.get();
        updateTaskStatus(taskId, "RUNNING", worker.getWorkerId(), null, null);
        dispatched.incrementAndGet();
        log.info("Dispatching task {} to worker {}", taskId, worker.getWorkerId());

        try {
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = getWorkerStub(worker);

            ExecuteTaskRequest request = ExecuteTaskRequest.newBuilder()
                    .setTaskId(taskId)
                    .setTaskType(taskType)
                    .setPayload(payload)
                    .setPriority(priority)
                    .setTimeoutMs(timeout)
                    .build();

            ExecuteTaskResponse response = stub
                    .withDeadlineAfter(timeout + 2000, TimeUnit.MILLISECONDS)
                    .executeTask(request);

            handleResponse(taskId, response, worker, retries);

        } catch (StatusRuntimeException e) {
            log.error("gRPC error dispatching task {} to worker {}: {}", taskId, worker.getWorkerId(), e.getStatus());
            handleDispatchFailure(task, worker.getWorkerId(), e.getMessage(), retries);
        } catch (Exception e) {
            log.error("Unexpected error dispatching task {}", taskId, e);
            handleDispatchFailure(task, worker.getWorkerId(), e.getMessage(), retries);
        }
    }

    // ── Response handling ─────────────────────────────────────────────────

    private void handleResponse(String taskId, ExecuteTaskResponse response,
                                WorkerInfo worker, int retries) {
        switch (response.getStatus()) {
            case COMPLETED -> {
                updateTaskStatus(taskId, "COMPLETED", worker.getWorkerId(),
                        response.getResult(), null);
                completed.incrementAndGet();
                recordMetrics(true, response.getExecutionTimeMs());
                log.info("Task {} COMPLETED in {}ms", taskId, response.getExecutionTimeMs());
            }
            case FAILED -> {
                if (retries < MAX_RETRIES) {
                    log.warn("Task {} FAILED (attempt {}/{}), retrying", taskId, retries + 1, MAX_RETRIES);
                    updateTaskStatus(taskId, "RETRYING", worker.getWorkerId(), null,
                            response.getErrorMessage());
                    requeueTaskWithRetry(taskId, retries + 1);
                } else {
                    updateTaskStatus(taskId, "FAILED", worker.getWorkerId(),
                            null, response.getErrorMessage());
                    failed.incrementAndGet();
                    recordMetrics(false, response.getExecutionTimeMs());
                    log.error("Task {} PERMANENTLY FAILED after {} retries", taskId, MAX_RETRIES);
                }
            }
            default -> log.warn("Task {} returned unexpected status: {}", taskId, response.getStatus());
        }
    }

    private void handleDispatchFailure(Map<String, String> task, String workerId,
                                       String error, int retries) {
        String taskId = task.get("taskId");
        int    pri    = Integer.parseInt(task.getOrDefault("priority", "5"));

        if (retries < MAX_RETRIES) {
            updateTaskStatus(taskId, "RETRYING", workerId, null, error);
            requeueTaskWithRetry(taskId, retries + 1);
        } else {
            updateTaskStatus(taskId, "FAILED", workerId, null, error);
            failed.incrementAndGet();
        }
    }

    // ── Redis helpers ─────────────────────────────────────────────────────

    private Map<String, String> loadTask(String taskId) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(TASK_PREFIX + taskId);
        } catch (Exception e) {
            log.error("Error loading task {}", taskId, e);
            return null;
        }
    }

    private void updateTaskStatus(String taskId, String status, String workerId,
                                  String result, String error) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields = new HashMap<>();
            fields.put("status",      status);
            fields.put("workerId",    workerId != null ? workerId : "");
            fields.put("updatedAt",   String.valueOf(System.currentTimeMillis()));
            if (result != null) fields.put("result", result);
            if (error  != null) fields.put("errorMessage", error);
            if ("RUNNING".equals(status))   fields.put("startedAt",   String.valueOf(System.currentTimeMillis()));
            if ("COMPLETED".equals(status) || "FAILED".equals(status))
                                             fields.put("completedAt", String.valueOf(System.currentTimeMillis()));
            jedis.hset(TASK_PREFIX + taskId, fields);
        } catch (Exception e) {
            log.error("Error updating task status", e);
        }
    }

    private void requeueTask(String taskId, int priority) {
        String queue = priority >= 8 ? QUEUE_HIGH : priority >= 4 ? QUEUE_NORMAL : QUEUE_LOW;
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.lpush(queue, taskId);
        } catch (Exception e) {
            log.error("Error re-queuing task {}", taskId, e);
        }
    }

    private void requeueTaskWithRetry(String taskId, int newRetryCount) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(TASK_PREFIX + taskId, "retryCount", String.valueOf(newRetryCount));
            jedis.lpush(QUEUE_NORMAL, taskId);
        } catch (Exception e) {
            log.error("Error re-queuing task {} for retry", taskId, e);
        }
    }

    private void recordMetrics(boolean success, long execTimeMs) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (success) jedis.incr("metrics:tasks_completed");
            else         jedis.incr("metrics:tasks_failed");
            jedis.lpush("metrics:exec_times", String.valueOf(execTimeMs));
            jedis.ltrim("metrics:exec_times", 0, 999); // keep last 1000 samples
        } catch (Exception ignored) {}
    }

    // ── gRPC channel management ───────────────────────────────────────────

    private WorkerServiceGrpc.WorkerServiceBlockingStub getWorkerStub(WorkerInfo worker) {
        ManagedChannel channel = channelCache.computeIfAbsent(worker.getWorkerId(), id ->
                NettyChannelBuilder
                        .forAddress(new InetSocketAddress(worker.getHost(), worker.getPort()))
                        .usePlaintext()
                        .build()
        );
        return WorkerServiceGrpc.newBlockingStub(channel);
    }
}
