package com.taskscheduler.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Entry point for the Scheduler (master) node.
 *
 * Responsibilities:
 *  1. Connect to Redis
 *  2. Start gRPC server to receive worker heartbeats / registrations
 *  3. Run leader election via Redis SET NX
 *  4. If elected leader: poll task queue and dispatch tasks to workers
 *  5. Monitor worker heartbeats and evict dead workers
 */
public class SchedulerApplication {

    private static final Logger log = LoggerFactory.getLogger(SchedulerApplication.class);

    public static void main(String[] args) throws Exception {
        // ── Configuration (read from env vars, fall back to defaults) ──
        String redisHost    = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int    redisPort    = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        int    grpcPort     = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT",  "9090"));
        String schedulerId  = System.getenv().getOrDefault("SCHEDULER_ID", "scheduler-" + System.currentTimeMillis());

        log.info("Starting Scheduler [id={}] grpcPort={} redis={}:{}", schedulerId, grpcPort, redisHost, redisPort);

        // ── Redis connection pool ──
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(50);
        poolConfig.setMaxIdle(20);
        poolConfig.setMinIdle(5);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setMaxWait(Duration.ofSeconds(5));

        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 5000);
        log.info("Connected to Redis at {}:{}", redisHost, redisPort);

        // ── Shared components ──
        WorkerRegistry   workerRegistry  = new WorkerRegistry(jedisPool);
        LoadBalancer     loadBalancer    = new LoadBalancer(workerRegistry);
        LeaderElection   leaderElection  = new LeaderElection(jedisPool, schedulerId);
        TaskDispatcher   taskDispatcher  = new TaskDispatcher(jedisPool, loadBalancer, leaderElection, schedulerId);
        HeartbeatMonitor hbMonitor       = new HeartbeatMonitor(workerRegistry, jedisPool);

        // ── gRPC server (receives Register + Heartbeat from workers) ──
        SchedulerGrpcServer grpcServer = new SchedulerGrpcServer(grpcPort, workerRegistry, leaderElection, schedulerId);
        grpcServer.start();

        // ── Background threads ──
        leaderElection.start();
        hbMonitor.start();
        taskDispatcher.start();

        log.info("Scheduler fully initialized. Waiting for tasks...");

        // Keep main thread alive; shutdown gracefully on SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            taskDispatcher.stop();
            hbMonitor.stop();
            leaderElection.stop();
            grpcServer.stop();
            jedisPool.close();
            log.info("Scheduler shut down cleanly");
        }));

        Thread.currentThread().join();
    }
}
