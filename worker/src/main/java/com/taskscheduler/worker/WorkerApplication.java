package com.taskscheduler.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Entry point for a Worker node.
 *
 * Configuration via environment variables:
 *   WORKER_ID       - unique ID for this worker (default: worker-<hostname>)
 *   WORKER_HOST     - advertised hostname/IP for the scheduler to reach us
 *   WORKER_PORT     - gRPC port this worker listens on (default: 9091)
 *   CAPACITY        - max concurrent tasks (default: 10)
 *   SCHEDULER_HOST  - scheduler hostname (default: scheduler)
 *   SCHEDULER_PORT  - scheduler gRPC port (default: 9090)
 *   REDIS_HOST      - Redis hostname (default: redis)
 *   REDIS_PORT      - Redis port (default: 6379)
 */
public class WorkerApplication {

    private static final Logger log = LoggerFactory.getLogger(WorkerApplication.class);

    public static void main(String[] args) throws Exception {
        String hostname      = java.net.InetAddress.getLocalHost().getHostName();
        String workerId      = System.getenv().getOrDefault("WORKER_ID",      "worker-" + hostname);
        String workerHost    = System.getenv().getOrDefault("WORKER_HOST",    hostname);
        int    workerPort    = Integer.parseInt(System.getenv().getOrDefault("WORKER_PORT",    "9091"));
        int    capacity      = Integer.parseInt(System.getenv().getOrDefault("CAPACITY",       "10"));
        String schedulerHost = System.getenv().getOrDefault("SCHEDULER_HOST", "scheduler");
        int    schedulerPort = Integer.parseInt(System.getenv().getOrDefault("SCHEDULER_PORT", "9090"));
        String redisHost     = System.getenv().getOrDefault("REDIS_HOST",     "redis");
        int    redisPort     = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT",     "6379"));

        log.info("Starting Worker [id={} addr={}:{} capacity={} scheduler={}:{}]",
                workerId, workerHost, workerPort, capacity, schedulerHost, schedulerPort);

        // ── Redis (worker writes task results directly) ──
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setMaxWait(Duration.ofSeconds(5));
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 5000);

        // ── Core components ──
        TaskExecutor    executor        = new TaskExecutor();
        WorkerGrpcServer grpcServer     = new WorkerGrpcServer(workerPort, workerId, executor, jedisPool);
        HeartbeatClient  heartbeatClient = new HeartbeatClient(
                workerId, workerHost, workerPort, capacity, schedulerHost, schedulerPort,
                grpcServer::getActiveTasks, grpcServer::getCpuLoad
        );

        // ── Start everything ──
        grpcServer.start();
        heartbeatClient.registerAndStartBeating();

        log.info("Worker {} fully initialized. Ready for tasks.", workerId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Worker {} shutting down", workerId);
            heartbeatClient.stop();
            grpcServer.stop();
            jedisPool.close();
            log.info("Worker {} shut down cleanly", workerId);
        }));

        Thread.currentThread().join();
    }
}
