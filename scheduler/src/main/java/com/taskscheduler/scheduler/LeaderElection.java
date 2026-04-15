package com.taskscheduler.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Distributed leader election using Redis SET NX with TTL.
 *
 * Algorithm:
 *  - Each scheduler node tries SET scheduler:leader <id> NX EX <TTL> periodically.
 *  - The node that wins the SET becomes leader and must refresh (GETEX) before TTL expires.
 *  - If the leader crashes, the key expires, and another node wins the next election round.
 *  - Only the leader runs the task dispatcher loop.
 */
public class LeaderElection {

    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

    private static final String LEADER_KEY      = "scheduler:leader";
    private static final int    LEASE_TTL_SEC   = 10;   // key TTL in seconds
    private static final int    REFRESH_INTERVAL_SEC = 4; // refresh every N seconds (< TTL/2)
    private static final int    RETRY_INTERVAL_SEC   = 3; // non-leaders retry every N seconds

    private final JedisPool  jedisPool;
    private final String     schedulerId;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "leader-election"); t.setDaemon(true); return t; }
    );

    public LeaderElection(JedisPool jedisPool, String schedulerId) {
        this.jedisPool   = jedisPool;
        this.schedulerId = schedulerId;
    }

    public void start() {
        // Try to acquire immediately, then periodically
        executor.scheduleAtFixedRate(this::tryAcquireOrRefresh, 0, RETRY_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("Leader election started for node {}", schedulerId);
    }

    public void stop() {
        executor.shutdownNow();
        // Release leadership if held
        if (isLeader.get()) {
            try (Jedis jedis = jedisPool.getResource()) {
                String current = jedis.get(LEADER_KEY);
                if (schedulerId.equals(current)) {
                    jedis.del(LEADER_KEY);
                    log.info("Released leadership for {}", schedulerId);
                }
            } catch (Exception e) {
                log.error("Error releasing leadership", e);
            }
        }
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    public String getCurrentLeader() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(LEADER_KEY);
        } catch (Exception e) {
            log.error("Error reading leader key", e);
            return null;
        }
    }

    // ── Core election logic ───────────────────────────────────────────────

    private void tryAcquireOrRefresh() {
        try (Jedis jedis = jedisPool.getResource()) {
            if (isLeader.get()) {
                refreshLease(jedis);
            } else {
                tryAcquire(jedis);
            }
        } catch (Exception e) {
            log.error("Leader election error", e);
            isLeader.set(false);
        }
    }

    /**
     * Attempt to acquire the leader key using SET NX EX (atomic).
     */
    private void tryAcquire(Jedis jedis) {
        SetParams params = SetParams.setParams().nx().ex(LEASE_TTL_SEC);
        String result = jedis.set(LEADER_KEY, schedulerId, params);
        if ("OK".equals(result)) {
            isLeader.set(true);
            log.info("*** Elected as LEADER: {} ***", schedulerId);
        } else {
            String leader = jedis.get(LEADER_KEY);
            log.debug("Not leader. Current leader: {}", leader);
        }
    }

    /**
     * Refresh our lease TTL to prevent expiry while we are still alive.
     * If the key no longer belongs to us, step down.
     */
    private void refreshLease(Jedis jedis) {
        String current = jedis.get(LEADER_KEY);
        if (!schedulerId.equals(current)) {
            // Someone else took over (or key expired)
            isLeader.set(false);
            log.warn("Lost leadership! Current leader: {}", current);
            return;
        }
        // Re-set with fresh TTL (GETEX would be cleaner but requires Redis 6.2+)
        SetParams params = SetParams.setParams().xx().ex(LEASE_TTL_SEC);
        jedis.set(LEADER_KEY, schedulerId, params);
        log.debug("Leadership lease refreshed for {}", schedulerId);
    }
}
