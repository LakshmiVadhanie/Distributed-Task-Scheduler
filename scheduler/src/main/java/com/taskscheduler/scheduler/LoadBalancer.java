package com.taskscheduler.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Load balancer that selects the optimal worker for a task.
 *
 * Strategy: Weighted Least-Connections
 *   score = activeTasks / capacity   (lower is better)
 *
 * Falls back to simple round-robin for tie-breaking.
 */
public class LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);

    private final WorkerRegistry workerRegistry;

    public LoadBalancer(WorkerRegistry workerRegistry) {
        this.workerRegistry = workerRegistry;
    }

    /**
     * Select the best available worker.
     *
     * @return Optional WorkerInfo, empty if no workers are available
     */
    public Optional<WorkerInfo> selectWorker() {
        List<WorkerInfo> available = workerRegistry.getAvailableWorkers();

        if (available.isEmpty()) {
            log.warn("No available workers for task dispatch");
            return Optional.empty();
        }

        // Weighted least-connections: pick worker with lowest load ratio
        WorkerInfo chosen = available.stream()
                .min(Comparator.comparingDouble(this::loadRatio))
                .orElse(null);

        if (chosen != null) {
            log.debug("Selected worker {} (load={:.2f}, active={}/{})",
                    chosen.getWorkerId(),
                    loadRatio(chosen),
                    chosen.getActiveTasks(),
                    chosen.getCapacity());
        }

        return Optional.ofNullable(chosen);
    }

    /**
     * Load ratio: 0.0 (completely idle) to 1.0+ (at/over capacity).
     * Also factors in CPU load to avoid overwhelming a hot machine.
     */
    private double loadRatio(WorkerInfo worker) {
        double taskRatio = (double) worker.getActiveTasks() / Math.max(1, worker.getCapacity());
        double cpuFactor = worker.getCpuLoad() / 100.0;
        // Weighted blend: 70% task ratio, 30% CPU
        return 0.7 * taskRatio + 0.3 * cpuFactor;
    }

    /**
     * Returns a summary of worker load for observability.
     */
    public String getLoadSummary() {
        StringBuilder sb = new StringBuilder("Worker load: ");
        for (WorkerInfo w : workerRegistry.getAllWorkers()) {
            sb.append(String.format("[%s active=%d/%d cpu=%.1f%%] ",
                    w.getWorkerId(), w.getActiveTasks(), w.getCapacity(), w.getCpuLoad()));
        }
        return sb.toString().trim();
    }
}
