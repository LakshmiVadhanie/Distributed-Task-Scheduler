package com.taskscheduler.scheduler;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents a registered worker node tracked by the scheduler.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkerInfo {

    public enum Status { IDLE, BUSY, OVERLOADED, OFFLINE }

    private String workerId;
    private String host;
    private int    port;
    private int    capacity;        // max concurrent tasks
    private int    activeTasks;     // current running tasks
    private double cpuLoad;
    private double memoryUsage;
    private long   lastHeartbeat;   // epoch millis
    private Status status;

    public WorkerInfo() {}

    public WorkerInfo(String workerId, String host, int port, int capacity) {
        this.workerId      = workerId;
        this.host          = host;
        this.port          = port;
        this.capacity      = capacity;
        this.activeTasks   = 0;
        this.status        = Status.IDLE;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    // ── Computed ──────────────────────────────────────────────────────────
    public boolean isAvailable()   { return status != Status.OFFLINE && activeTasks < capacity; }
    public int     freeSlots()     { return Math.max(0, capacity - activeTasks); }
    public boolean isStale(long timeoutMs) {
        return System.currentTimeMillis() - lastHeartbeat > timeoutMs;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────
    public String  getWorkerId()                       { return workerId; }
    public void    setWorkerId(String workerId)        { this.workerId = workerId; }
    public String  getHost()                           { return host; }
    public void    setHost(String host)                { this.host = host; }
    public int     getPort()                           { return port; }
    public void    setPort(int port)                   { this.port = port; }
    public int     getCapacity()                       { return capacity; }
    public void    setCapacity(int capacity)           { this.capacity = capacity; }
    public int     getActiveTasks()                    { return activeTasks; }
    public void    setActiveTasks(int activeTasks)     { this.activeTasks = activeTasks; }
    public double  getCpuLoad()                        { return cpuLoad; }
    public void    setCpuLoad(double cpuLoad)          { this.cpuLoad = cpuLoad; }
    public double  getMemoryUsage()                    { return memoryUsage; }
    public void    setMemoryUsage(double memoryUsage)  { this.memoryUsage = memoryUsage; }
    public long    getLastHeartbeat()                  { return lastHeartbeat; }
    public void    setLastHeartbeat(long lastHeartbeat){ this.lastHeartbeat = lastHeartbeat; }
    public Status  getStatus()                         { return status; }
    public void    setStatus(Status status)            { this.status = status; }

    @Override
    public String toString() {
        return String.format("WorkerInfo{id=%s, addr=%s:%d, active=%d/%d, status=%s}",
                workerId, host, port, activeTasks, capacity, status);
    }
}
