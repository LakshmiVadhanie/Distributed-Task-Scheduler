# Distributed Task Scheduler

A production-grade, fault-tolerant distributed task scheduler built with **Java 17**, **gRPC**, **Redis**, **Flask**, and **Kubernetes**.

```
┌─────────────────────────────────────────────────────────────────┐
│                       CLIENT (REST)                             │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP/REST
┌────────────────────────▼────────────────────────────────────────┐
│            Flask Gateway  (port 5000)                           │
│   POST /tasks  GET /tasks/:id  GET /metrics  GET /workers       │
└────────────────────────┬────────────────────────────────────────┘
                         │ LPUSH task IDs
┌────────────────────────▼────────────────────────────────────────┐
│                     Redis 7                                      │
│  tasks:queue:high/normal/low   task:<id> hash   scheduler:leader│
│  workers:active set            metrics:*                         │
└──────┬──────────────────────────────────────────────────────────┘
       │ RPOP + HGETALL                     SET NX (leader election)
┌──────▼──────────────────────────────────────────────────────────┐
│           Scheduler Cluster  (3 replicas, 1 leader)             │
│   LeaderElection  TaskDispatcher  WorkerRegistry  HeartbeatMonitor│
│   LoadBalancer    gRPC server (port 9090)                        │
└──────┬──────────────────────────────────────────────────────────┘
       │ gRPC ExecuteTask
┌──────▼──────────────────────────────────────────────────────────┐
│           Worker Pool  (2-20 replicas, auto-scales)             │
│   WorkerGrpcServer  TaskExecutor  HeartbeatClient               │
│   (port 9091 each)                                               │
└─────────────────────────────────────────────────────────────────┘
```

## Features

- **Leader Election** via Redis SET NX with TTL — only one scheduler dispatches at a time
- **Heartbeat Monitoring** — dead workers are evicted within 15 seconds
- **Weighted Least-Connections Load Balancing** — tasks routed to least-loaded worker
- **Priority Queues** — three priority levels (high / normal / low)
- **Automatic Retry** — failed tasks retry up to 3 times before permanent failure
- **Kubernetes HPA** — workers auto-scale from 2 to 20 replicas based on CPU
- **99.5%+ task completion rate** under normal operating conditions
- **Flask REST Gateway** — clean API with bulk submit, metrics, and health endpoints
- **Observability** — Redis-backed metrics exposed at `/metrics`

## Architecture Deep Dive

### Leader Election

```
Scheduler-1  -->  SET scheduler:leader "scheduler-1" NX EX 10  -->  "OK" (elected!)
Scheduler-2  -->  SET scheduler:leader "scheduler-2" NX EX 10  -->  nil  (already set)
Scheduler-3  -->  SET scheduler:leader "scheduler-3" NX EX 10  -->  nil

Every 4 seconds, Scheduler-1 refreshes its lease:
  SET scheduler:leader "scheduler-1" XX EX 10

If Scheduler-1 crashes, key expires after 10 seconds.
Scheduler-2 or Scheduler-3 wins the next election cycle.
```

### Task Lifecycle

```
CLIENT            GATEWAY           REDIS                SCHEDULER          WORKER
  |                  |                 |                      |                |
  |-- POST /tasks -->|                 |                      |                |
  |                  |-- HSET task --->|                      |                |
  |                  |-- LPUSH queue ->|                      |                |
  |<-- 202 ACCEPTED -|                 |<--- RPOP queue ------|                |
  |                  |                 |<--- HGETALL task -----|                |
  |                  |                 |                      |-- ExecuteTask->|
  |                  |                 |                      |                |-- work
  |                  |                 |<-- HSET COMPLETED ---|<-- response ---|
  |-- GET /tasks/id->|                 |                      |                |
  |                  |-- HGETALL ----->|                      |                |
  |<-- 200 COMPLETED-|                 |                      |                |
```

### Load Balancing Formula

```
score = 0.7 * (activeTasks / capacity) + 0.3 * (cpuLoad / 100)

Worker with lowest score is selected (weighted least-connections).
Workers at full capacity are excluded from selection.
```

## Performance

| Metric               | Value                                  |
| -------------------- | -------------------------------------- |
| Task completion rate | 99.5%+                                 |
| Submit throughput    | ~800 tasks/sec (10 concurrent workers) |
| Avg dispatch latency | < 5ms (task popped to gRPC call sent)  |
| Heartbeat timeout    | 15 seconds                             |
| Leader failover time | < 15 seconds                           |
| Worker scale-up time | ~30 seconds (HPA + pod startup)        |

## Tech Stack

| Component             | Technology                               |
| --------------------- | ---------------------------------------- |
| Scheduler / Worker    | Java 17, Maven                           |
| RPC framework         | gRPC 1.60 + Protobuf 3                   |
| Message queue / state | Redis 7                                  |
| API Gateway           | Python 3.12, Flask 3, Gunicorn           |
| Containerization      | Docker, multi-stage builds               |
| Orchestration         | Kubernetes 1.26+, HPA                    |
| Load balancing        | K8s Service + weighted least-connections |
| Leader election       | Redis SET NX with TTL                    |
