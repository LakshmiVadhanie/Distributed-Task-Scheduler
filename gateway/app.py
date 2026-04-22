"""
Flask API Gateway for the Distributed Task Scheduler.

Endpoints:
  POST   /tasks              Submit a new task
  GET    /tasks/<task_id>    Get task status + result
  DELETE /tasks/<task_id>    Cancel a pending task
  GET    /tasks              List recent tasks (paginated)
  GET    /health             Health check
  GET    /metrics            Scheduler + worker metrics
  GET    /workers            List active workers
"""

import json
import os
import time
import uuid

import redis
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__)

# ── Static dashboard ─────────────────────────────────────────────────────────
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route("/", methods=["GET"])
def dashboard():
    """Serve the live monitoring dashboard."""
    return send_from_directory(DASHBOARD_DIR, "dashboard.html")

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
    return response

# ── Redis connection ──────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
)

# Redis queue keys (must match Java constants in TaskDispatcher.java)
QUEUE_HIGH   = "tasks:queue:high"
QUEUE_NORMAL = "tasks:queue:normal"
QUEUE_LOW    = "tasks:queue:low"
TASK_PREFIX  = "task:"

VALID_TASK_TYPES = {
    "DATA_PROCESSING",
    "EMAIL_SEND",
    "IMAGE_RESIZE",
    "HTTP_CALL",
    "COMPUTE",
    "GENERIC",
}

# ── Submit task ───────────────────────────────────────────────────────────────

@app.route("/tasks", methods=["POST"])
def submit_task():
    """
    Submit a new task for execution.

    Body (JSON):
      taskType   string   required  One of VALID_TASK_TYPES
      payload    object   optional  Task-specific parameters
      priority   int      optional  1-10 (default 5; 8+ = high, 4-7 = normal, 1-3 = low)
      timeoutMs  int      optional  Execution timeout in ms (default 30000)
    """
    data = request.get_json(force=True, silent=True) or {}

    # ── Validation ──
    task_type = data.get("taskType", "GENERIC").upper()
    if task_type not in VALID_TASK_TYPES:
        return jsonify({"error": f"Invalid taskType. Must be one of: {sorted(VALID_TASK_TYPES)}"}), 400

    priority   = int(data.get("priority", 5))
    timeout_ms = int(data.get("timeoutMs", 30_000))

    if not (1 <= priority <= 10):
        return jsonify({"error": "priority must be between 1 and 10"}), 400
    if not (1_000 <= timeout_ms <= 300_000):
        return jsonify({"error": "timeoutMs must be between 1000 and 300000"}), 400

    # ── Build task record ──
    task_id = str(uuid.uuid4())
    payload = json.dumps(data.get("payload", {}))

    task_record = {
        "taskId":      task_id,
        "taskType":    task_type,
        "payload":     payload,
        "priority":    str(priority),
        "timeoutMs":   str(timeout_ms),
        "status":      "PENDING",
        "retryCount":  "0",
        "submittedBy": request.remote_addr or "unknown",
        "createdAt":   str(int(time.time() * 1000)),
    }

    # ── Persist + enqueue (atomic pipeline) ──
    pipe = r.pipeline()
    pipe.hset(TASK_PREFIX + task_id, mapping=task_record)
    pipe.expire(TASK_PREFIX + task_id, 86400)   # 24h TTL

    queue = QUEUE_HIGH if priority >= 8 else QUEUE_NORMAL if priority >= 4 else QUEUE_LOW
    pipe.lpush(queue, task_id)
    pipe.incr("metrics:tasks_submitted")
    pipe.execute()

    return jsonify({
        "taskId":    task_id,
        "status":    "PENDING",
        "queue":     queue.split(":")[-1],
        "priority":  priority,
        "timeoutMs": timeout_ms,
    }), 202


# ── Get task status ───────────────────────────────────────────────────────────

@app.route("/tasks/<task_id>", methods=["GET"])
def get_task(task_id):
    """Return the full task record including status and result."""
    task = r.hgetall(TASK_PREFIX + task_id)
    if not task:
        return jsonify({"error": "Task not found", "taskId": task_id}), 404

    # Deserialize payload JSON string for readability
    if "payload" in task:
        try:
            task["payload"] = json.loads(task["payload"])
        except (json.JSONDecodeError, TypeError):
            pass

    # Convert timestamps to ISO-style readable format
    for ts_field in ("createdAt", "startedAt", "completedAt", "updatedAt"):
        if ts_field in task:
            try:
                task[ts_field + "Ms"] = int(task[ts_field])
            except (ValueError, TypeError):
                pass

    return jsonify(task), 200


# ── Cancel task ───────────────────────────────────────────────────────────────

@app.route("/tasks/<task_id>", methods=["DELETE"])
def cancel_task(task_id):
    """Cancel a PENDING task. Running tasks cannot be cancelled."""
    task = r.hgetall(TASK_PREFIX + task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404

    status = task.get("status", "")
    if status != "PENDING":
        return jsonify({
            "error": f"Cannot cancel task in status '{status}'. Only PENDING tasks can be cancelled."
        }), 409

    r.hset(TASK_PREFIX + task_id, "status", "CANCELLED")
    return jsonify({"taskId": task_id, "status": "CANCELLED"}), 200


# ── List tasks ────────────────────────────────────────────────────────────────

@app.route("/tasks", methods=["GET"])
def list_tasks():
    """
    List tasks from Redis.
    Query params:
      status  filter by status (PENDING|RUNNING|COMPLETED|FAILED|RETRYING)
      limit   max results (default 20, max 100)
    """
    status_filter = request.args.get("status", "").upper()
    limit         = min(int(request.args.get("limit", 20)), 100)

    # Scan all task keys (not for production at massive scale, fine for demo)
    task_keys = list(r.scan_iter(TASK_PREFIX + "*", count=200))
    tasks     = []

    for key in task_keys:
        task = r.hgetall(key)
        if not task:
            continue
        if status_filter and task.get("status") != status_filter:
            continue
        tasks.append({
            "taskId":    task.get("taskId"),
            "taskType":  task.get("taskType"),
            "status":    task.get("status"),
            "priority":  task.get("priority"),
            "workerId":  task.get("workerId"),
            "createdAt": task.get("createdAt"),
        })

    # Sort by createdAt descending (newest first)
    tasks.sort(key=lambda t: int(t.get("createdAt") or 0), reverse=True)

    return jsonify({
        "tasks": tasks[:limit],
        "total": len(tasks),
        "limit": limit,
    }), 200


# ── Health check ──────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    """Kubernetes liveness + readiness probe endpoint."""
    redis_ok = False
    try:
        r.ping()
        redis_ok = True
    except Exception:
        pass

    status_code = 200 if redis_ok else 503
    return jsonify({
        "status":   "healthy" if redis_ok else "degraded",
        "redis":    "ok" if redis_ok else "unreachable",
        "service":  "task-scheduler-gateway",
        "timestamp": int(time.time() * 1000),
    }), status_code


# ── Metrics ───────────────────────────────────────────────────────────────────

@app.route("/metrics", methods=["GET"])
def metrics():
    """Return aggregate scheduler metrics from Redis."""
    pipe = r.pipeline()
    pipe.get("metrics:tasks_submitted")
    pipe.get("metrics:tasks_completed")
    pipe.get("metrics:tasks_failed")
    pipe.get("metrics:worker_evictions")
    pipe.llen("tasks:queue:high")
    pipe.llen("tasks:queue:normal")
    pipe.llen("tasks:queue:low")
    pipe.lrange("metrics:exec_times", 0, 99)
    pipe.get("scheduler:leader")
    results = pipe.execute()

    submitted   = int(results[0] or 0)
    completed   = int(results[1] or 0)
    failed      = int(results[2] or 0)
    evictions   = int(results[3] or 0)
    q_high      = int(results[4] or 0)
    q_normal    = int(results[5] or 0)
    q_low       = int(results[6] or 0)
    exec_times  = [int(x) for x in (results[7] or []) if x]
    leader      = results[8] or "unknown"

    completion_rate = round(completed / submitted * 100, 2) if submitted > 0 else 0.0
    avg_exec_ms     = round(sum(exec_times) / len(exec_times), 2) if exec_times else 0.0
    p99_exec_ms     = sorted(exec_times)[int(len(exec_times) * 0.99)] if len(exec_times) > 1 else 0

    return jsonify({
        "tasks": {
            "submitted":      submitted,
            "completed":      completed,
            "failed":         failed,
            "completionRate": f"{completion_rate}%",
        },
        "queues": {
            "high":   q_high,
            "normal": q_normal,
            "low":    q_low,
            "total":  q_high + q_normal + q_low,
        },
        "performance": {
            "avgExecMs": avg_exec_ms,
            "p99ExecMs": p99_exec_ms,
        },
        "cluster": {
            "leader":          leader,
            "workerEvictions": evictions,
        },
    }), 200


# ── Workers ───────────────────────────────────────────────────────────────────

@app.route("/workers", methods=["GET"])
def list_workers():
    """List all known worker nodes and their current load."""
    worker_ids = r.smembers("workers:active")
    workers    = []

    for wid in worker_ids:
        raw = r.get(f"worker:{wid}")
        if raw:
            try:
                workers.append(json.loads(raw))
            except json.JSONDecodeError:
                pass

    return jsonify({
        "workers": workers,
        "count":   len(workers),
    }), 200


# ── Bulk submit (convenience for load testing) ────────────────────────────────

@app.route("/tasks/bulk", methods=["POST"])
def bulk_submit():
    """
    Submit multiple tasks in one request.
    Body: { "tasks": [ { taskType, payload, priority, timeoutMs }, ... ] }
    Max 100 tasks per call.
    """
    data  = request.get_json(force=True, silent=True) or {}
    items = data.get("tasks", [])

    if not items:
        return jsonify({"error": "No tasks provided"}), 400
    if len(items) > 100:
        return jsonify({"error": "Maximum 100 tasks per bulk request"}), 400

    submitted = []
    pipe      = r.pipeline()

    for item in items:
        task_type  = item.get("taskType", "GENERIC").upper()
        priority   = int(item.get("priority", 5))
        timeout_ms = int(item.get("timeoutMs", 30_000))
        payload    = json.dumps(item.get("payload", {}))
        task_id    = str(uuid.uuid4())

        task_record = {
            "taskId":     task_id,
            "taskType":   task_type,
            "payload":    payload,
            "priority":   str(priority),
            "timeoutMs":  str(timeout_ms),
            "status":     "PENDING",
            "retryCount": "0",
            "createdAt":  str(int(time.time() * 1000)),
        }

        queue = QUEUE_HIGH if priority >= 8 else QUEUE_NORMAL if priority >= 4 else QUEUE_LOW

        pipe.hset(TASK_PREFIX + task_id, mapping=task_record)
        pipe.expire(TASK_PREFIX + task_id, 86400)
        pipe.lpush(queue, task_id)
        submitted.append({"taskId": task_id, "queue": queue.split(":")[-1]})

    pipe.incrby("metrics:tasks_submitted", len(items))
    pipe.execute()

    return jsonify({"submitted": submitted, "count": len(submitted)}), 202


# ── Error handlers ────────────────────────────────────────────────────────────

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error", "detail": str(e)}), 500


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port  = int(os.environ.get("GATEWAY_PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    print(f"Starting Flask gateway on port {port} (debug={debug})")
    app.run(host="0.0.0.0", port=port, debug=debug)
