#!/usr/bin/env python3
"""
load_test.py — Stress test for the Distributed Task Scheduler

Usage:
    python load_test.py --url http://localhost:5000 --tasks 500 --concurrency 20

Outputs a summary report with throughput, completion rate, and latency percentiles.
"""

import argparse
import json
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib.request
import urllib.error

# ─────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────

TASK_TYPES = ["DATA_PROCESSING", "EMAIL_SEND", "IMAGE_RESIZE", "HTTP_CALL", "COMPUTE", "GENERIC"]

PAYLOADS = {
    "DATA_PROCESSING": {"records": 500},
    "EMAIL_SEND":      {"to": "test@example.com", "subject": "Load Test"},
    "IMAGE_RESIZE":    {"width": 1280, "height": 720, "format": "JPEG"},
    "HTTP_CALL":       {"url": "https://api.example.com/data", "method": "GET"},
    "COMPUTE":         {"iterations": 100_000},
    "GENERIC":         {"durationMs": 300},
}


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def http_post(url, data):
    body    = json.dumps(data).encode()
    req     = urllib.request.Request(url, data=body,
                                      headers={"Content-Type": "application/json"},
                                      method="POST")
    start   = time.time()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()), time.time() - start, None
    except urllib.error.HTTPError as e:
        return None, time.time() - start, f"HTTP {e.code}: {e.read().decode()}"
    except Exception as ex:
        return None, time.time() - start, str(ex)


def http_get(url):
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()), None
    except Exception as ex:
        return None, str(ex)


def submit_task(base_url):
    task_type = random.choice(TASK_TYPES)
    priority  = random.randint(1, 10)
    payload   = PAYLOADS.get(task_type, {})

    body = {
        "taskType":  task_type,
        "payload":   payload,
        "priority":  priority,
        "timeoutMs": 30_000,
    }

    result, elapsed, error = http_post(f"{base_url}/tasks", body)
    return {
        "taskId":   result.get("taskId") if result else None,
        "taskType": task_type,
        "priority": priority,
        "elapsed":  elapsed,
        "error":    error,
    }


def check_completion(base_url, task_ids, timeout_sec=120):
    """Poll until all tasks reach a terminal state or timeout."""
    deadline    = time.time() + timeout_sec
    remaining   = set(task_ids)
    completed   = {}
    failed      = {}
    check_count = 0

    print(f"\nPolling {len(task_ids)} tasks (timeout={timeout_sec}s)...")

    while remaining and time.time() < deadline:
        batch    = list(remaining)[:50]    # check 50 at a time
        finished = set()

        for tid in batch:
            task, err = http_get(f"{base_url}/tasks/{tid}")
            if err or not task:
                continue
            status = task.get("status", "")
            if status == "COMPLETED":
                completed[tid] = task
                finished.add(tid)
            elif status in ("FAILED", "CANCELLED"):
                failed[tid] = task
                finished.add(tid)

        remaining -= finished
        check_count += 1

        if remaining:
            pct = (len(task_ids) - len(remaining)) / len(task_ids) * 100
            print(f"  [{check_count:3d}] {pct:5.1f}% done | remaining={len(remaining)}", end="\r")
            time.sleep(2)

    print()  # newline after \r
    return completed, failed, list(remaining)


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Task Scheduler Load Test")
    parser.add_argument("--url",         default="http://localhost:5000", help="Gateway base URL")
    parser.add_argument("--tasks",       type=int, default=100,  help="Number of tasks to submit")
    parser.add_argument("--concurrency", type=int, default=10,   help="Concurrent submit threads")
    parser.add_argument("--wait",        action="store_true",    help="Wait for task completion")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Distributed Task Scheduler — Load Test")
    print(f"{'='*60}")
    print(f"  Target:      {args.url}")
    print(f"  Tasks:       {args.tasks}")
    print(f"  Concurrency: {args.concurrency}")
    print(f"{'='*60}\n")

    # ── Health check ──
    health, err = http_get(f"{args.url}/health")
    if err or not health:
        print(f"ERROR: Gateway not reachable at {args.url}/health — {err}")
        sys.exit(1)
    print(f"Gateway status: {health.get('status')} | Redis: {health.get('redis')}\n")

    # ── Submit tasks ──
    print(f"Submitting {args.tasks} tasks with {args.concurrency} concurrent workers...")
    submit_start = time.time()
    results      = []

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(submit_task, args.url) for _ in range(args.tasks)]
        for i, future in enumerate(as_completed(futures), 1):
            r = future.result()
            results.append(r)
            if i % 50 == 0 or i == args.tasks:
                print(f"  Submitted {i}/{args.tasks}...", end="\r")

    submit_elapsed = time.time() - submit_start
    print(f"\nSubmission complete in {submit_elapsed:.2f}s")

    # ── Submit stats ──
    submit_errors = [r for r in results if r["error"]]
    submit_ok     = [r for r in results if not r["error"]]
    task_ids      = [r["taskId"] for r in submit_ok if r["taskId"]]
    latencies     = [r["elapsed"] for r in submit_ok]

    print(f"\n── Submission Results ──────────────────────────────────")
    print(f"  Submitted OK:   {len(submit_ok)}")
    print(f"  Errors:         {len(submit_errors)}")
    print(f"  Throughput:     {len(submit_ok) / submit_elapsed:.1f} tasks/sec")
    print(f"  Avg latency:    {statistics.mean(latencies)*1000:.1f}ms")
    print(f"  P99 latency:    {sorted(latencies)[int(len(latencies)*0.99)]*1000:.1f}ms")

    if submit_errors:
        print(f"\n  Sample errors:")
        for e in submit_errors[:3]:
            print(f"    {e['error']}")

    # ── Wait for completion ──
    if args.wait and task_ids:
        wait_time    = max(60, args.tasks // 5)
        completed, failed, timed_out = check_completion(args.url, task_ids, timeout_sec=wait_time)

        total    = len(task_ids)
        comp_pct = len(completed) / total * 100 if total > 0 else 0
        fail_pct = len(failed)    / total * 100 if total > 0 else 0
        to_pct   = len(timed_out) / total * 100 if total > 0 else 0

        print(f"\n── Completion Results ──────────────────────────────────")
        print(f"  Total submitted:  {total}")
        print(f"  Completed:        {len(completed)} ({comp_pct:.1f}%)")
        print(f"  Failed:           {len(failed)}    ({fail_pct:.1f}%)")
        print(f"  Timed out:        {len(timed_out)} ({to_pct:.1f}%)")

        if comp_pct >= 99.5:
            print(f"\n  TARGET MET: {comp_pct:.1f}% >= 99.5% completion rate ✓")
        else:
            print(f"\n  TARGET MISSED: {comp_pct:.1f}% < 99.5% completion rate ✗")

    # ── Final metrics from gateway ──
    print(f"\n── Scheduler Metrics ───────────────────────────────────")
    metrics, err = http_get(f"{args.url}/metrics")
    if metrics:
        t = metrics.get("tasks", {})
        q = metrics.get("queues", {})
        p = metrics.get("performance", {})
        c = metrics.get("cluster", {})
        print(f"  Tasks submitted:  {t.get('submitted', 'N/A')}")
        print(f"  Tasks completed:  {t.get('completed', 'N/A')}")
        print(f"  Completion rate:  {t.get('completionRate', 'N/A')}")
        print(f"  Avg exec time:    {p.get('avgExecMs', 'N/A')}ms")
        print(f"  P99 exec time:    {p.get('p99ExecMs', 'N/A')}ms")
        print(f"  Queue depth:      {q.get('total', 'N/A')}")
        print(f"  Leader:           {c.get('leader', 'N/A')}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
