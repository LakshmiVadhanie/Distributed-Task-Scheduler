#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# demo.sh — Quick demo of the task scheduler API
# Run after: docker compose up --build
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

GATEWAY="http://localhost:5000"
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${CYAN}[demo]${NC} $*"; }
ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[NOTE]${NC} $*"; }

# ── Wait for gateway ──────────────────────────────────────────────
log "Waiting for gateway to be ready..."
for i in {1..20}; do
  STATUS=$(curl -sf "$GATEWAY/health" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['status'])" 2>/dev/null || echo "")
  if [ "$STATUS" = "healthy" ]; then
    ok "Gateway is healthy"; break
  fi
  echo -n "." && sleep 2
done

echo ""

# ── Health check ──────────────────────────────────────────────────
log "1. Health check"
curl -s "$GATEWAY/health" | python3 -m json.tool
echo ""

# ── Submit tasks of various types ────────────────────────────────
log "2. Submitting tasks..."

TASK1=$(curl -sf -X POST "$GATEWAY/tasks" \
  -H "Content-Type: application/json" \
  -d '{"taskType":"DATA_PROCESSING","payload":{"records":1000},"priority":9}')
TASK1_ID=$(echo "$TASK1" | python3 -c "import sys,json; print(json.load(sys.stdin)['taskId'])")
ok "Submitted DATA_PROCESSING (HIGH priority): $TASK1_ID"

TASK2=$(curl -sf -X POST "$GATEWAY/tasks" \
  -H "Content-Type: application/json" \
  -d '{"taskType":"EMAIL_SEND","payload":{"to":"user@example.com","subject":"Hello"},"priority":5}')
TASK2_ID=$(echo "$TASK2" | python3 -c "import sys,json; print(json.load(sys.stdin)['taskId'])")
ok "Submitted EMAIL_SEND (NORMAL priority): $TASK2_ID"

TASK3=$(curl -sf -X POST "$GATEWAY/tasks" \
  -H "Content-Type: application/json" \
  -d '{"taskType":"COMPUTE","payload":{"iterations":50000},"priority":3}')
TASK3_ID=$(echo "$TASK3" | python3 -c "import sys,json; print(json.load(sys.stdin)['taskId'])")
ok "Submitted COMPUTE (LOW priority): $TASK3_ID"

echo ""

# ── Bulk submit ───────────────────────────────────────────────────
log "3. Bulk submitting 10 tasks..."
BULK=$(curl -sf -X POST "$GATEWAY/tasks/bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "tasks": [
      {"taskType":"GENERIC","payload":{"durationMs":200},"priority":5},
      {"taskType":"GENERIC","payload":{"durationMs":300},"priority":5},
      {"taskType":"IMAGE_RESIZE","payload":{"width":800,"height":600},"priority":6},
      {"taskType":"HTTP_CALL","payload":{"url":"https://api.example.com"},"priority":7},
      {"taskType":"DATA_PROCESSING","payload":{"records":200},"priority":8},
      {"taskType":"EMAIL_SEND","payload":{"to":"a@example.com","subject":"Test"},"priority":5},
      {"taskType":"COMPUTE","payload":{"iterations":10000},"priority":4},
      {"taskType":"GENERIC","payload":{"durationMs":100},"priority":9},
      {"taskType":"GENERIC","payload":{"durationMs":150},"priority":3},
      {"taskType":"IMAGE_RESIZE","payload":{"width":1920,"height":1080},"priority":6}
    ]
  }')
BULK_COUNT=$(echo "$BULK" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
ok "Bulk submitted $BULK_COUNT tasks"

echo ""

# ── Poll task status ──────────────────────────────────────────────
log "4. Polling task status (waiting up to 30s)..."
for i in {1..15}; do
  STATUS=$(curl -sf "$GATEWAY/tasks/$TASK1_ID" | \
           python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "?")
  echo "  Task 1 [$TASK1_ID]: $STATUS"
  if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ]; then break; fi
  sleep 2
done

echo ""

# ── List workers ──────────────────────────────────────────────────
log "5. Active workers"
curl -s "$GATEWAY/workers" | python3 -m json.tool
echo ""

# ── Metrics ───────────────────────────────────────────────────────
log "6. Scheduler metrics"
curl -s "$GATEWAY/metrics" | python3 -m json.tool
echo ""

# ── List all tasks ────────────────────────────────────────────────
log "7. Task list (latest 5)"
curl -s "$GATEWAY/tasks?limit=5" | python3 -m json.tool
echo ""

ok "Demo complete! Run 'python3 scripts/load_test.py --tasks 500 --wait' for a full load test."
