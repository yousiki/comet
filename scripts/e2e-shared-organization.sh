#!/usr/bin/env bash
# Two-user shared-Organization E2E: real edge (wrangler dev), two headless engines
# signed in as DIFFERENT users of the SAME Organization, and the e2e_driver proving the
# Organization-shared session path:
#
#   alice's engine A hosts a chat; bob's engine B sees it in the shared
#   registry (reg2/{organizationId}), queues a Run into it through the chat3 room, A
#   executes, and the transcript — with `userId: bob` attribution on the user
#   entry — syncs back to B.
#
# Usage: scripts/e2e-shared-organization.sh
# Env:   ZERON_E2E_EDGE_PORT (default 27640), ZERON_E2E_KEEP_LOGS=1 to keep logs.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v cargo >/dev/null 2>&1 || PATH="$HOME/.cargo/bin:$PATH"
EDGE_PORT="${ZERON_E2E_EDGE_PORT:-27640}"
EDGE_URL="http://localhost:${EDGE_PORT}"
ORGANIZATION_ID="shorg1"
TOKEN_A="alice@${ORGANIZATION_ID}"
TOKEN_B="bob@${ORGANIZATION_ID}"
A_PORT=27811
B_PORT=27812
A_DIR=/tmp/e2e-shared-a
B_DIR=/tmp/e2e-shared-b
LOG_DIR="$(mktemp -d /tmp/zeron-e2e-shared-logs.XXXXXX)"

EDGE_PID=""
A_PID=""
B_PID=""
STATUS=1

cleanup() {
  for pid in "$A_PID" "$B_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  [[ -n "$EDGE_PID" ]] && kill -- -"$EDGE_PID" 2>/dev/null || true
  sleep 1
  for pid in "$A_PID" "$B_PID"; do
    [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
  done
  [[ -n "$EDGE_PID" ]] && kill -9 -- -"$EDGE_PID" 2>/dev/null || true
  rm -rf "$A_DIR" "$B_DIR"
  if [[ "$STATUS" -ne 0 ]]; then
    echo "--- engine A log (tail) ---"; tail -n 40 "$LOG_DIR/engine-a.log" 2>/dev/null || true
    echo "--- engine B log (tail) ---"; tail -n 40 "$LOG_DIR/engine-b.log" 2>/dev/null || true
    echo "--- edge log (tail) ---"; tail -n 40 "$LOG_DIR/edge.log" 2>/dev/null || true
  fi
  if [[ "${ZERON_E2E_KEEP_LOGS:-0}" != "1" ]]; then
    rm -rf "$LOG_DIR"
  else
    echo "logs kept in $LOG_DIR"
  fi
}
trap cleanup EXIT

wait_for() { # wait_for <description> <timeout_s> <command...>
  local what="$1" timeout="$2"; shift 2
  local waited=0
  until "$@" >/dev/null 2>&1; do
    sleep 1
    waited=$((waited + 1))
    if [[ "$waited" -ge "$timeout" ]]; then
      echo "FAIL: timed out waiting for $what" >&2
      exit 1
    fi
  done
}

if curl -sf -m 3 "$EDGE_URL/health" | grep -q '"auth":"dev"'; then
  echo "edge: reusing healthy dev-mode worker on :$EDGE_PORT"
else
  echo "edge: starting wrangler dev on :$EDGE_PORT"
  set -m
  bash -c "cd '$ROOT/edge' && exec npx wrangler dev --port '$EDGE_PORT' --var AUTH_MODE:dev" \
    >"$LOG_DIR/edge.log" 2>&1 &
  EDGE_PID=$!
  set +m
  wait_for "edge /health" 90 curl -sf -m 3 "$EDGE_URL/health"
fi

echo "build: zeron + e2e_driver"
(cd "$ROOT" && cargo build -q -p zeron)
(cd "$ROOT" && cargo build -q -p zeron-rpc --example e2e_driver)
ZERON="$ROOT/target/debug/zeron"
DRIVER="$ROOT/target/debug/examples/e2e_driver"

rm -rf "$A_DIR" "$B_DIR"
mkdir -p "$A_DIR" "$B_DIR"

# ZERON_USER_ID must match the dev bearer's user half: the edge derives the
# caller from `user@organization`, the engine stamps doc attribution from its profile.
start_engine() { # start_engine <data_dir> <ipc_port> <name> <token> <user> <log>
  ZERON_DATA_DIR="$1" ZERON_IPC_PORT="$2" ZERON_DEVICE_NAME="$3" \
    ZERON_EDGE_URL="$EDGE_URL" ZERON_EDGE_TOKEN="$4" ZERON_ORGANIZATION_ID="$ORGANIZATION_ID" \
    ZERON_USER_ID="$5" ZERON_HARNESS=mock RUST_LOG=info \
    "$ZERON" headless >"$6" 2>&1 &
}

start_engine "$A_DIR" "$A_PORT" "shared-device-alice" "$TOKEN_A" alice "$LOG_DIR/engine-a.log"; A_PID=$!
start_engine "$B_DIR" "$B_PORT" "shared-device-bob" "$TOKEN_B" bob "$LOG_DIR/engine-b.log"; B_PID=$!

wait_for "engine A ipc :$A_PORT" 60 bash -c "exec 3<>/dev/tcp/127.0.0.1/$A_PORT"
wait_for "engine B ipc :$B_PORT" 60 bash -c "exec 3<>/dev/tcp/127.0.0.1/$B_PORT"
echo "engines: alice pid=$A_PID ipc=:$A_PORT  bob pid=$B_PID ipc=:$B_PORT"

# Same flow as e2e-smoke, PLUS the bob-authorship assertion (third arg): the
# dev bearer `bob@shorg1` resolves to userId `bob` — the user entry the host
# writes for bob's queued run must carry it.
"$DRIVER" "$A_PORT" "$B_PORT" bob
STATUS=$?
exit "$STATUS"
