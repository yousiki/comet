#!/usr/bin/env bash
# Interactive local playground for org-shared sessions + agent-to-agent sends.
# NO Cloudflare account needed: the edge runs locally under `wrangler dev`
# with AUTH_MODE=dev (bearer string IS the identity, `user@org` form).
#
# Starts:
#   1. the local edge worker on :27640 (unless one is already healthy)
#   2. alice's HEADLESS engine (:27801) — simulates the teammate/VPS side
# then prints the command that opens bob's GUI against the same org.
#
# Both users share org "myteam": every session either side creates is visible
# and drivable by the other, and agents can message each other's sessions.
#
# Usage: scripts/try-shared.sh [claude|mock]   (harness; default claude,
#        falls back to mock when the `claude` CLI is not installed)
# Stop:  Ctrl-C (kills the engine; the edge keeps running for the GUI).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v cargo >/dev/null 2>&1 || PATH="$HOME/.cargo/bin:$PATH"
EDGE_PORT="${ZERON_E2E_EDGE_PORT:-27640}"
EDGE_URL="http://127.0.0.1:${EDGE_PORT}"
ORG=myteam

HARNESS="${1:-claude}"
if [[ "$HARNESS" == "claude" ]] && ! command -v claude >/dev/null 2>&1; then
  echo "note: 'claude' CLI not found — using the mock harness (canned replies)"
  HARNESS=mock
fi

ZERON="$ROOT/target/release/zeron"
[[ -x "$ZERON" ]] || ZERON="$ROOT/target/debug/zeron"
[[ -x "$ZERON" ]] || { echo "build first: cargo build --release -p zeron"; exit 1; }

# ── 1. local edge ──────────────────────────────────────────────────────────
if curl -sf -m 3 "$EDGE_URL/health" | grep -q '"auth":"dev"'; then
  echo "edge: reusing dev worker on :$EDGE_PORT"
else
  echo "edge: starting wrangler dev on :$EDGE_PORT (log: /tmp/zeron-try-edge.log)"
  set -m
  bash -c "cd '$ROOT/edge' && exec npx wrangler dev --port '$EDGE_PORT' --var AUTH_MODE:dev" \
    >/tmp/zeron-try-edge.log 2>&1 &
  set +m
  until curl -sf -m 3 "$EDGE_URL/health" >/dev/null 2>&1; do sleep 1; done
fi

# ── 2. alice: headless engine (the "teammate on a VPS") ────────────────────
echo
echo "alice: headless engine on ipc :27801 (data: ~/.zeron-try-alice)"
echo "-------------------------------------------------------------------"
cat <<EOF
In ANOTHER terminal, open bob's GUI (same org, different user):

  ZERON_DATA_DIR=~/.zeron-try-bob ZERON_IPC_PORT=27802 \\
  ZERON_EDGE_URL=$EDGE_URL ZERON_EDGE_TOKEN=bob@$ORG \\
  ZERON_ORG_ID=$ORG ZERON_USER_ID=bob ZERON_WORKOS_CLIENT_ID= \\
  ZERON_HARNESS=$HARNESS $ZERON

Things to try in bob's GUI:
  * every session alice's engine hosts shows up in bob's sidebar
    (rows read "folder @ device · alice") — click one and send a message:
    alice's engine executes it, the turn is attributed to bob
  * create two sessions and tell one agent:
      "use the list_sessions tool, then send_to_session to the other
       session asking it to summarize its current task"
    the message lands in the other session as a user turn tagged agent:{id}

To drive alice's side from a THIRD terminal (optional):
  ZERON_DATA_DIR=~/.zeron-try-alice2 ZERON_IPC_PORT=27803 \\
  ZERON_EDGE_URL=$EDGE_URL ZERON_EDGE_TOKEN=alice@$ORG \\
  ZERON_ORG_ID=$ORG ZERON_USER_ID=alice ZERON_WORKOS_CLIENT_ID= \\
  ZERON_HARNESS=$HARNESS $ZERON
-------------------------------------------------------------------
EOF

exec env \
  ZERON_DATA_DIR="$HOME/.zeron-try-alice" ZERON_IPC_PORT=27801 \
  ZERON_DEVICE_NAME=alice-vps \
  ZERON_EDGE_URL="$EDGE_URL" ZERON_EDGE_TOKEN="alice@$ORG" \
  ZERON_ORG_ID="$ORG" ZERON_USER_ID=alice ZERON_WORKOS_CLIENT_ID= \
  ZERON_HARNESS="$HARNESS" RUST_LOG=info \
  "$ZERON" headless
