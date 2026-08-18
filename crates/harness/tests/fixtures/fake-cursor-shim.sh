#!/bin/sh
# Fake zeron cursor shim for zeron-harness tests: speaks the shim's JSONL
# protocol (see crates/harness/src/cursor/shim.mjs) without node or the SDK.
# Driven by crates/harness/tests/cursor.rs.

emit() { printf '%s\n' "$1"; }
has() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac; }

read -r first || exit 1
case "$first" in
*'"op":"run"'*) ;;
*) emit '{"ev":"fatal","message":"expected op run first"}'; exit 1 ;;
esac

case "$first" in

*scenario:mcp*)
  for want in '"resume":"agent-existing"' '"mcpServers":{"zeron"' \
    '"command":"/opt/zeron bin/comet"' \
    '"args":["mcp-bridge","--literal=$HOME"]' \
    '"ZERON_CHAT_ID":"chat-a"' '"ZERON_IPC_PORT":"12345"'; do
    if ! has "$first" "$want"; then
      emit "{\"ev\":\"fatal\",\"message\":\"Cursor MCP option missing: $want\"}"
      exit 1
    fi
  done
  emit '{"ev":"ready","agentId":"agent-existing","model":"auto"}'
  emit '{"ev":"tool","phase":"start","id":"mcp-1","name":"mcp","args":{"providerIdentifier":"zeron","toolName":"send_to_session","args":{"target_chat_id":"chat-b"}}}'
  emit '{"ev":"tool","phase":"end","id":"mcp-1","name":"mcp","args":{"providerIdentifier":"zeron","toolName":"send_to_session","args":{"target_chat_id":"chat-b"}},"error":false}'
  emit '{"ev":"turn","status":"finished"}'
  exit 0
  ;;

*scenario:happy*)
  emit '{"ev":"ready","agentId":"agent-1","model":"composer-2.5"}'
  emit '{"ev":"thinking","text":"planning"}'
  emit '{"ev":"text","text":"Hello from cursor"}'
  emit '{"ev":"tool","phase":"start","id":"c1","name":"shell","args":{"command":"ls -la"}}'
  emit '{"ev":"tool","phase":"end","id":"c1","name":"shell","args":{"command":"ls -la"},"error":false}'
  # A spawned subagent: the task chip on the parent feed, its interior tagged.
  emit '{"ev":"tool","phase":"start","id":"task1","name":"task","args":{"description":"scan repo"}}'
  emit '{"ev":"text","text":"sub scanning","parent":"task1"}'
  emit '{"ev":"tool","phase":"start","id":"s1","name":"grep","args":{"pattern":"todo"},"parent":"task1"}'
  emit '{"ev":"tool","phase":"end","id":"s1","name":"grep","args":{"pattern":"todo"},"error":false,"parent":"task1"}'
  emit '{"ev":"tool","phase":"end","id":"task1","name":"task","args":{"description":"scan repo"},"error":false}'
  # Unknown frame kinds must be tolerated.
  emit '{"ev":"someNewThing","x":1}'
  emit '{"ev":"usage","input":11,"output":5}'
  emit '{"ev":"turn","status":"finished"}'
  # Parked: wait for a follow-up or stdin EOF.
  read -r next || exit 0
  case "$next" in
  *'"op":"user"'*)
    emit '{"ev":"text","text":"second turn"}'
    emit '{"ev":"turn","status":"finished"}'
    ;;
  esac
  exit 0
  ;;

*scenario:interrupt*)
  emit '{"ev":"ready","agentId":"agent-int","model":"auto"}'
  emit '{"ev":"text","text":"working"}'
  read -r msg || exit 0
  case "$msg" in
  *'"op":"interrupt"'*)
    emit '{"ev":"turn","status":"cancelled"}'
    ;;
  esac
  exit 0
  ;;

*scenario:fatal*)
  emit '{"ev":"fatal","message":"Cursor SDK is not authenticated (its login is separate from `cursor-agent login`): set CURSOR_API_KEY from cursor.com/settings, then retry."}'
  exit 1
  ;;

*scenario:crash*)
  emit '{"ev":"ready","agentId":"agent-c","model":"auto"}'
  echo "shim exploded" >&2
  exit 3
  ;;

*)
  emit '{"ev":"fatal","message":"unknown scenario"}'
  exit 1
  ;;
esac
