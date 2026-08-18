// zeron cursor shim — a thin, zeron-owned wrapper around the PINNED
// @cursor/sdk (see CURSOR_SDK_PIN in crates/harness/src/cursor/mod.rs; the
// SDK is public beta with ~weekly releases — expect churn, revalidate on
// every bump). Materialized by zeron into the SDK's managed install dir so
// `import("@cursor/sdk")` resolves from the sibling node_modules.
//
// Why a shim at all: @cursor/sdk does NOT wrap the cursor-agent binary — it
// is Cursor's agent runtime bundled in-process, speaking proprietary
// protobuf/ConnectRPC to Cursor's backend, with client-side tool execution.
// There is no stdio wire to drive from Rust; this file IS the wire.
//
// Protocol: JSONL, one frame per line.
//   stdin  (engine → shim):
//     {"op":"run","prompt","cwd","model"?,"resume"?,
//      "mcpServers"?}                                      start / first turn
//     {"op":"user","prompt"}                            next turn (parked)
//     {"op":"interrupt"}                                cancel the live run
//   stdout (shim → engine):
//     {"ev":"ready","agentId","model"?}
//     {"ev":"text","text","parent"?}        parent = spawning task callId
//     {"ev":"thinking","text","parent"?}
//     {"ev":"tool","phase":"start"|"end","id","name","args"?,"error"?,"parent"?}
//     {"ev":"usage","input","output"}
//     {"ev":"turn","status":"finished"|"error"|"cancelled","error"?}
//     {"ev":"fatal","message"}              unrecoverable (auth, SDK init)
//
// Unknown SDK update kinds are ignored (never fatal): the SDK is beta and
// its delta union grows; the engine treats a degraded stream as chip-only.

import readline from "node:readline";

const out = (o) => process.stdout.write(JSON.stringify(o) + "\n");
const fatal = (message) => {
  out({ ev: "fatal", message: String(message) });
  process.exit(1);
};

let sdk;
try {
  sdk = await import("@cursor/sdk");
} catch (e) {
  fatal(`@cursor/sdk failed to load: ${e?.message ?? e}`);
}
const { Agent, Cursor } = sdk;

let agent = null;
let run = null;
let interrupted = false;

// One InteractionUpdate → zero or one frame. `parent` attributes nested
// subagent traffic (tool-call-delta carries the child's updates tagged by
// the spawning task's callId — the full subagent transcript, live).
function mapUpdate(u, parent) {
  if (!u || typeof u !== "object") return;
  const tag = parent ? { parent } : {};
  switch (u.type) {
    case "text-delta":
      if (u.text) out({ ev: "text", text: u.text, ...tag });
      break;
    case "thinking-delta":
      if (u.text) out({ ev: "thinking", text: u.text, ...tag });
      break;
    case "tool-call-started":
      out({
        ev: "tool",
        phase: "start",
        id: u.callId,
        name: u.toolCall?.type ?? "tool",
        args: u.toolCall?.args ?? null,
        ...tag,
      });
      break;
    case "tool-call-completed": {
      const r = u.toolCall?.result;
      const failed =
        r?.status === "error" ||
        (r?.status === "success" &&
          typeof r?.value?.exitCode === "number" &&
          r.value.exitCode !== 0);
      out({
        ev: "tool",
        phase: "end",
        id: u.callId,
        name: u.toolCall?.type ?? "tool",
        args: u.toolCall?.args ?? null,
        error: Boolean(failed),
        ...tag,
      });
      break;
    }
    case "tool-call-delta":
      // Nested subagent update, tagged by the spawning task call.
      if (u.taskUpdate) mapUpdate(u.taskUpdate, u.callId);
      break;
    case "turn-ended":
      if (u.usage) {
        out({
          ev: "usage",
          input: u.usage.inputTokens ?? 0,
          output: u.usage.outputTokens ?? 0,
        });
      }
      break;
    default:
      // step-*/summary-*/token-delta/partial-tool-call/…: no consumer.
      break;
  }
}

// Auth errors surface at RUN time (Agent.create succeeds unauthed —
// verified live: the turn fails with "[unknown] Invalid User API Key").
// Attach the exact fix, because the SDK's credentials are separate from
// `cursor-agent login`.
function withAuthHint(message) {
  const text = String(message ?? "");
  if (/api key|not authenticated|unauthorized/i.test(text)) {
    return (
      text +
      " — the Cursor SDK's login is separate from `cursor-agent login`: " +
      "set CURSOR_API_KEY (from cursor.com/settings) in zeron's environment."
    );
  }
  return text;
}

async function runTurn(prompt) {
  interrupted = false;
  try {
    run = await agent.send(prompt, {
      onDelta: ({ update }) => {
        try {
          mapUpdate(update);
        } catch {
          // A malformed update must never kill the turn.
        }
      },
    });
  } catch (e) {
    out({ ev: "turn", status: "error", error: withAuthHint(e?.message ?? e) });
    run = null;
    return;
  }
  let result;
  try {
    result = await run.wait();
  } catch (e) {
    out({
      ev: "turn",
      status: interrupted ? "cancelled" : "error",
      error: withAuthHint(e?.message ?? e),
    });
    run = null;
    return;
  }
  run = null;
  out({
    ev: "turn",
    status: result?.status ?? "finished",
    ...(result?.error?.message ? { error: withAuthHint(result.error.message) } : {}),
  });
}

async function start(msg) {
  const model = msg.model ? { id: msg.model } : { id: "auto" };
  const options = {
    model,
    // askQuestion has no public answer channel in this SDK (SDKRequestMessage
    // carries only a request id) — a question would block the run forever.
    // generateImage has nowhere to land in a zeron session (ACP parity).
    disallowedTools: ["askQuestion", "generateImage"],
    local: { cwd: msg.cwd || process.cwd(), enableAgentRetries: true },
  };
  // @cursor/sdk 1.0.28 AgentOptions accepts this map for both create and
  // resume. Omit the property entirely when zeron has no session bridge so
  // the SDK's ambient project/user MCP resolution remains unchanged.
  if (msg.mcpServers && Object.keys(msg.mcpServers).length > 0) {
    options.mcpServers = msg.mcpServers;
  }
  try {
    agent = msg.resume
      ? await Agent.resume(msg.resume, options)
      : await Agent.create(options);
  } catch (e) {
    // Auth is the common cause: the SDK's credentials are SEPARATE from
    // `cursor-agent login` (verified) — name the fix precisely.
    const auth = await Cursor.auth.status().catch(() => null);
    if (!process.env.CURSOR_API_KEY && auth?.status !== "logged-in") {
      fatal(
        "Cursor SDK is not authenticated (its login is separate from " +
          "`cursor-agent login`): set CURSOR_API_KEY from cursor.com/settings, " +
          `then retry. (${e?.message ?? e})`,
      );
    }
    fatal(`cursor agent failed to start: ${e?.message ?? e}`);
  }
  out({ ev: "ready", agentId: agent.agentId, model: agent.model?.id ?? model.id });
  await runTurn(msg.prompt ?? "");
}

const rl = readline.createInterface({ input: process.stdin });
let chain = Promise.resolve();
rl.on("line", (line) => {
  line = line.trim();
  if (!line) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }
  switch (msg.op) {
    case "run":
      chain = chain.then(() => start(msg)).catch((e) => fatal(e));
      break;
    case "user":
      chain = chain
        .then(() => (agent ? runTurn(msg.prompt ?? "") : undefined))
        .catch((e) => fatal(e));
      break;
    case "interrupt":
      interrupted = true;
      if (run) run.cancel().catch(() => {});
      break;
    default:
      break;
  }
});
rl.on("close", () => {
  // stdin EOF: the engine is done with the session.
  const r = run;
  run = null;
  (r ? r.cancel().catch(() => {}) : Promise.resolve()).finally(() => {
    try {
      agent?.close();
    } catch {}
    process.exit(0);
  });
});
