//! `zeron mcp-bridge` — a minimal MCP stdio server the engine attaches to
//! every agent session (ACP `mcpServers`). Tools:
//!
//! - `list_sessions`   → the registry's chats index (id/title/cwd/harness)
//! - `send_to_session` → queue a message into another session as its next
//!   user turn (engine `SendToSession` RPC: hop-limited, rate-limited,
//!   attributed `agent:{thisChat}`)
//!
//! Identity/wiring ride the environment the engine set at dispatch:
//! `ZERON_CHAT_ID` (which session this bridge speaks for) and
//! `ZERON_IPC_PORT` (the engine's localhost IPC). Local IPC is the existing
//! local trust boundary (the UI drives it unauthenticated); the bridge adds
//! no new exposure.
//!
//! Protocol: JSON-RPC 2.0 over stdio, one message per line — the subset of
//! MCP that `initialize` / `tools/list` / `tools/call` clients need. No SDK;
//! the surface is three methods.

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub async fn run() -> anyhow::Result<()> {
    let chat_id = std::env::var("ZERON_CHAT_ID")
        .map_err(|_| anyhow::anyhow!("ZERON_CHAT_ID not set (bridge must be engine-spawned)"))?;
    let ipc_port: u16 = std::env::var("ZERON_IPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("ZERON_IPC_PORT not set"))?;

    let stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(req) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = req.get("id").cloned();
        let method = req.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => Some(json!({
                "protocolVersion": req["params"]["protocolVersion"].as_str().unwrap_or("2025-06-18"),
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "zeron", "version": env!("CARGO_PKG_VERSION") },
            })),
            "notifications/initialized" | "notifications/cancelled" => None, // notifications: no reply
            "tools/list" => Some(json!({ "tools": tool_specs() })),
            "tools/call" => Some(handle_tool_call(&req["params"], &chat_id, ipc_port).await),
            "ping" => Some(json!({})),
            _ => {
                if id.is_some() {
                    write_line(
                        &mut stdout,
                        &json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("unknown method {method}") } }),
                    )
                    .await?;
                }
                continue;
            }
        };
        if let (Some(id), Some(result)) = (id, response) {
            write_line(
                &mut stdout,
                &json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            )
            .await?;
        }
    }
    Ok(())
}

async fn write_line(stdout: &mut tokio::io::Stdout, value: &Value) -> anyhow::Result<()> {
    stdout.write_all(value.to_string().as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

fn tool_specs() -> Value {
    json!([
        {
            "name": "list_sessions",
            "description": "List the other agent sessions in this workspace (id, title, working directory, harness). Use it to find a session to message.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        },
        {
            "name": "send_to_session",
            "description": "Send a one-way message to another agent session. It arrives as that session's next user turn, attributed to this session. No reply channel — ask the user to check the target session, or have the target message you back.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target_chat_id": { "type": "string", "description": "The target session's chat id (from list_sessions)." },
                    "message": { "type": "string", "description": "The message to deliver." }
                },
                "required": ["target_chat_id", "message"],
                "additionalProperties": false
            }
        }
    ])
}

/// MCP tool-call result envelope: `content` blocks + `isError`.
fn tool_text(text: impl Into<String>, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text.into() }], "isError": is_error })
}

async fn handle_tool_call(params: &Value, chat_id: &str, ipc_port: u16) -> Value {
    let name = params["name"].as_str().unwrap_or("");
    let args = &params["arguments"];
    let client = match zeron_rpc::connect_ws(&format!("ws://127.0.0.1:{ipc_port}")).await {
        Ok(client) => client,
        Err(err) => return tool_text(format!("engine unreachable: {err}"), true),
    };
    match name {
        "list_sessions" => {
            // WatchChats is a stream; the first frame is the full snapshot.
            let mut rx = match client
                .subscribe(zeron_rpc::methods::WATCH_CHATS, json!({}))
                .await
            {
                Ok(rx) => rx,
                Err(err) => return tool_text(format!("WatchChats failed: {err}"), true),
            };
            let Some(snapshot) = rx.recv().await else {
                return tool_text("WatchChats returned no snapshot", true);
            };
            let sessions: Vec<Value> = snapshot
                .as_array()
                .map(|chats| {
                    chats
                        .iter()
                        .filter(|c| !c["archived"].as_bool().unwrap_or(false))
                        .filter(|c| c["id"].as_str() != Some(chat_id))
                        .map(|c| {
                            json!({
                                "chat_id": c["id"],
                                "title": c["title"],
                                "cwd": c["cwd"],
                                "harness": c["config"]["harness"],
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            tool_text(
                serde_json::to_string_pretty(&sessions).unwrap_or_else(|_| "[]".into()),
                false,
            )
        }
        "send_to_session" => {
            let target = args["target_chat_id"].as_str().unwrap_or("");
            let message = args["message"].as_str().unwrap_or("");
            if target.is_empty() || message.is_empty() {
                return tool_text("target_chat_id and message are required", true);
            }
            match client
                .call(
                    zeron_rpc::methods::SEND_TO_SESSION,
                    json!({
                        "fromChatId": chat_id,
                        "targetChatId": target,
                        "message": message,
                    }),
                )
                .await
            {
                Ok(reply) => tool_text(
                    format!(
                        "delivered to {target} (command {})",
                        reply["commandId"].as_str().unwrap_or("?")
                    ),
                    false,
                ),
                Err(err) => tool_text(format!("send failed: {err}"), true),
            }
        }
        other => tool_text(format!("unknown tool {other}"), true),
    }
}
