//! Agent-to-agent send e2e driver (`scripts/e2e-agent-send.sh` runs it).
//!
//! Two engines (different users, same org), a chat on each. Spawns the real
//! `zeron mcp-bridge` subprocess as chat X's agent would see it (env-wired to
//! engine A) and drives one MCP `tools/call send_to_session` over stdio at
//! chat Y (hosted on engine B). Then proves on B that Y's transcript gained a
//! user turn attributed `agent:{X}` and a mock assistant reply.
//!
//! Args: <a_port> <b_port> <zeron_bin>

use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use zeron_rpc::{RpcClient, connect_ws, methods};

const STEP_TIMEOUT: Duration = Duration::from_secs(90);

fn fail(message: &str) -> ! {
    eprintln!("FAIL: {message}");
    std::process::exit(1);
}

fn pass(message: &str) {
    println!("PASS: {message}");
}

async fn device_id(client: &RpcClient, label: &str) -> String {
    match client
        .call(methods::LOCAL_DEVICE, serde_json::json!({}))
        .await
    {
        Ok(value) => value
            .get("deviceId")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| fail(&format!("{label}: LocalDevice reply missing deviceId"))),
        Err(err) => fail(&format!("{label}: LocalDevice call failed: {err}")),
    }
}

async fn create_mock_chat(client: &RpcClient, device: &str, label: &str) -> String {
    let space_id = uuid::Uuid::new_v4().to_string();
    client
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createSpace",
                "spaceId": space_id,
                "deviceId": device,
                "path": "/tmp",
            }),
        )
        .await
        .unwrap_or_else(|err| fail(&format!("createSpace on {label}: {err}")));
    let chat_id = uuid::Uuid::new_v4().to_string();
    client
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createChat",
                "chatId": chat_id,
                "spaceId": space_id,
                "config": {
                    "harness": "mock",
                    "model": null,
                    "reasoning": null,
                    "sandbox": "workspace-write",
                },
            }),
        )
        .await
        .unwrap_or_else(|err| fail(&format!("createChat on {label}: {err}")));
    chat_id
}

/// Same resubscribing stream-wait as e2e_driver (examples cannot share code).
async fn wait_stream<T>(
    client: &RpcClient,
    method: &str,
    params: serde_json::Value,
    what: &str,
    mut predicate: impl FnMut(&serde_json::Value) -> Option<T>,
) -> T {
    let deadline = Instant::now() + STEP_TIMEOUT;
    'resubscribe: loop {
        if deadline.saturating_duration_since(Instant::now()).is_zero() {
            fail(&format!(
                "{what}: timed out after {}s",
                STEP_TIMEOUT.as_secs()
            ));
        }
        let mut rx = match client.subscribe(method, params.clone()).await {
            Ok(rx) => rx,
            Err(err) => fail(&format!("{what}: subscribe {method} failed: {err}")),
        };
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                fail(&format!(
                    "{what}: timed out after {}s",
                    STEP_TIMEOUT.as_secs()
                ));
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(item)) => {
                    if let Some(found) = predicate(&item) {
                        return found;
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue 'resubscribe;
                }
                Err(_) => fail(&format!(
                    "{what}: timed out after {}s",
                    STEP_TIMEOUT.as_secs()
                )),
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let a_port: u16 = args.next().expect("a_port").parse().expect("A port");
    let b_port: u16 = args.next().expect("b_port").parse().expect("B port");
    let zeron_bin = args.next().expect("zeron binary path");

    let a = connect_ws(&format!("ws://127.0.0.1:{a_port}"))
        .await
        .unwrap_or_else(|err| fail(&format!("connect A ipc :{a_port}: {err}")));
    let b = connect_ws(&format!("ws://127.0.0.1:{b_port}"))
        .await
        .unwrap_or_else(|err| fail(&format!("connect B ipc :{b_port}: {err}")));

    let a_dev = device_id(&a, "A").await;
    let b_dev = device_id(&b, "B").await;
    let chat_x = create_mock_chat(&a, &a_dev, "A").await;
    let chat_y = create_mock_chat(&b, &b_dev, "B").await;
    pass(&format!("chats created: X={chat_x} (A)  Y={chat_y} (B)"));

    // A must see Y's registry row (host device + config) before it can send.
    wait_stream(
        &a,
        methods::WATCH_CHATS,
        serde_json::json!({}),
        "chat Y row visible on A",
        |item| {
            item.as_array()?
                .iter()
                .find(|chat| chat.get("id").and_then(|v| v.as_str()) == Some(chat_y.as_str()))
                .map(|_| ())
        },
    )
    .await;
    pass("chat Y registry row synced to A");

    // ── Drive the real MCP bridge over stdio, as chat X's agent ────────────
    let mut bridge = tokio::process::Command::new(&zeron_bin)
        .arg("mcp-bridge")
        .env("ZERON_CHAT_ID", &chat_x)
        .env("ZERON_IPC_PORT", a_port.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|err| fail(&format!("spawn mcp-bridge: {err}")));
    let mut stdin = bridge.stdin.take().expect("bridge stdin");
    let stdout = BufReader::new(bridge.stdout.take().expect("bridge stdout"));
    let mut lines = stdout.lines();

    async fn rpc(
        stdin: &mut tokio::process::ChildStdin,
        lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
        req: serde_json::Value,
    ) -> serde_json::Value {
        stdin
            .write_all((req.to_string() + "\n").as_bytes())
            .await
            .unwrap_or_else(|err| fail(&format!("bridge stdin write: {err}")));
        stdin.flush().await.ok();
        loop {
            match tokio::time::timeout(STEP_TIMEOUT, lines.next_line()).await {
                Ok(Ok(Some(line))) => {
                    let value: serde_json::Value = match serde_json::from_str(&line) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    if value.get("id") == req.get("id") {
                        return value;
                    }
                }
                Ok(Ok(None)) => fail("bridge stdout closed"),
                Ok(Err(err)) => fail(&format!("bridge stdout read: {err}")),
                Err(_) => fail("bridge reply timed out"),
            }
        }
    }

    let init = rpc(&mut stdin, &mut lines, serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "e2e", "version": "0" } }
    }))
    .await;
    if init["result"]["serverInfo"]["name"] != "zeron" {
        fail(&format!("unexpected initialize reply: {init}"));
    }
    pass("mcp-bridge initialized");

    let tools = rpc(
        &mut stdin,
        &mut lines,
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
        }),
    )
    .await;
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .map(|t| t.iter().filter_map(|x| x["name"].as_str()).collect())
        .unwrap_or_default();
    if !names.contains(&"send_to_session") || !names.contains(&"list_sessions") {
        fail(&format!("tools/list missing tools: {names:?}"));
    }
    pass("tools/list exposes send_to_session + list_sessions");

    let listed = rpc(
        &mut stdin,
        &mut lines,
        serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "list_sessions", "arguments": {} }
        }),
    )
    .await;
    let listing = listed["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    if !listing.contains(&chat_y) {
        fail(&format!("list_sessions does not show chat Y: {listing}"));
    }
    pass("list_sessions shows chat Y");

    const MESSAGE: &str = "hello from X: please report status";
    let sent = rpc(&mut stdin, &mut lines, serde_json::json!({
        "jsonrpc": "2.0", "id": 4, "method": "tools/call",
        "params": { "name": "send_to_session", "arguments": { "target_chat_id": chat_y, "message": MESSAGE } }
    }))
    .await;
    if sent["result"]["isError"].as_bool().unwrap_or(true) {
        fail(&format!("send_to_session errored: {sent}"));
    }
    pass("send_to_session accepted");
    drop(stdin);
    let _ = bridge.kill().await;

    // ── Prove delivery on B: user turn attributed agent:{X}, mock reply ────
    let mut transcript: Vec<zeron_doc::SessionMessageEntry> = Vec::new();
    wait_stream(
        &b,
        methods::WATCH_DOC_MESSAGES,
        serde_json::json!({ "chatId": chat_y }),
        "agent message + reply on B",
        |item| {
            let frame: zeron_doc::TranscriptFrame = serde_json::from_value(item.clone()).ok()?;
            zeron_doc::apply_transcript_frame(&mut transcript, frame).ok()?;
            let user_ok = transcript.iter().any(|e| {
                e.role == zeron_doc::MessageRole::User
                    && e.user_id.as_deref() == Some(&format!("agent:{chat_x}"))
                    && serde_json::to_string(e)
                        .unwrap_or_default()
                        .contains(MESSAGE)
            });
            let reply_ok = transcript.iter().any(|e| {
                e.role == zeron_doc::MessageRole::Assistant
                    && e.status == Some(zeron_doc::MessageStatus::Complete)
            });
            (user_ok && reply_ok).then_some(())
        },
    )
    .await;
    pass(&format!(
        "chat Y executed the agent send (user turn attributed agent:{chat_x}, mock reply complete)"
    ));

    println!("PASS: agent-to-agent send e2e complete");
}
