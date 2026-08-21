/**
 * End-to-end smoke test against a running `wrangler dev` instance
 * (AUTH_MODE=dev). Exercises the non-chat design surface:
 *   1. device room relays client↔host frames and serves sidecar slots
 *   2. nudges deliver live and replay from the offline queue
 *   3. absorbed /auth routes: 501 without WORKOS_API_KEY; cli callback page
 * (Chat-room coverage lives in scripts/chat3-check.mjs and the vitest tiers.)
 *
 * Usage: node scripts/smoke.mjs [baseUrl]   (default http://127.0.0.1:27640)
 */
import { randomUUID } from "node:crypto";

const base = process.argv[2] ?? "http://127.0.0.1:27640";
const wsBase = base.replace(/^http/, "ws");
const token = "smoke-user";
const deviceId = `smokedev-${randomUUID().slice(0, 8)}`;

const fail = (msg) => {
  console.error(`✗ ${msg}`);
  process.exit(1);
};
const ok = (msg) => console.log(`✓ ${msg}`);
const until = async (fn, what, ms = 8000) => {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  fail(`timeout waiting for ${what}`);
};

// ── health ────────────────────────────────────────────────────────────────
{
  const res = await fetch(`${base}/health`);
  const body = await res.json();
  if (!body.ok) fail("health");
  if (body.auth !== "dev") fail(`expected dev auth mode, got ${body.auth} — run wrangler dev with --var AUTH_MODE:dev`);
  ok("health (dev auth)");
}

// ── device room ───────────────────────────────────────────────────────────
{
  const { encodeDeviceFrame, decodeDeviceFrame } = await import("./device-frame.mjs");
  const host = new WebSocket(`${wsBase}/device/${deviceId}/ws?token=${token}&role=host`);
  host.binaryType = "arraybuffer";
  await new Promise((resolve, reject) => {
    host.onopen = resolve;
    host.onerror = reject;
  });
  const hostFrames = [];
  host.onmessage = (e) => {
    if (typeof e.data === "string") return;
    const frame = decodeDeviceFrame(new Uint8Array(e.data));
    hostFrames.push(frame);
    // echo rpc payloads back to the sender
    if (frame.header.k === "rpc" && frame.header.from) {
      host.send(
        encodeDeviceFrame(
          { s: frame.header.s, k: "rpc", to: frame.header.from },
          frame.payload
        )
      );
    }
  };

  const connId = "conn-1";
  const client = new WebSocket(`${wsBase}/device/${deviceId}/ws?token=${token}&role=client&connId=${connId}`);
  client.binaryType = "arraybuffer";
  await new Promise((resolve, reject) => {
    client.onopen = resolve;
    client.onerror = reject;
  });
  const clientFrames = [];
  client.onmessage = (e) => {
    if (typeof e.data === "string") return;
    clientFrames.push(decodeDeviceFrame(new Uint8Array(e.data)));
  };
  client.send(encodeDeviceFrame({ s: "rpc-1", k: "rpc" }, new TextEncoder().encode("hello-host")));
  await until(() => clientFrames.length > 0, "device rpc echo");
  const echoed = new TextDecoder().decode(clientFrames[0].payload);
  if (echoed !== "hello-host") fail(`device echo got ${echoed}`);
  ok("device room rpc echo (client→host→client)");

  // intruder cannot join the device room
  const evil = new WebSocket(`${wsBase}/device/${deviceId}/ws?token=evil&role=client`);
  const evilResult = await new Promise((resolve) => {
    evil.onopen = () => resolve("open");
    evil.onerror = () => resolve("error");
    setTimeout(() => resolve("timeout"), 3000);
  });
  if (evilResult === "open") fail("intruder joined device room");
  ok("device room ownership enforced");

  // sidecar slot
  const post = await fetch(`${base}/device/${deviceId}/sidecar/repos?token=${token}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ repos: [{ path: "/x", name: "x" }] })
  });
  if (post.status !== 200) fail(`sidecar post ${post.status}`);
  const got = await (await fetch(`${base}/device/${deviceId}/sidecar/repos?token=${token}`)).json();
  if (got.repos?.[0]?.name !== "x") fail("sidecar round-trip");
  ok("device sidecar slot round-trip");

  // nudge: live delivery to the connected host
  const nudge = await fetch(`${base}/device/${deviceId}/nudge?token=${token}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ chatId: "chat-live" })
  });
  if ((await nudge.json()).delivered !== true) fail("live nudge not delivered");
  await until(
    () => hostFrames.some((f) => f.header.k === "nudge" && new TextDecoder().decode(f.payload).includes("chat-live")),
    "live nudge frame"
  );
  ok("nudge delivered live to connected host");

  host.close();
  client.close();

  // nudge: queued while host offline, replayed on rejoin
  await new Promise((r) => setTimeout(r, 200)); // let the close land
  const queued = await fetch(`${base}/device/${deviceId}/nudge?token=${token}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ chatId: "chat-cold" })
  });
  if ((await queued.json()).queued !== true) fail("offline nudge not queued");
  const host2 = new WebSocket(`${wsBase}/device/${deviceId}/ws?token=${token}&role=host`);
  host2.binaryType = "arraybuffer";
  const replayed = [];
  host2.onmessage = (e) => {
    if (typeof e.data === "string") return;
    replayed.push(decodeDeviceFrame(new Uint8Array(e.data)));
  };
  await until(
    () => replayed.some((f) => f.header.k === "nudge" && new TextDecoder().decode(f.payload).includes("chat-cold")),
    "queued nudge replay on host join"
  );
  ok("nudge queued offline and replayed on host join");
  host2.close();
}

// ── absorbed auth routes ──────────────────────────────────────────────────
{
  // Dev instances have no WORKOS_API_KEY: secret-bearing routes answer 501
  // (matching the old apps/server behavior when WorkOS is unconfigured).
  const exchange = await fetch(`${base}/auth/exchange`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ code: "test" })
  });
  if (exchange.status !== 501) fail(`auth exchange expected 501 in dev, got ${exchange.status}`);
  const refresh = await fetch(`${base}/auth/refresh`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ refreshToken: "test" })
  });
  if (refresh.status !== 501) fail(`auth refresh expected 501 in dev, got ${refresh.status}`);
  ok("auth exchange/refresh answer 501 without WORKOS_API_KEY");

  // The headless callback needs no WorkOS config: it just renders state.code.
  const cb = await fetch(`${base}/auth/cli/callback?code=abc123&state=xyz789`);
  if (cb.status !== 200) fail(`cli callback ${cb.status}`);
  const page = await cb.text();
  if (!page.includes("xyz789.abc123")) fail("cli callback paste code missing");
  const cbBad = await fetch(`${base}/auth/cli/callback`);
  if (cbBad.status !== 400) fail(`cli callback without code expected 400, got ${cbBad.status}`);
  ok("auth cli callback renders paste code");
}

console.log("\nALL SMOKE TESTS PASSED");
process.exit(0);
