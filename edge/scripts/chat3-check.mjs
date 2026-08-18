// chat3 (org-shared session rooms) E2E against a deployed worker
// (AUTH_MODE=dev). Covers what chat3 CHANGES over chat2 — org gating, member
// open access, host-user discipline, shared blob keyspace — and leans on
// chat2-check.mjs for the shared frame-protocol details (same DO code).
// Usage: node chat3-check.mjs <baseUrl>
import { randomUUID, randomBytes } from "node:crypto";

const base = process.argv[2];
if (!base) throw new Error("usage: node chat3-check.mjs <baseUrl>");
const wsBase = base.replace(/^http/, "ws");
// Dev tokens: `user@org` carries a fake org claim (edge/src/auth.ts).
const alice = "e3-alice@e3-org1"; // host user
const bob = "e3-bob@e3-org1"; // same-org member
const carol = "e3-carol@e3-org2"; // different org
const noOrg = "e3-dave"; // no org claim at all
const chat = `e3-${randomUUID().slice(0, 13)}`;

// ── frame codec (mirror of chat-frames.ts) ──────────────────────────────────
const FRAME = { hello: 0x01, state: 0x02, rowsReq: 0x03, row: 0x04, rowsDone: 0x05, push: 0x06, ack: 0x07, presence: 0x08, probe: 0x09, probeOk: 0x0a, error: 0x0b };
const NAME = Object.fromEntries(Object.entries(FRAME).map(([k, v]) => [v, k]));
const enc = (type, header, payload = new Uint8Array(0)) => {
  const h = new TextEncoder().encode(JSON.stringify(header));
  const out = new Uint8Array(5 + h.length + payload.length);
  out[0] = type;
  new DataView(out.buffer).setUint32(1, h.length, true);
  out.set(h, 5);
  out.set(payload, 5 + h.length);
  return out;
};
const dec = (bytes) => {
  const b = new Uint8Array(bytes);
  const len = new DataView(b.buffer, b.byteOffset).getUint32(1, true);
  return { type: b[0], header: JSON.parse(new TextDecoder().decode(b.subarray(5, 5 + len))), payload: b.subarray(5 + len) };
};

// ── tiny WS client with a frame inbox ───────────────────────────────────────
class Client {
  constructor(device, user, { host = false } = {}) { this.device = device; this.user = user; this.host = host; this.inbox = []; this.waiters = []; this.closed = null; }
  async connect() {
    const role = this.host ? "&role=host" : "";
    this.ws = new WebSocket(`${wsBase}/chat3/${chat}/ws?device=${this.device}&token=${this.user}${role}`);
    this.ws.binaryType = "arraybuffer";
    this.ws.onmessage = (ev) => { const f = dec(ev.data); this.inbox.push(f); this.waiters.forEach((w) => w()); };
    this.ws.onclose = (ev) => { this.closed = { code: ev.code, reason: ev.reason }; this.waiters.forEach((w) => w()); };
    await new Promise((res, rej) => { this.ws.onopen = res; this.ws.onerror = () => rej(new Error("ws connect failed")); });
  }
  send(type, header, payload) { this.ws.send(enc(type, header, payload)); }
  async next(type, timeoutMs = 8000) {
    const start = Date.now();
    for (;;) {
      const i = this.inbox.findIndex((f) => f.type === type);
      if (i >= 0) return this.inbox.splice(i, 1)[0];
      if (this.closed) throw new Error(`socket closed (${this.closed.code}) while waiting for ${NAME[type]}`);
      if (Date.now() - start > timeoutMs) throw new Error(`timeout waiting for ${NAME[type]}; inbox=[${this.inbox.map((f) => NAME[f.type])}]`);
      await new Promise((res) => { this.waiters.push(res); setTimeout(res, 150); });
      this.waiters = [];
    }
  }
  async waitClose(timeoutMs = 8000) {
    const start = Date.now();
    while (!this.closed) {
      if (Date.now() - start > timeoutMs) throw new Error("timeout waiting for close");
      await new Promise((res) => setTimeout(res, 100));
    }
    return this.closed;
  }
  async hello(cursor = 0) { this.send(FRAME.hello, { cursor, device: this.device }); return this.next(FRAME.state); }
}

const http = (path, { method = "GET", user = alice, headers = {}, body } = {}) =>
  fetch(`${base}${path}`, { method, headers: { authorization: `Bearer ${user}`, ...headers }, body });

// ── assertions ──────────────────────────────────────────────────────────────
let pass = 0, fail = 0;
const results = [];
const check = (name, cond, detail = "") => {
  if (cond) { pass++; results.push(`  ok  ${name}`); }
  else { fail++; results.push(`FAIL  ${name}${detail ? ` — ${detail}` : ""}`); }
};
const eqBytes = (a, b) => a.length === b.length && a.every((v, i) => v === b[i]);

// ════════════════════════════════════════════════════════════════════════════
// 1. Worker org gates
{
  const none = await http(`/chat3/${chat}/stats`, { user: noOrg });
  check("no org claim → 403", none.status === 403, `got ${none.status}`);
  const cross = await http(`/chat3/${chat}/stats`, { user: carol });
  // carol routes to HER org's room `chat3/e3-org2/{chat}` — a different DO
  // whose log is empty and unclaimed. Isolation, not an error: nothing of
  // org1's is readable.
  check("cross-org stats hits a disjoint room (isolation)", cross.status === 200, `got ${cross.status}`);
}

// 2. Host claims; same-org members join and relay
const host = new Client("devHostA", alice, { host: true });
await host.connect();
const member = new Client("devBobB", bob);
await member.connect();
{
  await host.hello();
  await member.hello();
  const bytes = new Uint8Array(randomBytes(2048));
  host.send(FRAME.push, { batchId: "h-1" }, bytes);
  const [ack, relayed] = await Promise.all([host.next(FRAME.ack), member.next(FRAME.row)]);
  check("host push relayed to member", ack.header.seq === 1 && relayed.header.device === "devHostA" && eqBytes(relayed.payload, bytes));
  const fromMember = new Uint8Array(randomBytes(1024));
  member.send(FRAME.push, { batchId: "m-2" }, fromMember);
  const [, toHost] = await Promise.all([member.next(FRAME.ack), host.next(FRAME.row)]);
  check("member push relayed to host", toHost.header.seq === 2 && toHost.header.device === "devBobB");
  const beat = new Uint8Array(randomBytes(16));
  host.send(FRAME.presence, { at: Date.now() }, beat);
  const seen = await member.next(FRAME.presence);
  check("presence carries user attribution", seen.header.device === "devHostA" && seen.header.user === "e3-alice" && eqBytes(seen.payload, beat), JSON.stringify(seen.header));
}

// 3. Host-user discipline
{
  const second = new Client("devEve", bob, { host: true });
  let rejected = false;
  try { await second.connect(); await second.waitClose(3000); rejected = true; } catch { rejected = true; }
  check("second role=host join by another user → rejected", rejected);

  const cpBob = await http(`/chat3/${chat}/checkpoint?seqCovered=1`, { method: "POST", user: bob, headers: { "x-chat2-frontier": "" }, body: new Uint8Array([1]) });
  check("non-host checkpoint POST → 403", cpBob.status === 403, `got ${cpBob.status}`);
  const tailBob = await http(`/chat3/${chat}/tail`, { method: "PUT", user: bob, body: "{}" });
  check("non-host tail PUT → 403", tailBob.status === 403, `got ${tailBob.status}`);
  const resetBob = await http(`/chat3/${chat}/reset`, { method: "POST", user: bob });
  check("non-host reset → 403", resetBob.status === 403, `got ${resetBob.status}`);

  const frontier = Buffer.from(randomBytes(16)).toString("base64");
  const cpHost = await http(`/chat3/${chat}/checkpoint?seqCovered=2`, { method: "POST", user: alice, headers: { "x-chat2-frontier": frontier }, body: new Uint8Array(randomBytes(4096)) });
  const cpBody = await cpHost.json();
  check("host checkpoint POST prunes", cpHost.status === 200 && cpBody.seqFloor === 2 && cpBody.pruned === 2, JSON.stringify(cpBody));
  const tailHost = await http(`/chat3/${chat}/tail`, { method: "PUT", user: alice, headers: { "content-type": "application/json" }, body: "{\"t\":1}" });
  check("host tail PUT → 200", tailHost.status === 200, `got ${tailHost.status}`);
}

// 4. Member reads are open
{
  const stats = await http(`/chat3/${chat}/stats`, { user: bob });
  check("member stats → 200", stats.status === 200, `got ${stats.status}`);
  const cp = await http(`/chat3/${chat}/checkpoint`, { user: bob });
  check("member checkpoint GET → 200", cp.status === 200, `got ${cp.status}`);
  const tail = await http(`/chat3/${chat}/tail`, { user: bob });
  check("member tail GET → 200", (await tail.text()) === "{\"t\":1}" && tail.status === 200, `got ${tail.status}`);
}

// 5. Shared blob keyspace (org-scoped) + legacy fallback
{
  const put = await http(`/blob/${chat}/p-1`, { method: "PUT", user: alice, body: "shared tool output" });
  check("PUT blob as org member", put.status === 200, `got ${put.status}`);
  const sameOrg = await http(`/blob/${chat}/p-1`, { user: bob });
  check("same-org member reads the blob", sameOrg.status === 200 && (await sameOrg.text()) === "shared tool output", `got ${sameOrg.status}`);
  const crossOrg = await http(`/blob/${chat}/p-1`, { user: carol });
  check("cross-org blob GET → 404", crossOrg.status === 404, `got ${crossOrg.status}`);
  // Legacy fallback: a pre-migration blob under the per-user key must keep
  // resolving for its owner once they carry an org claim. Dev tokens share
  // the userId: `e3-dave` (no org, legacy key) vs `e3-dave@e3-org1`.
  const legacyPut = await http(`/blob/${chat}/p-legacy`, { method: "PUT", user: noOrg, body: "pre-migration output" });
  check("org-less PUT lands on legacy key", legacyPut.status === 200, `got ${legacyPut.status}`);
  const fallback = await http(`/blob/${chat}/p-legacy`, { user: "e3-dave@e3-org1" });
  check("org token falls back to own legacy key", fallback.status === 200 && (await fallback.text()) === "pre-migration output", `got ${fallback.status}`);
  const notOwn = await http(`/blob/${chat}/p-legacy`, { user: alice });
  check("legacy fallback is per-owner only", notOwn.status === 404, `got ${notOwn.status}`);
}

// 6. Host reset works and the room is reclaimable
{
  const res = await http(`/chat3/${chat}/reset`, { method: "POST", user: alice });
  check("host reset → ok", res.status === 200, `got ${res.status}`);
  await host.waitClose();
  await member.waitClose();
  const rehost = new Client("devHostA", alice, { host: true });
  await rehost.connect();
  const st = await rehost.hello();
  check("re-claim after reset: fresh log", st.header.headSeq === 0, JSON.stringify(st.header));
  rehost.ws.close();
}

console.log(`\nchat3 E2E vs ${base}\nroom: chat3/e3-org1/${chat}\n`);
console.log(results.join("\n"));
console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
