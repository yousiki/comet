/**
 * ChatRoom — one Durable Object per chat session (`chat2/{chatId}` legacy
 * single-owner, Organization-shared legacy `chat3` namespace), the dumb authenticated
 * log relay replacing SessionRoom's loro-aware s2 rooms (docs/chat2-sync.md
 * workstream B). Modeled line-for-line on RegistryRoom, NOT on SessionRoom:
 * no loro-wasm import anywhere in this class.
 *
 * The DO's entire job: append opaque update blobs to a seq-ordered log,
 * relay them to live sockets, store one client-built checkpoint blob, and
 * serve both back. All CRDT semantics live in the clients. Cold start is a
 * table read — the s2 wedge class (CPU-limited export/replay in the DO)
 * cannot exist here by construction.
 *
 * Sidecars (`/tail`, `/diff`) are host-PUBLISHED and served verbatim: the DO
 * never materializes anything.
 *
 * Hibernation discipline: ZERO wall-clock timers; ping/pong rides the
 * auto-response pair; the daily alarm does the nightly R2 backup only.
 */
import { createBlobStore, type BlobStore } from "./blobs";
import {
  appendRow,
  CHECKPOINT_BLOB,
  commitCheckpoint,
  ensureChatLog,
  FRONTIER_BLOB,
  getMeta,
  headSeq,
  logStats,
  MAX_ROW_BYTES,
  rowsAfter,
  setMeta
} from "./chat-log";
import { decodeFrame, encodeFrame, FRAME } from "./chat-frames";
import { authorizeChatRoom, type ChatOp } from "./chat-room-auth";
import {
  AUTH_USER_HEADER,
  LEGACY_ORGANIZATION_CHAT_ROOM_KIND,
  ROOM_KIND_HEADER,
  type Env
} from "./env";

const DAY_MS = 24 * 60 * 60 * 1000;
/** Inbound frame budget: one pushed row (+ header slack). */
const MAX_FRAME_BYTES = MAX_ROW_BYTES + 8192;
/** Host-published sidecar budget (tail JSON / diff payload). */
const MAX_SIDECAR_BYTES = 4 * 1024 * 1024;
/** Checkpoint upload budget — a REBUILT thin doc is ~KB-to-100s-of-KB scale;
 * 16MB is deliberate headroom over any sane doc, not a target. */
const MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024;
/** Presence beats older than this are swept before relay/stats. */
const PRESENCE_TTL_MS = 30_000;
/** Per-(user, device) push quota, rolling window (in-memory; resets on
 * hibernation — it exists to contain a runaway client loop, not to meter
 * honest traffic). */
const QUOTA_WINDOW_MS = 60_000;
const QUOTA_MAX_PUSHES = 300;
const QUOTA_MAX_BYTES = 8 * 1024 * 1024;

interface SocketState {
  userId: string;
  device: string;
  /** Historical serialized attachment field. Explicit on new sockets; missing
   * on pre-deploy attachments, where disabling excludeOwn is data-safe. */
  orgChat?: boolean;
  /** Set once a valid hello established the session. */
  ready?: boolean;
}

interface PushOutcome {
  ok: number;
  rejected: number;
  lastOkAt: number;
}

interface QuotaWindow {
  since: number;
  pushes: number;
  bytes: number;
}

export class ChatRoom implements DurableObject {
  private readonly ctx: DurableObjectState;
  private readonly env: Env;
  private readonly blobs: BlobStore;
  /** device → last presence beat (epoch ms). Memory-only by construction. */
  private readonly presence = new Map<string, number>();
  /** device → rolling push quota window. Memory-only. */
  private readonly quotas = new Map<string, QuotaWindow>();

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
    ensureChatLog(ctx.storage.sql);
    this.blobs = createBlobStore(ctx.storage.sql);
    // Runtime-answered keepalive; proves nothing about this DO's health.
    // Clients judge liveness by probe frames (same caveat as RegistryRoom).
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  // ── HTTP surface (only reachable through the authed Worker) ──────────────

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const userId = request.headers.get(AUTH_USER_HEADER);
    if (!userId) return json({ error: "unauthenticated" }, 401);

    const sql = this.ctx.storage.sql;
    // Organization-shared legacy `chat3` rooms use host-user discipline via
    // authorizeChatRoom; legacy `chat2`
    // rooms keep the single-owner discipline, claiming on the first
    // authenticated contact. The header is Worker-controlled
    // (deleted-then-set on every forward).
    const organizationSharedRoom =
      request.headers.get(ROOM_KIND_HEADER) === LEGACY_ORGANIZATION_CHAT_ROOM_KIND;
    const roleHost = url.searchParams.get("role") === "host";
    /** Drain an unread body before answering a denial: responding with the
     * request stream unconsumed makes workerd abort the connection ("Can't
     * read from request stream after response has been sent"), which kills
     * unrelated keep-alive requests riding the same socket. */
    const deny = async (response: Response): Promise<Response> => {
      try {
        await request.arrayBuffer();
      } catch {
        /* stream already gone */
      }
      return response;
    };
    /** null = allowed (a shared-room host claim is persisted as a side effect). */
    const gate = (op: ChatOp): Response | null => {
      if (organizationSharedRoom) {
        const verdict = authorizeChatRoom(op, userId, getMeta(sql, "hostUser") ?? null, roleHost);
        if (!verdict.allow) return json({ error: "forbidden" }, 403);
        if (verdict.claimHost) setMeta(sql, "hostUser", userId);
        return null;
      }
      // Claim on first authenticated contact, then stay owner-only forever.
      // The /rows twins must be able to claim too: otherwise a brand-new chat
      // deadlocks on networks where the WebSocket upgrade never succeeds.
      const owner = getMeta(sql, "owner");
      if (!owner) setMeta(sql, "owner", userId);
      else if (owner !== userId) return json({ error: "forbidden" }, 403);
      return null;
    };

    if (url.pathname === "/ws") {
      const denied = gate("join");
      if (denied) return deny(denied);
      const device = url.searchParams.get("device") ?? "";
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      // `orgChat` is retained in serialized attachments for hibernated-socket compatibility.
      const state: SocketState = { userId, device, orgChat: organizationSharedRoom };
      pair[1].serializeAttachment(state);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }

    if (url.pathname === "/checkpoint" && request.method === "POST") {
      const denied = gate("checkpointPost");
      if (denied) return deny(denied);
      const seqCovered = Number(url.searchParams.get("seqCovered") ?? "");
      if (!Number.isInteger(seqCovered) || seqCovered < 0) {
        return json({ error: "bad_seq_covered" }, 400);
      }
      const frontier = decodeBase64(request.headers.get("x-chat2-frontier") ?? "");
      if (frontier === undefined) return json({ error: "bad_frontier" }, 400);
      // A checkpoint that claims to cover rows must name its state: an empty
      // frontier label on a content-bearing checkpoint made every fresh
      // reader skip it and park all dependent rows invisibly ("Add Tweets"
      // incident, 2026-08-18). Empty stays legal only for seqCovered 0
      // (the M1 empty-doc seed).
      if (frontier.byteLength === 0 && seqCovered > 0) {
        return json({ error: "bad_frontier", message: "empty frontier on a content checkpoint" }, 400);
      }
      const body = new Uint8Array(await request.arrayBuffer());
      if (body.byteLength > MAX_CHECKPOINT_BYTES) return json({ error: "too_large" }, 413);
      const outcome = commitCheckpoint(sql, this.blobs, seqCovered, frontier, body, Date.now());
      if (!outcome.ok) return json({ error: outcome.error }, 409);
      this.markBackupDirty();
      return json({ ok: true, seqFloor: outcome.seqFloor, pruned: outcome.pruned });
    }

    if (url.pathname === "/checkpoint" && request.method === "GET") {
      const denied = gate("checkpointGet");
      if (denied) return deny(denied);
      const bytes = this.blobs.get(CHECKPOINT_BLOB);
      if (!bytes) return json({ error: "not_found" }, 404);
      // Range-resumable (bytes=N- only): a 1MB load over a 1.2Mbps link that
      // dies at byte 800k resumes, where s2's export-per-join restarted.
      const range = parseRangeStart(request.headers.get("range"));
      if (range !== null && range >= bytes.byteLength) {
        return new Response(null, {
          status: 416,
          headers: { "content-range": `bytes */${bytes.byteLength}` }
        });
      }
      const body = range !== null ? bytes.subarray(range) : bytes;
      const headers = new Headers({
        "content-type": "application/octet-stream",
        "content-length": String(body.byteLength),
        "accept-ranges": "bytes",
        "x-chat2-checkpoint-seq": getMeta(sql, "checkpointSeq") ?? "0"
      });
      if (range !== null) {
        headers.set(
          "content-range",
          `bytes ${range}-${bytes.byteLength - 1}/${bytes.byteLength}`
        );
      }
      return new Response(body, { status: range !== null ? 206 : 200, headers });
    }

    if (url.pathname === "/rows" && request.method === "GET") {
      const denied = gate("rowsGet");
      if (denied) return deny(denied);
      // Pull over plain HTTPS: one GET collapses connect → hello → state →
      // rowsReq → backfill (4+ serial round trips on a WS, and impossible on
      // networks that strip the upgrade) into a single request. The body is
      // u32-LE length-prefixed frames — state (frontier payload), rows after
      // `?after=`, rowsDone — byte-identical frame encoding to the WS path,
      // so clients reuse their existing decoder.
      const afterRaw = Number(url.searchParams.get("after") ?? "0");
      const after = Number.isInteger(afterRaw) && afterRaw >= 0 ? afterRaw : 0;
      const device = url.searchParams.get("device") ?? "";
      const exclude = excludedDevice(
        organizationSharedRoom,
        url.searchParams.get("excludeOwn") === "1",
        device
      );
      const stats = logStats(sql);
      const frontier = this.blobs.get(FRONTIER_BLOB) ?? new Uint8Array(0);
      const frames: Uint8Array[] = [
        encodeFrame(
          FRAME.state,
          {
            headSeq: stats.headSeq,
            seqFloor: stats.seqFloor,
            checkpointSeq: stats.checkpointSeq,
            checkpointSize: stats.checkpointSize,
            rowCount: stats.rowCount,
            rowBytes: stats.rowBytes
          },
          frontier
        )
      ];
      for (const row of rowsAfter(sql, after, exclude)) {
        frames.push(
          encodeFrame(
            FRAME.row,
            { seq: row.seq, device: row.device, batchId: row.batchId },
            row.bytes
          )
        );
      }
      frames.push(encodeFrame(FRAME.rowsDone, { headSeq: headSeq(sql) }));
      const total = frames.reduce((n, f) => n + 4 + f.length, 0);
      const body = new Uint8Array(total);
      const view = new DataView(body.buffer);
      let off = 0;
      for (const f of frames) {
        view.setUint32(off, f.length, true);
        body.set(f, off + 4);
        off += 4 + f.length;
      }
      return new Response(body, {
        headers: { "content-type": "application/octet-stream" }
      });
    }

    if (url.pathname === "/rows" && request.method === "POST") {
      const denied = gate("rowsPost");
      if (denied) return deny(denied);
      // Push over plain HTTPS — the WS push's fallback twin for networks
      // where the upgrade never completes. batchId dedupe (UNIQUE column)
      // makes at-least-once delivery exact-once in effect.
      const device = url.searchParams.get("device") ?? "";
      // `device` is caller-controlled even though `userId` is Worker-stamped.
      // Match the WS path's attribution so one Organization member cannot collide with
      // another member's quota window or incident ledger by reusing/omitting
      // a device id.
      const attribution = attributionKey({ userId, device });
      const batchId = url.searchParams.get("batchId") ?? "";
      if (batchId === "" || batchId.length > 128) {
        this.recordPush(attribution, false);
        return json({ error: "bad_push" }, 400);
      }
      const payload = new Uint8Array(await request.arrayBuffer());
      if (!this.admitQuota(attribution, payload.byteLength)) {
        this.recordPush(attribution, false);
        return json({ error: "quota" }, 429);
      }
      const outcome = appendRow(sql, device, batchId, payload, Date.now());
      if (!outcome.ok) {
        this.recordPush(attribution, false);
        return json({ error: outcome.error }, outcome.error === "too_large" ? 413 : 400);
      }
      this.recordPush(attribution, true);
      if (!outcome.dup) {
        this.markBackupDirty();
        // Live relay to every ready socket — a same-device socket would
        // re-import its own bytes as a Loro no-op, so no exclusion needed.
        for (const socket of this.ctx.getWebSockets()) {
          const socketState = socket.deserializeAttachment() as SocketState | null;
          if (!socketState?.ready) continue;
          send(socket, FRAME.row, { seq: outcome.seq, device, batchId }, payload);
        }
      }
      return json({ batchId, seq: outcome.seq, dup: outcome.dup });
    }

    if ((url.pathname === "/tail" || url.pathname === "/diff") && request.method === "PUT") {
      const denied = gate("sidecarPut");
      if (denied) return deny(denied);
      const name = url.pathname === "/tail" ? "sidecar-tail" : "sidecar-diff";
      const body = new Uint8Array(await request.arrayBuffer());
      if (body.byteLength > MAX_SIDECAR_BYTES) return json({ error: "too_large" }, 413);
      this.blobs.put(name, body);
      setMeta(sql, `${name}-type`, request.headers.get("content-type") ?? "application/json");
      return json({ ok: true, bytes: body.byteLength });
    }

    if ((url.pathname === "/tail" || url.pathname === "/diff") && request.method === "GET") {
      const denied = gate("sidecarGet");
      if (denied) return deny(denied);
      const name = url.pathname === "/tail" ? "sidecar-tail" : "sidecar-diff";
      const bytes = this.blobs.get(name);
      if (!bytes) return json({ error: "not_found" }, 404);
      return new Response(bytes, {
        headers: {
          "content-type": getMeta(sql, `${name}-type`) ?? "application/json",
          "content-length": String(bytes.byteLength)
        }
      });
    }

    if (url.pathname === "/stats" && request.method === "GET") {
      const denied = gate("stats");
      if (denied) return deny(denied);
      this.sweepPresence();
      return json({
        ...logStats(sql),
        connectedSockets: this.ctx.getWebSockets().length,
        presence: Object.fromEntries(this.presence),
        // The ONLY per-(user, device) attribution surface — kept from the
        // 2026-08-05 incident tooling (SessionRoom's /stats pushOutcomes).
        pushOutcomes: JSON.parse(getMeta(sql, "pushOutcomes") ?? "{}") as Record<
          string,
          PushOutcome
        >,
        lastBackupSeq: Number(getMeta(sql, "backupSeq") ?? "0")
      });
    }

    if (url.pathname === "/reset" && request.method === "POST") {
      const denied = gate("reset");
      if (denied) return deny(denied);
      // Operator wipe. Recovery is host-driven: the host detects
      // `headSeq < cursor` on its next hello and re-seeds via checkpoint —
      // same shape as the registry reset recipe.
      sql.exec("DELETE FROM rows");
      sql.exec("DELETE FROM meta");
      sql.exec("DELETE FROM blobs");
      for (const ws of this.ctx.getWebSockets()) {
        try {
          ws.close(4410, "chat room reset");
        } catch {
          /* already gone */
        }
      }
      return json({ ok: true });
    }

    return json({ error: "not found" }, 404);
  }

  // ── WebSocket protocol (binary frames, chat-frames.ts) ───────────────────

  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
    if (typeof message === "string") {
      ws.close(1003, "binary frames only");
      return;
    }
    if (message.byteLength > MAX_FRAME_BYTES) {
      ws.close(1009, "frame too large");
      return;
    }
    const frame = decodeFrame(new Uint8Array(message));
    const state = ws.deserializeAttachment() as SocketState;
    if (!frame) {
      send(ws, FRAME.error, { code: "bad_frame", message: "malformed frame" });
      return;
    }
    switch (frame.type) {
      case FRAME.hello:
        this.handleHello(ws, state, frame.header);
        return;
      case FRAME.rowsReq:
        this.handleRowsReq(ws, state, frame.header);
        return;
      case FRAME.push:
        this.handlePush(ws, state, frame.header, frame.payload);
        return;
      case FRAME.presence:
        this.handlePresence(ws, state, frame.header, frame.payload);
        return;
      case FRAME.probe:
        send(ws, FRAME.probeOk, { headSeq: headSeq(this.ctx.storage.sql) });
        return;
      default:
        send(ws, FRAME.error, { code: "bad_frame", message: `unexpected type ${frame.type}` });
    }
  }

  async webSocketClose(): Promise<void> {
    /* nothing buffered; rows are written synchronously on push */
  }

  async webSocketError(): Promise<void> {
    /* ditto */
  }

  private handleHello(ws: WebSocket, state: SocketState, header: Record<string, unknown>): void {
    // The URL `device` param (Worker-validated against ID_RE) is the ONLY
    // device source. The hello header's copy is ignored so attribution cannot
    // change after the authenticated upgrade. Quotas/outcomes also include
    // `userId`, and Organization-shared rooms disable raw-device `excludeOwn` below.
    void header;
    state.ready = true;
    ws.serializeAttachment(state);
    const sql = this.ctx.storage.sql;
    const stats = logStats(sql);
    const frontier = this.blobs.get(FRONTIER_BLOB) ?? new Uint8Array(0);
    // Metadata + frontier only — the CLIENT decides what to load next
    // (frontier included in local doc → rows only; not included → GET
    // /checkpoint first; cursor > headSeq → server lost state, re-seed).
    send(
      ws,
      FRAME.state,
      {
        headSeq: stats.headSeq,
        seqFloor: stats.seqFloor,
        checkpointSeq: stats.checkpointSeq,
        checkpointSize: stats.checkpointSize,
        rowCount: stats.rowCount,
        rowBytes: stats.rowBytes
      },
      frontier
    );
  }

  private handleRowsReq(ws: WebSocket, state: SocketState, header: Record<string, unknown>): void {
    if (!state.ready) {
      send(ws, FRAME.error, { code: "hello_first", message: "rows before hello" });
      return;
    }
    const after = typeof header.after === "number" && header.after >= 0 ? header.after : 0;
    // In an Organization-shared room `device` is caller-selected, not identity-bound. Another
    // member may legitimately or maliciously reuse it; filtering by the raw
    // value would hide that member's rows forever. Re-importing our own Loro
    // rows is a no-op, so chat3 disables this bandwidth-only optimization.
    // Old serialized attachments have `orgChat === undefined`; fail safe and
    // disable exclusion until their next reconnect stamps the room kind.
    const exclude = excludedDevice(state.orgChat, header.excludeOwn === true, state.device);
    const sql = this.ctx.storage.sql;
    for (const row of rowsAfter(sql, after, exclude)) {
      send(ws, FRAME.row, { seq: row.seq, device: row.device, batchId: row.batchId }, row.bytes);
    }
    send(ws, FRAME.rowsDone, { headSeq: headSeq(sql) });
  }

  private handlePush(
    ws: WebSocket,
    state: SocketState,
    header: Record<string, unknown>,
    payload: Uint8Array
  ): void {
    const batchId = typeof header.batchId === "string" ? header.batchId : "";
    // Push errors carry the batchId so clients can RETIRE permanently
    // rejected batches from their replay queues (an unretireable batch
    // replays on every reconnect forever — the wedge class this replaces).
    if (!state.ready || batchId === "" || batchId.length > 128) {
      this.recordPush(attributionKey(state), false);
      send(ws, FRAME.error, { code: "bad_push", message: "hello first / malformed push", batchId });
      return;
    }
    if (!this.admitQuota(attributionKey(state), payload.byteLength)) {
      this.recordPush(attributionKey(state), false);
      send(ws, FRAME.error, { code: "quota", message: "per-device push quota exceeded", batchId });
      return;
    }
    const sql = this.ctx.storage.sql;
    const outcome = appendRow(sql, state.device, batchId, payload, Date.now());
    if (!outcome.ok) {
      this.recordPush(attributionKey(state), false);
      send(ws, FRAME.error, {
        code: outcome.error,
        message: `push rejected: ${outcome.error}`,
        batchId
      });
      return;
    }
    this.recordPush(attributionKey(state), true);
    if (!outcome.dup) {
      this.markBackupDirty();
      // Live relay to every OTHER ready socket — the sender has its own
      // bytes; it gets the ack (contrast RegistryRoom, whose LWW merge means
      // the sender must see the merged truth — here bytes are opaque and
      // Loro convergence is the client's business).
      for (const socket of this.ctx.getWebSockets()) {
        if (socket === ws) continue;
        const socketState = socket.deserializeAttachment() as SocketState | null;
        if (!socketState?.ready) continue;
        send(
          socket,
          FRAME.row,
          { seq: outcome.seq, device: state.device, batchId },
          payload
        );
      }
    }
    send(ws, FRAME.ack, { batchId, seq: outcome.seq, dup: outcome.dup });
  }

  private handlePresence(
    ws: WebSocket,
    state: SocketState,
    header: Record<string, unknown>,
    payload: Uint8Array
  ): void {
    if (!state.ready || state.device === "") return;
    const at = typeof header.at === "number" ? header.at : Date.now();
    this.presence.set(state.device, at);
    this.sweepPresence();
    // Broadcast-only relay of the opaque payload — no EphemeralStore, no
    // storage; a device that joins later simply waits for the next beat.
    // `user` is additive (old clients ignore unknown header keys): shared
    // rooms need to know WHO is present, not just which device.
    for (const socket of this.ctx.getWebSockets()) {
      if (socket === ws) continue;
      const socketState = socket.deserializeAttachment() as SocketState | null;
      if (!socketState?.ready) continue;
      send(socket, FRAME.presence, { device: state.device, at, user: state.userId }, payload);
    }
  }

  private sweepPresence(): void {
    const horizon = Date.now() - PRESENCE_TTL_MS;
    for (const [device, at] of this.presence) {
      if (at < horizon) this.presence.delete(device);
    }
  }

  /** Rolling per-(user, device) quota. True = admitted. */
  private admitQuota(key: string, bytes: number): boolean {
    const now = Date.now();
    let window = this.quotas.get(key);
    if (!window || now - window.since > QUOTA_WINDOW_MS) {
      window = { since: now, pushes: 0, bytes: 0 };
      this.quotas.set(key, window);
    }
    window.pushes += 1;
    window.bytes += bytes;
    return window.pushes <= QUOTA_MAX_PUSHES && window.bytes <= QUOTA_MAX_BYTES;
  }

  private recordPush(key: string, ok: boolean): void {
    const sql = this.ctx.storage.sql;
    const outcomes = JSON.parse(getMeta(sql, "pushOutcomes") ?? "{}") as Record<
      string,
      PushOutcome
    >;
    const entry = outcomes[key] ?? { ok: 0, rejected: 0, lastOkAt: 0 };
    if (ok) {
      entry.ok += 1;
      entry.lastOkAt = Date.now();
    } else {
      entry.rejected += 1;
    }
    outcomes[key] = entry;
    setMeta(sql, "pushOutcomes", JSON.stringify(outcomes));
  }

  private markBackupDirty(): void {
    setMeta(this.ctx.storage.sql, "backupDirty", "1");
    void this.ctx.storage.getAlarm().then((existing) => {
      if (existing === null) void this.ctx.storage.setAlarm(Date.now() + DAY_MS);
    });
  }

  /** Daily alarm: nightly R2 backup, seq-monotonic so a reset-and-reseeding
   * room can never replace the last good copy with a hollow one. */
  async alarm(): Promise<void> {
    const sql = this.ctx.storage.sql;
    if (getMeta(sql, "backupDirty") !== "1") return; // idle: stop the chain
    const head = headSeq(sql);
    if (head > Number(getMeta(sql, "backupSeq") ?? "0")) {
      const rows = [...rowsAfter(sql, 0)].map((row) => ({
        seq: row.seq,
        device: row.device,
        batchId: row.batchId,
        bytes: encodeBase64(row.bytes)
      }));
      const checkpoint = this.blobs.get(CHECKPOINT_BLOB);
      const frontier = this.blobs.get(FRONTIER_BLOB);
      await this.env.BLOBS.put(
        `backup/chat2/${this.ctx.id.toString()}/latest.json`,
        JSON.stringify({
          at: Date.now(),
          ...logStats(sql),
          checkpoint: checkpoint ? encodeBase64(checkpoint) : null,
          frontier: frontier ? encodeBase64(frontier) : null,
          rows
        })
      );
      setMeta(sql, "backupSeq", String(head));
    }
    setMeta(sql, "backupDirty", "0");
  }
}

/** Quota/outcome attribution: `(user, device)` — two users on the same
 * device string (or a blank one) must never share a window or a ledger row. */
const attributionKey = (state: SocketState): string =>
  `${state.userId}:${state.device === "" ? "(unknown)" : state.device}`;

/** Legacy `chat2` may omit its own device's replay rows. Organization-shared `chat3` must
 * not: device ids are not bound to user identity, so a collision would hide a
 * foreign member's update. */
const excludedDevice = (
  organizationSharedRoom: boolean | undefined,
  requested: boolean,
  device: string
): string | undefined =>
  requested && organizationSharedRoom === false && device !== "" ? device : undefined;

const send = (
  ws: WebSocket,
  type: (typeof FRAME)[keyof typeof FRAME],
  header: Record<string, unknown>,
  payload?: Uint8Array
): void => {
  try {
    const frame = encodeFrame(type, header, payload);
    ws.send(frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength) as ArrayBuffer);
  } catch {
    /* socket already gone; hibernation API cleans it up */
  }
};

const json = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" }
  });

/** `bytes=N-` (open-ended resume) only; anything fancier is ignored → 200. */
const parseRangeStart = (header: string | null): number | null => {
  const match = header?.match(/^bytes=(\d+)-$/);
  if (!match) return null;
  const start = Number(match[1]);
  return Number.isSafeInteger(start) && start > 0 ? start : null;
};

const encodeBase64 = (bytes: Uint8Array): string => {
  let bin = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(bin);
};

/** Standard base64 (empty string ⇒ empty frontier). `undefined` = malformed. */
const decodeBase64 = (text: string): Uint8Array | undefined => {
  try {
    const bin = atob(text);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  } catch {
    return undefined;
  }
};
