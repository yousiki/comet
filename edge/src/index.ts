/**
 * Zeron-native edge Worker (design §2, ARCHITECTURE §6): JWT auth at the
 * edge, then forwarding into per-session, per-registry, and per-device
 * Durable Objects. Also serves content-addressed R2 attachments (§1.2) and
 * the absorbed WorkOS auth routes (formerly apps/server).
 *
 * Routes:
 *   GET  /health
 *   POST /auth/exchange               — WorkOS code → tokens
 *   POST /auth/refresh                — WorkOS refresh → fresh tokens
 *   GET  /auth/organizations          — caller's active Organization memberships
 *   POST /auth/organizations          — create Organization + admin membership
 *   /auth/orgs[...]                    — complete legacy alias (`{orgs}` list payload)
 *   GET  /auth/cli/callback           — headless sign-in paste-code page
 *   GET  /session/:chatId/ws          — loro-protocol room (wss upgrade)
 *   GET  /tail/:chatId                — L2 instant-open tail JSON (§5)
 *   GET  /diff/:chatId                — latest working-tree diff (§6.1)
 *   POST /diff/:chatId                — host publishes the diff sidecar
 *   GET  /snapshot/:chatId            — repair: read current doc snapshot
 *   POST /append/:chatId              — repair: merge-import a Loro update
 *   GET  /workspace/:organizationId/ws   — legacy workspace-doc `ws/*` room
 *   GET  /workspace/:organizationId/tail — legacy workspace-doc tail JSON
 *   GET  /registry/:organizationId/ws    — Organization registry (`reg2` legacy namespace)
 *   GET  /registry/:organizationId/stats — registry seq/rows/attribution
 *   GET  /registry/:organizationId/rows  — registry full-table repair read
 *   POST /registry/:organizationId/reset — registry operator wipe (self-healing)
 *   GET  /device/:deviceId/ws?role=   — device-room byte pipe (§8)
 *   GET  /device/:deviceId/sidecar/:name
 *   POST /device/:deviceId/sidecar/:name
 *   GET  /device/:deviceId/status
 *   PUT  /blob/:chatId/:partId        — tool-output sidecar (chat2-sync A2)
 *   GET  /blob/:chatId/:partId
 *   GET  /chat2/:chatId/ws            — chat2 log-relay room (wss, chat2-sync B)
 *   GET|POST /chat2/:chatId/checkpoint — client-built doc snapshot (Range-resumable GET)
 *   GET|POST /chat2/:chatId/rows      — plain-HTTPS log pull/push
 *   GET|PUT  /chat2/:chatId/tail      — host-published sidecars, served verbatim
 *   GET|PUT  /chat2/:chatId/diff
 *   GET  /chat2/:chatId/stats
 *   POST /chat2/:chatId/reset
 *   /chat3/:chatId/*                  — same surface as /chat2, Organization-shared;
 *                                       `chat3` is the retained legacy namespace
 *                                       reads/writes, host-user checkpoint
 */
import { authenticate } from "./auth";
import { handleAuthRoute } from "./auth-routes";
import {
  AUTH_ORGANIZATION_HEADER,
  AUTH_USER_HEADER,
  LEGACY_ORGANIZATION_CHAT_ROOM_KIND,
  LEGACY_WORKSPACE_ROOM_KIND,
  ROOM_KIND_HEADER,
  type Env,
  type RoomKind
} from "./env";
import { SessionRoom } from "./session-room";
import { DeviceRoom } from "./device-room";
import { RegistryRoom } from "./registry-room";
import { ChatRoom } from "./chat-room";
import installSh from "./install.sh";

export { SessionRoom, DeviceRoom, RegistryRoom, ChatRoom };

const ID_RE = /^[A-Za-z0-9_-]{1,128}$/;

/** `decodeURIComponent` that answers `undefined` for malformed %-escapes. */
const safeDecode = (segment: string): string | undefined => {
  try {
    return decodeURIComponent(segment);
  } catch {
    return undefined;
  }
};
/** Tool part ids are harness-minted (`tool-1`, `call_x`, `m1#c1`-style) —
 * wider than ID_RE but still no slashes, so a part id can't traverse keys. */
const PART_RE = /^[A-Za-z0-9._:#~-]{1,200}$/;
const MAX_TOOL_BLOB_BYTES = 1024 * 1024;

const json = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" }
  });

/** Forward into a DO with the verified user stamped on the request. */
const forward = (
  ns: DurableObjectNamespace,
  name: string,
  request: Request,
  userId: string,
  path: string,
  search?: string,
  roomKind?: RoomKind,
  organizationId?: string
): Promise<Response> => {
  const stub = ns.get(ns.idFromName(name));
  const url = new URL(request.url);
  url.pathname = path;
  if (search !== undefined) url.search = search;
  const headers = new Headers(request.headers);
  // Room kind and Organization are Worker-controlled signals. Clear inbound
  // values so only code reached after the Organization-membership check can
  // assert them; passthrough would let callers choose their own scope.
  headers.delete(ROOM_KIND_HEADER);
  headers.delete(AUTH_ORGANIZATION_HEADER);
  headers.set(AUTH_USER_HEADER, userId);
  if (roomKind) headers.set(ROOM_KIND_HEADER, roomKind);
  if (organizationId) headers.set(AUTH_ORGANIZATION_HEADER, organizationId);
  return stub.fetch(new Request(url.toString(), { ...requestInit(request), headers }));
};

const requestInit = (request: Request): RequestInit => ({
  method: request.method,
  body: request.body
});

/** Carry the dialing engine's `&device=` through to the DO (socket
 * attribution in logs — the 2026-08-04 deaf socket was only identifiable by
 * reverse-engineering rotating IPv6 privacy addresses). Validated so a
 * hand-crafted value can't inject into log lines or the DO's query. */
const deviceParam = (url: URL): string => {
  const device = url.searchParams.get("device") ?? "";
  return ID_RE.test(device) ? `&device=${device}` : "";
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);

    if (url.pathname === "/health") {
      return json({ ok: true, auth: env.AUTH_MODE === "dev" ? "dev" : "workos" });
    }

    // ── public install surface (also routed from zeron.sh): the
    //    `curl | sh` installer and the release artifacts it downloads ───────
    if (url.pathname === "/install.sh" && (request.method === "GET" || request.method === "HEAD")) {
      return new Response(request.method === "HEAD" ? null : installSh, {
        headers: {
          "content-type": "application/x-sh",
          "cache-control": "public, max-age=0, must-revalidate"
        }
      });
    }
    if (
      parts[0] === "releases" &&
      parts.length >= 2 &&
      (request.method === "GET" || request.method === "HEAD")
    ) {
      const key = decodeURIComponent(url.pathname.slice("/releases/".length));
      if (key.length === 0 || key.includes("..")) return json({ error: "bad request" }, 400);
      const object = await env.RELEASES.get(key);
      if (!object) return json({ error: "not_found" }, 404);
      // latest.txt / manifest.json flip on release; artifacts are immutable by name.
      const mutable = key.endsWith(".txt") || key.endsWith(".json");
      const headers = new Headers({
        "content-type": key.endsWith(".txt")
          ? "text/plain; charset=utf-8"
          : key.endsWith(".json")
            ? "application/json"
            : "application/octet-stream",
        "content-length": String(object.size),
        "cache-control": mutable ? "public, max-age=60" : "public, max-age=86400, immutable",
        etag: object.httpEtag
      });
      return new Response(request.method === "HEAD" ? null : object.body, { headers });
    }

    // ── WorkOS auth routes (pre-bearer: exchange/refresh/callback have no
    //    access token yet; the Organization routes verify the bearer themselves) ─
    const authRouted = await handleAuthRoute(request, env, url);
    if (authRouted) return authRouted;

    const auth = await authenticate(env, request);
    if (!auth) return json({ error: "unauthenticated" }, 401);

    // ── session rooms ───────────────────────────────────────────────────────
    if (parts[0] === "session" && parts[1] && ID_RE.test(parts[1]) && parts[2] === "ws") {
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return json({ error: "expected websocket" }, 426);
      }
      // `s2/` = the WorkOS staging→production identity break: rooms are
      // claim-on-first-join per user id, and prod issued a fresh id for
      // everyone — a new namespace lets prod identities claim fresh rooms
      // while hosts re-upload doc state from their local snapshots (same
      // playbook as `ws3` below). Frame-level room ids stay the bare chatId.
      return forward(
        env.SESSION_ROOMS,
        `s2/${parts[1]}`,
        request,
        auth.userId,
        "/ws",
        `?chatId=${parts[1]}${deviceParam(url)}`
      );
    }
    if (parts[0] === "tail" && parts[1] && ID_RE.test(parts[1]) && request.method === "GET") {
      return forward(env.SESSION_ROOMS, `s2/${parts[1]}`, request, auth.userId, "/tail", "");
    }
    if (parts[0] === "stats" && parts[1] && ID_RE.test(parts[1]) && request.method === "GET") {
      return forward(env.SESSION_ROOMS, `s2/${parts[1]}`, request, auth.userId, "/stats", "");
    }
    if (parts[0] === "diff" && parts[1] && ID_RE.test(parts[1])) {
      return forward(env.SESSION_ROOMS, `s2/${parts[1]}`, request, auth.userId, "/diff", "");
    }
    if (parts[0] === "snapshot" && parts[1] && ID_RE.test(parts[1]) && request.method === "GET") {
      return forward(env.SESSION_ROOMS, `s2/${parts[1]}`, request, auth.userId, "/snapshot", "");
    }
    if (parts[0] === "append" && parts[1] && ID_RE.test(parts[1]) && request.method === "POST") {
      return forward(env.SESSION_ROOMS, `s2/${parts[1]}`, request, auth.userId, "/append", "");
    }

    // ── chat2 rooms (docs/chat2-sync.md B): dumb log relays, one per chat.
    //    Claim-on-first-join ownership enforced in the DO (chat ids are
    //    client-minted). The DO handles /ws, /checkpoint (GET Range-resumable
    //    + POST floor-guarded), host-published /tail + /diff sidecars,
    //    /stats, /reset. ──────────────────────────────────────────────────────
    if (parts[0] === "chat2" && parts[1] && ID_RE.test(parts[1]) && parts[2]) {
      const room = `chat2/${parts[1]}`;
      if (parts[2] === "ws" && parts.length === 3) {
        if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
          return json({ error: "expected websocket" }, 426);
        }
        return forward(
          env.CHAT_ROOMS,
          room,
          request,
          auth.userId,
          "/ws",
          `?chatId=${parts[1]}${deviceParam(url)}`
        );
      }
      const routes: Record<string, string[]> = {
        checkpoint: ["GET", "POST"],
        // Pull/push over plain HTTPS (the airplane-wifi transport): GET
        // /rows?after= collapses connect→hello→state→rowsReq→backfill into
        // one round trip; POST /rows is the batchId-deduped push twin.
        rows: ["GET", "POST"],
        tail: ["GET", "PUT"],
        diff: ["GET", "PUT"],
        stats: ["GET"],
        reset: ["POST"]
      };
      if (parts.length === 3 && routes[parts[2]]?.includes(request.method)) {
        // Query carries through (`seqCovered` on POST /checkpoint), as do
        // headers (`x-chat2-frontier`, `range`).
        return forward(env.CHAT_ROOMS, room, request, auth.userId, `/${parts[2]}`, url.search);
      }
      return json({ error: "not found" }, 404);
    }

    // ── `chat3` legacy namespace: Organization-shared sessions. Same ChatRoom
    //    routes as `chat2`, but the room name carries the caller's verified
    //    Organization id. Every Organization member may join/push/read; only the
    //    host user (claimed via `?role=host` on join or bootstrap checkpoint)
    //    may checkpoint, publish sidecars, or reset. ─────────────────────────
    if (parts[0] === "chat3" && parts[1] && ID_RE.test(parts[1]) && parts[2]) {
      if (!auth.organizationId) return json({ error: "forbidden" }, 403);
      const room = `chat3/${auth.organizationId}/${parts[1]}`;
      if (parts[2] === "ws" && parts.length === 3) {
        if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
          return json({ error: "expected websocket" }, 426);
        }
        // `role` is caller-asserted intent, not authority: the DO only honors
        // it when the host-user slot is unclaimed or already the caller's.
        const role = url.searchParams.get("role") === "host" ? "&role=host" : "";
        return forward(
          env.CHAT_ROOMS,
          room,
          request,
          auth.userId,
          "/ws",
          `?chatId=${parts[1]}${deviceParam(url)}${role}`,
          LEGACY_ORGANIZATION_CHAT_ROOM_KIND,
          auth.organizationId
        );
      }
      const routes: Record<string, string[]> = {
        checkpoint: ["GET", "POST"],
        rows: ["GET", "POST"],
        tail: ["GET", "PUT"],
        diff: ["GET", "PUT"],
        stats: ["GET"],
        reset: ["POST"]
      };
      if (parts.length === 3 && routes[parts[2]]?.includes(request.method)) {
        return forward(
          env.CHAT_ROOMS,
          room,
          request,
          auth.userId,
          `/${parts[2]}`,
          url.search,
          LEGACY_ORGANIZATION_CHAT_ROOM_KIND,
          auth.organizationId
        );
      }
      return json({ error: "not found" }, 404);
    }

    // ── Legacy `/workspace` rooms: same SessionRoom DO class. The caller's
    //    WorkOS `org_id` wire claim must equal the URL Organization id,
    //    and the room itself is derived from the caller's OWN user id — the
    //    and the legacy workspace document is per-user; members of one
    //    Organization can never address each other's rooms. ────────────────
    if (parts[0] === "workspace" && parts[1] && ID_RE.test(parts[1])) {
      const organizationId = parts[1];
      if (auth.organizationId !== organizationId) return json({ error: "forbidden" }, 403);
      // `ws4` = the 2026-08-04 incident break: the ws3 instance's storage was
      // left with causally-broken update rows by the abort-thrash loop (acks
      // outran the debounced flush) and could not be trusted again even after
      // /reset-log; a name bump allocates a virgin DO. (`ws3` was the per-user
      // privacy break, `ws2` the spaces overhaul.) Legacy rooms are orphaned
      // (hibernated, ~zero cost). `/workspace/:organizationId/*` and `ws4`
      // are historical protocol identities; clients echo their own room ids.
      const room = `ws4/${organizationId}/${auth.userId}`;
      if (parts[2] === "ws") {
        if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
          return json({ error: "expected websocket" }, 426);
        }
        return forward(
          env.SESSION_ROOMS,
          room,
          request,
          auth.userId,
          "/ws",
          `?chatId=${encodeURIComponent(room)}${deviceParam(url)}`,
          LEGACY_WORKSPACE_ROOM_KIND
        );
      }
      if (parts[2] === "tail" && request.method === "GET") {
        return forward(
          env.SESSION_ROOMS,
          room,
          request,
          auth.userId,
          "/tail",
          "",
          LEGACY_WORKSPACE_ROOM_KIND
        );
      }
      // Observability for the per-user legacy workspace room. Organization
      // membership is already checked; the DO bypasses the owner gate for
      // this historical room kind.
      if (parts[2] === "stats" && request.method === "GET") {
        return forward(
          env.SESSION_ROOMS,
          room,
          request,
          auth.userId,
          "/stats",
          "",
          LEGACY_WORKSPACE_ROOM_KIND
        );
      }
      // Raw doc snapshot: the repair/reseed read (2026-08-04: a device stranded
      // behind the shallow-locked rebuild converges by replacing its local
      // legacy workspace document with this — see the incident repair recipe).
      if (parts[2] === "snapshot" && request.method === "GET") {
        return forward(
          env.SESSION_ROOMS,
          room,
          request,
          auth.userId,
          "/snapshot",
          "",
          LEGACY_WORKSPACE_ROOM_KIND
        );
      }
      // Operator wedge-break: clear a legacy workspace room whose update log
      // grew enough to CPU-reset the DO on every cold start (Organization membership already
      // checked; state re-uploads from each device's local doc on rejoin).
      if (parts[2] === "reset-log" && request.method === "POST") {
        return forward(
          env.SESSION_ROOMS,
          room,
          request,
          auth.userId,
          "/reset-log",
          "",
          LEGACY_WORKSPACE_ROOM_KIND
        );
      }
      // Merge-safe repair write (the chat rooms' /append, for the legacy workspace
      // doc): lets an operator seed a reset room with ONE compact
      // locally-exported history blob instead of waiting for every device to
      // re-upload its whole doc — the N-way redundant re-seed is what kept
      // ballooning the update log after the 2026-08-05 wedge breaks.
      if (parts[2] === "append" && request.method === "POST") {
        return forward(
          env.SESSION_ROOMS,
          room,
          request,
          auth.userId,
          "/append",
          "",
          LEGACY_WORKSPACE_ROOM_KIND
        );
      }
    }

    // ── Registry rooms: row-table replacement for the legacy Loro workspace
    //    document. The Organization claim must match the URL. `reg2` is the
    //    retained Organization-shared namespace: one room per Organization
    //    (after per-user `reg1/{organizationId}/{userId}`) — every member reads and
    //    writes the same table, which is what makes shared sessions visible in
    //    every member's sidebar. Old reg1 rooms orphan (hibernated, ~zero
    //    cost); clients detect the seq regression on their next hello and
    //    re-seed the new room from local rows, automatically. ───────────────
    if (parts[0] === "registry" && parts[1] && ID_RE.test(parts[1])) {
      const organizationId = parts[1];
      if (auth.organizationId !== organizationId) return json({ error: "forbidden" }, 403);
      const room = `reg2/${organizationId}`;
      if (parts[2] === "ws") {
        if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
          return json({ error: "expected websocket" }, 426);
        }
        return forward(
          env.REGISTRY_ROOMS,
          room,
          request,
          auth.userId,
          "/ws",
          `?${deviceParam(url).replace(/^&/, "")}`
        );
      }
      if (parts[2] === "stats" && request.method === "GET") {
        return forward(env.REGISTRY_ROOMS, room, request, auth.userId, "/stats", "");
      }
      // Pull over plain HTTPS: `?since=` returns the same delta the WS
      // hello would (full table without it — the original repair read).
      // One round trip on any network that passes HTTPS at all, where the
      // WS upgrade needs 4 and a cooperative middlebox.
      if (parts[2] === "rows" && request.method === "GET") {
        return forward(env.REGISTRY_ROOMS, room, request, auth.userId, "/rows", url.search);
      }
      // Push over plain HTTPS — the WS push's fallback twin (LWW clocks
      // make replays no-ops, so at-least-once delivery is safe).
      if (parts[2] === "push" && request.method === "POST") {
        return forward(env.REGISTRY_ROOMS, room, request, auth.userId, "/push", url.search);
      }
      // Operator wipe. Unlike the CRDT rooms this needs no recipe: clients
      // detect the seq regression on their next hello and re-seed the table
      // from local rows with original clocks, automatically.
      if (parts[2] === "reset" && request.method === "POST") {
        return forward(env.REGISTRY_ROOMS, room, request, auth.userId, "/reset", "");
      }
    }

    // ── device rooms ────────────────────────────────────────────────────────
    if (parts[0] === "device" && parts[1] && ID_RE.test(parts[1])) {
      const deviceId = parts[1];
      if (parts[2] === "ws") {
        if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
          return json({ error: "expected websocket" }, 426);
        }
        const role = url.searchParams.get("role") === "host" ? "host" : "client";
        const connId = url.searchParams.get("connId") ?? crypto.randomUUID();
        // `d2/` is the retained identity namespace. The Organization claim
        // rides along so the room can record its owner's Organization for the
        // nudge authorization gate.
        return forward(
          env.DEVICE_ROOMS,
          `d2/${deviceId}`,
          request,
          auth.userId,
          "/ws",
          `?role=${role}&connId=${encodeURIComponent(connId)}`,
          undefined,
          auth.organizationId
        );
      }
      if (parts[2] === "sidecar" && parts[3] && /^[a-z0-9-]{1,64}$/.test(parts[3])) {
        return forward(env.DEVICE_ROOMS, `d2/${deviceId}`, request, auth.userId, `/sidecar/${parts[3]}`, "");
      }
      if (parts[2] === "status") {
        return forward(env.DEVICE_ROOMS, `d2/${deviceId}`, request, auth.userId, "/status", "");
      }
      // Durable command nudge (§7): "chat X has pending commands — open its
      // doc". Delivered live if the host is connected, else queued in the DO
      // and replayed on the host's next join. For Organization-shared sessions,
      // any member of the owner's Organization may nudge (the payload is only a chat id; the host
      // validates against its own doc before executing anything).
      if (parts[2] === "nudge" && request.method === "POST") {
        return forward(
          env.DEVICE_ROOMS,
          `d2/${deviceId}`,
          request,
          auth.userId,
          "/nudge",
          "",
          undefined,
          auth.organizationId
        );
      }
    }

    // ── R2 tool-output sidecar (docs/chat2-sync.md A2): full tool outputs
    //    and diffs live here, keyed `{chatId}/{partId}[.diff]`; the doc keeps
    //    only a one-line summary + this key. Straight R2, no DO involvement —
    //    the doc stays thin whether or not these uploads land. Per-user
    //    prefix = owner auth. ─────────────────────────────────────────────
    if (parts[0] === "blob" && parts.length === 3 && ID_RE.test(parts[1])) {
      // Percent-decode the part segment before validating: PART_RE allows
      // `#` (`m1#c1`-style harness ids), which HTTP clients cannot send raw
      // (fragment delimiter) — the host percent-encodes it. Decode-then-
      // validate keeps traversal shut: `%2F` decodes to `/`, fails PART_RE.
      const partId = safeDecode(parts[2]);
      if (partId === undefined || !PART_RE.test(partId)) {
        return json({ error: "bad part id" }, 400);
      }
      // Organization-scoped legacy `blob3/` keyspace lets every member of a
      // shared session resolve the same tool outputs. Organization-less development callers keep
      // the per-user prefix. Reads fall back to the caller's own legacy
      // per-user key so pre-migration outputs keep resolving (miss-path only).
      const legacyKey = `blob/${auth.userId}/${parts[1]}/${partId}`;
      const key = auth.organizationId
        ? `blob3/${auth.organizationId}/${parts[1]}/${partId}`
        : legacyKey;
      if (request.method === "PUT") {
        const body = await request.arrayBuffer();
        // Outputs are 4KiB-capped at the harness boundary; diffs can run
        // larger but a sidecar entry is one tool result, never a dump.
        if (body.byteLength > MAX_TOOL_BLOB_BYTES) return json({ error: "too_large" }, 413);
        await env.BLOBS.put(key, body, {
          httpMetadata: {
            contentType: request.headers.get("content-type") ?? "text/plain; charset=utf-8"
          }
        });
        return json({ ok: true, bytes: body.byteLength });
      }
      if (request.method === "GET" || request.method === "HEAD") {
        let object =
          request.method === "GET" ? await env.BLOBS.get(key) : await env.BLOBS.head(key);
        if (!object && key !== legacyKey) {
          object =
            request.method === "GET"
              ? await env.BLOBS.get(legacyKey)
              : await env.BLOBS.head(legacyKey);
        }
        if (!object) return json({ error: "not_found" }, 404);
        const headers = new Headers();
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        // Re-resolved tool parts overwrite their key, so short-lived caching only.
        headers.set("cache-control", "private, max-age=300");
        const body =
          request.method === "GET" && "body" in object ? (object as R2ObjectBody).body : null;
        return new Response(body, { headers });
      }
    }

    // ── retired attachment mirror (clients ≤0.1.62): acknowledge and discard
    //    so old outboxes drain once instead of retrying the PUT forever ──────
    if (parts[0] === "attachments" && parts[1] && request.method === "PUT") {
      return json({ ok: true });
    }

    return json({ error: "not_found" }, 404);
  }
} satisfies ExportedHandler<Env>;
