export interface Env {
  SESSION_ROOMS: DurableObjectNamespace;
  DEVICE_ROOMS: DurableObjectNamespace;
  /** Per-user workspace registries (`reg1/{orgId}/{userId}`) — the row-table
   * replacement for the Loro workspace doc (docs/registry-sync.md). */
  REGISTRY_ROOMS: DurableObjectNamespace;
  /** chat2 session rooms (`chat2/{chatId}`) — dumb authenticated log relays
   * replacing SessionRoom's loro-aware s2 rooms (docs/chat2-sync.md). */
  CHAT_ROOMS: DurableObjectNamespace;
  BLOBS: R2Bucket;
  /** Release artifacts (headless tarballs, dmgs, latest.txt) served at
   * /releases/* for the curl-install flow. */
  RELEASES: R2Bucket;
  WORKOS_CLIENT_ID: string;
  /** "workos" (verify AuthKit JWTs) or "dev" (bearer == userId, never prod). */
  AUTH_MODE: string;
  /** Optional overrides for the WorkOS trust anchor. */
  WORKOS_ISSUER?: string;
  WORKOS_JWKS_URL?: string;
  /** WorkOS secret API key (wrangler secret) — powers the absorbed /auth/*
   * routes (code exchange, refresh, orgs). Unset ⇒ those routes answer 501,
   * matching the old apps/server dev-mode behavior. */
  WORKOS_API_KEY?: string;
}

/** Header the Worker stamps on requests it forwards into DOs after verifying
 * the caller's JWT. DOs trust it blindly — they are only reachable through
 * the Worker (design §2: "DO never sees an unauthenticated frame"). */
export const AUTH_USER_HEADER = "x-zeron-auth-user";

/** Header the Worker stamps on requests forwarded into workspace-doc rooms
 * (`ws/{orgId}`) and org-shared chat rooms (`chat3/{orgId}/{chatId}`).
 * Membership (JWT org claim == orgId) is enforced at the Worker; the DO sees
 * "workspace" (SessionRoom skips per-chat ownership) or "org-chat" (ChatRoom
 * uses host-user discipline instead of single-owner). */
export const ROOM_KIND_HEADER = "x-zeron-room-kind";

/** Header the Worker stamps with the caller's verified WorkOS org claim on
 * forwards that need an org-scoped decision inside the DO (device-room nudge
 * gate). Same trust rule as AUTH_USER_HEADER: Worker-controlled, deleted from
 * inbound requests before being set. */
export const AUTH_ORG_HEADER = "x-zeron-auth-org";
