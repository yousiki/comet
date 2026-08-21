export interface Env {
  DEVICE_ROOMS: DurableObjectNamespace;
  /** Organization registries. `reg1/{organizationId}/{userId}` and `reg2/*`
   * are retained legacy Durable Object namespaces. */
  REGISTRY_ROOMS: DurableObjectNamespace;
  /** Chat rooms — dumb authenticated log relays (docs/chat2-sync.md), one DO
   * instance per room name: `chat2/{chatId}` (retired single-owner namespace)
   * and `chat3/{organizationId}/{chatId}` (Organization-shared). */
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
   * routes (code exchange, refresh, Organizations). Unset ⇒ those routes answer 501,
   * matching the old apps/server dev-mode behavior. */
  WORKOS_API_KEY?: string;
}

/** Header the Worker stamps on requests it forwards into DOs after verifying
 * the caller's JWT. DOs trust it blindly — they are only reachable through
 * the Worker (design §2: "DO never sees an unauthenticated frame"). */
export const AUTH_USER_HEADER = "x-zeron-auth-user";

/** Header the Worker stamps on requests forwarded into Organization-shared
 * `chat3` rooms. Its value is historical wire protocol and must remain
 * stable. */
export const ROOM_KIND_HEADER = "x-zeron-room-kind";
export const LEGACY_ORGANIZATION_CHAT_ROOM_KIND = "org-chat";
export type RoomKind = typeof LEGACY_ORGANIZATION_CHAT_ROOM_KIND;

/** Header the Worker stamps with the caller's verified Organization id.
 * The `x-zeron-auth-org` value is a legacy internal wire name and must remain
 * stable. Same trust rule as AUTH_USER_HEADER: Worker-controlled and scrubbed
 * from inbound requests before being set. */
export const AUTH_ORGANIZATION_HEADER = "x-zeron-auth-org";
