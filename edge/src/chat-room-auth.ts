/**
 * Authorization for Organization-shared rooms in the legacy `chat3` namespace.
 *
 * Pure decision function (the pickLiveHost precedent: extract the branchy
 * decision so it's unit-testable without a DO). Organization membership is already
 * proven before this runs — the Worker builds the room name from the verified
 * JWT Organization claim, so anyone who reaches the DO is a member. What's decided
 * here is the HOST-USER discipline layered on top:
 *
 * - Member ops (join, push via ws or HTTP rows, rows/checkpoint/sidecar reads,
 *   stats) are open to every member.
 * - Host ops (checkpoint POST, tail/diff PUT, reset) belong to exactly one
 *   user: the first to claim with `?role=host` (on ws join, or on the
 *   bootstrap checkpoint POST that can precede it). This is what makes the
 *   checkpoint's row-pruning safe with N writers — only the engine that
 *   materializes the full doc may declare rows covered.
 * - The claim is sticky: a `role=host` join by a DIFFERENT user is denied
 *   rather than re-claiming. (Host takeover is a future operator recipe, not
 *   an accident.)
 */
export type ChatOp =
  | "join"
  | "rowsGet"
  | "rowsPost"
  | "checkpointGet"
  | "checkpointPost"
  | "sidecarGet"
  | "sidecarPut"
  | "stats"
  | "reset";

const HOST_OPS: ReadonlySet<ChatOp> = new Set(["checkpointPost", "sidecarPut", "reset"]);

export interface ChatAuthVerdict {
  allow: boolean;
  /** True ⇒ the caller becomes the room's host user (persist before serving). */
  claimHost?: boolean;
}

export const authorizeChatRoom = (
  op: ChatOp,
  userId: string,
  hostUser: string | null,
  roleHost: boolean
): ChatAuthVerdict => {
  const canClaim = roleHost && (op === "join" || op === "checkpointPost");
  if (hostUser === null && canClaim) return { allow: true, claimHost: true };
  if (HOST_OPS.has(op)) return { allow: hostUser === userId };
  if (op === "join" && roleHost) return { allow: hostUser === userId };
  return { allow: true };
};
