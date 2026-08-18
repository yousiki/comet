import { describe, expect, it } from "vitest";
import { authorizeChatRoom, type ChatOp } from "./chat-room-auth";

describe("authorizeChatRoom", () => {
  it("claims the host slot on the first role=host join", () => {
    expect(authorizeChatRoom("join", "alice", null, true)).toEqual({
      allow: true,
      claimHost: true
    });
  });

  it("claims the host slot on a bootstrap checkpoint POST with role=host", () => {
    expect(authorizeChatRoom("checkpointPost", "alice", null, true)).toEqual({
      allow: true,
      claimHost: true
    });
  });

  it("lets members join, pull, push, and read without a host claim", () => {
    for (const op of [
      "join",
      "rowsGet",
      "rowsPost",
      "checkpointGet",
      "sidecarGet",
      "stats"
    ] as ChatOp[]) {
      expect(authorizeChatRoom(op, "bob", null, false).allow).toBe(true);
      expect(authorizeChatRoom(op, "bob", "alice", false).allow).toBe(true);
    }
  });

  it("denies host ops to non-hosts", () => {
    for (const op of ["checkpointPost", "sidecarPut", "reset"] as ChatOp[]) {
      expect(authorizeChatRoom(op, "bob", "alice", false).allow).toBe(false);
    }
  });

  it("denies host ops before any claim without role=host", () => {
    for (const op of ["checkpointPost", "sidecarPut", "reset"] as ChatOp[]) {
      expect(authorizeChatRoom(op, "bob", null, false).allow).toBe(false);
    }
  });

  it("allows the host user's host ops", () => {
    for (const op of ["checkpointPost", "sidecarPut", "reset"] as ChatOp[]) {
      expect(authorizeChatRoom(op, "alice", "alice", false)).toEqual({ allow: true });
    }
  });

  it("keeps the claim sticky: a second role=host join by another user is denied", () => {
    expect(authorizeChatRoom("join", "bob", "alice", true).allow).toBe(false);
  });

  it("lets the host rejoin with role=host without re-claiming", () => {
    expect(authorizeChatRoom("join", "alice", "alice", true)).toEqual({ allow: true });
  });

  it("never claims from non-claiming ops", () => {
    expect(authorizeChatRoom("stats", "bob", null, true).claimHost).toBeUndefined();
    expect(authorizeChatRoom("reset", "bob", null, true).allow).toBe(false);
  });
});
