import { describe, expect, it } from "vitest";
import { clientJoinAllowed } from "./device-room";

// The bug this guards: the client-role admission gate was owner-only while the
// Organization-shared registry made the owner's sessions visible (and their
// device Live) to every member — members' right-pane surfaces (terminal, files,
// diff) dialed the device room, ate an HTTP 403, and the client misread it as
// "unreachable (backing off)". Members of the owner's Organization must be
// admitted, but ONLY while the host's latest join declared the device shared —
// an unshared device must never be reachable by members (ARCHITECTURE.md §2).
describe("device-room client admission", () => {
  const member = {
    owner: "user-a",
    userId: "user-b",
    ownerOrganizationId: "org-1",
    callerOrganizationId: "org-1",
    shared: "1"
  };

  it("admits the owner regardless of sharing", () => {
    expect(
      clientJoinAllowed({ ...member, userId: "user-a", shared: "0" })
    ).toBe(true);
    expect(
      clientJoinAllowed({ ...member, userId: "user-a", shared: undefined })
    ).toBe(true);
  });

  it("admits an Organization member while the device is shared", () => {
    expect(clientJoinAllowed(member)).toBe(true);
  });

  it("refuses an Organization member when the device is unshared", () => {
    expect(clientJoinAllowed({ ...member, shared: "0" })).toBe(false);
  });

  it("refuses when the host never declared sharing (older daemon)", () => {
    expect(clientJoinAllowed({ ...member, shared: undefined })).toBe(false);
  });

  it("refuses malformed declarations — only the literal \"1\" admits", () => {
    for (const shared of ["", "true", "01", "yes"]) {
      expect(clientJoinAllowed({ ...member, shared })).toBe(false);
    }
  });

  it("refuses a caller from another Organization", () => {
    expect(
      clientJoinAllowed({ ...member, callerOrganizationId: "org-2" })
    ).toBe(false);
  });

  it("refuses a caller with no Organization", () => {
    expect(clientJoinAllowed({ ...member, callerOrganizationId: null })).toBe(
      false
    );
  });

  it("refuses a member when the owner has no recorded Organization", () => {
    // Both undefined must not compare equal-and-admit.
    expect(
      clientJoinAllowed({
        ...member,
        ownerOrganizationId: undefined,
        callerOrganizationId: null
      })
    ).toBe(false);
  });

  it("refuses everyone while the room is unclaimed", () => {
    expect(clientJoinAllowed({ ...member, owner: undefined })).toBe(false);
  });
});
