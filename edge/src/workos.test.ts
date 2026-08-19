import { afterEach, describe, expect, it, vi } from "vitest";
import {
  WorkOsOutcomeUnknown,
  WorkOsRoleAssignmentFailed,
  WorkOsTransientFailure,
  addMemberByEmail,
  createOrg,
  deleteOrg,
  listMembers,
  listOrgs,
  refresh
} from "./workos";
import type { Env } from "./env";

const jsonResponse = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" }
  });

const stubResponses = (responses: readonly (Response | Error)[]) => {
  const pending = [...responses];
  const fetchMock = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit): Promise<Response> => {
      const outcome = pending.shift();
      if (!outcome) throw new Error("unexpected WorkOS request");
      if (outcome instanceof Error) throw outcome;
      return outcome;
    }
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
};

const requestBody = (call: unknown[] | undefined): Record<string, unknown> => {
  const init = call?.[1] as RequestInit | undefined;
  if (typeof init?.body !== "string") return {};
  return JSON.parse(init.body) as Record<string, unknown>;
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("WorkOS membership pagination", () => {
  const wireMembership = (index: number) => ({
    id: `membership-${index}`,
    organization_id: `org-${index}`,
    organization_name: `Team ${index}`,
    user_id: `user-${index}`,
    role: { slug: index % 10 === 0 ? "admin" : "member" }
  });

  it("returns organization memberships beyond the first 100-item page", async () => {
    const firstPage = Array.from({ length: 100 }, (_, index) => wireMembership(index));
    const fetchMock = stubResponses([
      jsonResponse({ data: firstPage, list_metadata: { after: "cursor-100" } }),
      jsonResponse({ data: [wireMembership(100)], list_metadata: { after: null } })
    ]);

    const orgs = await listOrgs("secret", "user-owner");

    expect(orgs).toHaveLength(101);
    expect(orgs[100]).toMatchObject({
      id: "membership-100",
      organizationId: "org-100",
      name: "Team 100",
      role: "admin"
    });
    expect(new URL(String(fetchMock.mock.calls[0]?.[0])).searchParams.get("after")).toBeNull();
    expect(new URL(String(fetchMock.mock.calls[1]?.[0])).searchParams.get("after")).toBe(
      "cursor-100"
    );
  });

  it("fails closed instead of looping on a repeated pagination cursor", async () => {
    const fetchMock = stubResponses([
      jsonResponse({ data: [], list_metadata: { after: "same-cursor" } }),
      jsonResponse({ data: [], list_metadata: { after: "same-cursor" } })
    ]);

    const error = await listOrgs("secret", "user-owner").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsTransientFailure);
    expect(error).toMatchObject({
      message: "WorkOS organization list: WorkOS repeated pagination cursor"
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("paginates the member roster and bounds user enrichment to eight requests", async () => {
    const memberships = Array.from({ length: 101 }, (_, index) => wireMembership(index));
    let activeUserLookups = 0;
    let maxActiveUserLookups = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL): Promise<Response> => {
      const url = new URL(String(input));
      if (url.pathname === "/user_management/organization_memberships") {
        const offset = url.searchParams.get("after") === "cursor-100" ? 100 : 0;
        const end = Math.min(offset + 100, memberships.length);
        return jsonResponse({
          data: memberships.slice(offset, end),
          list_metadata: { after: end < memberships.length ? "cursor-100" : null }
        });
      }
      if (url.pathname.startsWith("/user_management/users/")) {
        const userId = url.pathname.slice("/user_management/users/".length);
        activeUserLookups += 1;
        maxActiveUserLookups = Math.max(maxActiveUserLookups, activeUserLookups);
        await new Promise((resolve) => setTimeout(resolve, 0));
        activeUserLookups -= 1;
        return jsonResponse({
          id: userId,
          email: `${userId}@example.com`,
          first_name: "Member",
          last_name: userId
        });
      }
      throw new Error(`unexpected WorkOS request: ${url.pathname}${url.search}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const members = await listMembers("secret", "org-a");

    expect(members).toHaveLength(101);
    expect(members[100]).toMatchObject({
      membershipId: "membership-100",
      userId: "user-100",
      email: "user-100@example.com",
      name: "Member user-100",
      role: "admin"
    });
    expect(maxActiveUserLookups).toBe(8);
    const membershipCalls = fetchMock.mock.calls.filter(
      ([input]) => new URL(String(input)).pathname === "/user_management/organization_memberships"
    );
    expect(membershipCalls).toHaveLength(2);
  });

  it("surfaces a rate-limited user enrichment as a transient roster failure", async () => {
    stubResponses([
      jsonResponse({ data: [wireMembership(0)], list_metadata: { after: null } }),
      jsonResponse({ message: "user lookup rate limited" }, 429)
    ]);

    const error = await listMembers("secret", "org-a").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsTransientFailure);
    expect(error).toMatchObject({ message: "user lookup rate limited", upstreamStatus: 429 });
  });

  it("keeps the stable user id when a membership races a deleted user", async () => {
    stubResponses([
      jsonResponse({ data: [wireMembership(0)], list_metadata: { after: null } }),
      jsonResponse({ message: "no such user" }, 404)
    ]);

    const members = await listMembers("secret", "org-a");

    expect(members).toEqual([
      {
        membershipId: "membership-0",
        userId: "user-0",
        email: "user-0",
        name: null,
        role: "admin"
      }
    ]);
  });
});

describe("WorkOS organization role assignment", () => {
  it("rolls back a newly-created organization when its admin assignment fails", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-new" }, 201),
      jsonResponse({ message: "unknown role slug" }, 400),
      jsonResponse({ deleted: true })
    ]);

    const error = await createOrg("secret", "user-1", "New team").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message:
        "could not assign admin role: unknown role slug (organizationId=org-new; created organization rolled back)",
      organizationId: "org-new"
    });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(requestBody(fetchMock.mock.calls[1])).toMatchObject({ role_slug: "admin" });
    expect(fetchMock.mock.calls[2]?.[0]).toBe("https://api.workos.com/organizations/org-new");
    expect(fetchMock.mock.calls[2]?.[1]).toMatchObject({ method: "DELETE" });
    expect(warn).toHaveBeenCalledWith(
      "workos createOrg admin assignment failed; organization rolled back",
      { organizationId: "org-new" }
    );
    expect(JSON.stringify(warn.mock.calls)).not.toContain("secret");
  });

  it("preserves the admin error and recovery id when organization rollback also fails", async () => {
    const logError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-orphan" }, 201),
      jsonResponse({ message: "unknown admin role" }, 400),
      jsonResponse({ message: "organization deletion unavailable" }, 503)
    ]);

    const error = await createOrg("secret", "user-1", "New team").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message:
        "could not assign admin role: unknown admin role (organizationId=org-orphan; rollback failed: organization deletion unavailable; manual cleanup required before retry)",
      organizationId: "org-orphan",
      rollbackFailure: "organization deletion unavailable"
    });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls[2]?.[0]).toBe(
      "https://api.workos.com/organizations/org-orphan"
    );
    expect(fetchMock.mock.calls[2]?.[1]).toMatchObject({ method: "DELETE" });
    expect(logError).toHaveBeenCalledWith(
      "workos createOrg rollback failed after admin assignment failure",
      {
        organizationId: "org-orphan",
        rollbackFailure: "organization deletion unavailable"
      }
    );
    expect(JSON.stringify(logError.mock.calls)).not.toContain("secret");
  });

  it("accepts an ambiguous membership transport failure when live verification finds admin", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-verified" }, 201),
      new Error("connection reset after write"),
      jsonResponse({
        data: [
          {
            id: "membership-verified",
            organization_id: "org-verified",
            user_id: "user-1",
            role: { slug: "admin" }
          }
        ]
      })
    ]);

    await expect(createOrg("secret", "user-1", "New team")).resolves.toEqual({
      organizationId: "org-verified"
    });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    const verificationUrl = new URL(String(fetchMock.mock.calls[2]?.[0]));
    expect(verificationUrl.pathname).toBe("/user_management/organization_memberships");
    expect(verificationUrl.searchParams.get("user_id")).toBe("user-1");
    expect(verificationUrl.searchParams.get("organization_id")).toBe("org-verified");
    expect(warn).toHaveBeenCalledWith(
      "workos createOrg membership request failed; admin membership verified",
      { organizationId: "org-verified" }
    );
    expect(JSON.stringify(warn.mock.calls)).not.toContain("secret");
  });

  it("accepts a membership HTTP 5xx when live verification finds the committed admin", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-verified-5xx" }, 201),
      jsonResponse({ message: "membership failed after commit" }, 503),
      jsonResponse({
        data: [
          {
            id: "membership-verified",
            organization_id: "org-verified-5xx",
            user_id: "user-1",
            role: { slug: "admin" }
          }
        ]
      })
    ]);

    await expect(createOrg("secret", "user-1", "New team")).resolves.toEqual({
      organizationId: "org-verified-5xx"
    });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(warn).toHaveBeenCalledWith(
      "workos createOrg membership request failed; admin membership verified",
      { organizationId: "org-verified-5xx" }
    );
  });

  it("rolls back after a membership transport failure when verification finds no membership", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-transport" }, 201),
      new Error("connection reset after write"),
      jsonResponse({ data: [] }),
      jsonResponse({ deleted: true })
    ]);

    const error = await createOrg("secret", "user-1", "New team").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message: expect.stringContaining(
        "membership request outcome is uncertain: WorkOS POST /user_management/organization_memberships outcome is unknown: connection reset after write"
      ),
      organizationId: "org-transport"
    });
    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(fetchMock.mock.calls[3]?.[0]).toBe(
      "https://api.workos.com/organizations/org-transport"
    );
    expect(fetchMock.mock.calls[3]?.[1]).toMatchObject({ method: "DELETE" });
    expect(warn).toHaveBeenCalledWith(
      "workos createOrg admin membership not verified; rolling back organization",
      { organizationId: "org-transport", verification: "membership not found" }
    );
    expect(JSON.stringify(warn.mock.calls)).not.toContain("secret");
  });

  it("rolls back after a membership HTTP 5xx when verification finds no membership", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-5xx-empty" }, 201),
      jsonResponse({ message: "membership failed after write" }, 503),
      jsonResponse({ data: [] }),
      jsonResponse({ deleted: true })
    ]);

    const error = await createOrg("secret", "user-1", "New team").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message: expect.stringContaining(
        "membership request outcome is uncertain: membership failed after write"
      ),
      organizationId: "org-5xx-empty"
    });
    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(fetchMock.mock.calls[3]?.[0]).toBe(
      "https://api.workos.com/organizations/org-5xx-empty"
    );
    expect(warn).toHaveBeenCalledWith(
      "workos createOrg admin membership not verified; rolling back organization",
      { organizationId: "org-5xx-empty", verification: "membership not found" }
    );
  });

  it("still rolls back when membership verification also has a transport failure", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const fetchMock = stubResponses([
      jsonResponse({ id: "org-unverified" }, 201),
      new Error("membership write timed out"),
      new Error("membership lookup unavailable"),
      jsonResponse({ deleted: true })
    ]);

    const error = await createOrg("secret", "user-1", "New team").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message: expect.stringContaining(
        "membership request outcome is uncertain: WorkOS POST /user_management/organization_memberships outcome is unknown: membership write timed out"
      ),
      organizationId: "org-unverified"
    });
    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(fetchMock.mock.calls[3]?.[0]).toBe(
      "https://api.workos.com/organizations/org-unverified"
    );
    expect(warn).toHaveBeenCalledWith(
      "workos createOrg admin membership not verified; rolling back organization",
      {
        organizationId: "org-unverified",
        verification: expect.stringContaining(
          "verification failed: WorkOS GET /user_management/organization_memberships"
        )
      }
    );
    expect(JSON.stringify(warn.mock.calls)).not.toContain("secret");
  });

  it("fails an existing-user admin add without retrying as the default role", async () => {
    const fetchMock = stubResponses([
      jsonResponse({
        data: [
          {
            id: "user-2",
            email: "member@example.com",
            first_name: null,
            last_name: null
          }
        ]
      }),
      jsonResponse({ error: "admin role unavailable" }, 400)
    ]);

    const error = await addMemberByEmail(
      "secret",
      "org-1",
      "member@example.com",
      "admin"
    ).then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message: "could not assign admin role: admin role unavailable"
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(requestBody(fetchMock.mock.calls[1])).toMatchObject({ role_slug: "admin" });
  });

  it("identifies a rejected admin invitation as an admin-role failure", async () => {
    const fetchMock = stubResponses([
      jsonResponse({ data: [] }),
      jsonResponse({ message: "admin invitation role unavailable" }, 400)
    ]);

    const error = await addMemberByEmail(
      "secret",
      "org-1",
      "new-member@example.com",
      "admin"
    ).then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsRoleAssignmentFailed);
    expect(error).toMatchObject({
      message: "could not assign admin role: admin invitation role unavailable"
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(requestBody(fetchMock.mock.calls[1])).toMatchObject({ role_slug: "admin" });
  });

  it("keeps the default-role compatibility retry only for ordinary members", async () => {
    const fetchMock = stubResponses([
      jsonResponse({
        data: [
          {
            id: "user-2",
            email: "member@example.com",
            first_name: null,
            last_name: null
          }
        ]
      }),
      jsonResponse({ error: "member slug is customized" }, 400),
      jsonResponse({ id: "membership-2" }, 201)
    ]);

    await expect(
      addMemberByEmail("secret", "org-1", "member@example.com", "member")
    ).resolves.toEqual({ added: true, invited: false });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(requestBody(fetchMock.mock.calls[1])).toMatchObject({ role_slug: "member" });
    expect(requestBody(fetchMock.mock.calls[2])).not.toHaveProperty("role_slug");
  });

  it("does not duplicate an ordinary-member write after a WorkOS 5xx", async () => {
    const fetchMock = stubResponses([
      jsonResponse({
        data: [
          {
            id: "user-2",
            email: "member@example.com",
            first_name: null,
            last_name: null
          }
        ]
      }),
      jsonResponse({ error: "membership service failed after write" }, 503)
    ]);

    const error = await addMemberByEmail(
      "secret",
      "org-1",
      "member@example.com",
      "member"
    ).then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({
      message: "membership service failed after write",
      upstreamStatus: 503
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});

describe("WorkOS failure classification", () => {
  const env = { WORKOS_CLIENT_ID: "client" } as Env;

  it("classifies refresh HTTP 429 as a definite transient rejection", async () => {
    stubResponses([jsonResponse({ message: "please retry" }, 429)]);

    const error = await refresh(env, "secret", "refresh-old").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsTransientFailure);
    expect(error).not.toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({ message: "please retry", upstreamStatus: 429 });
  });

  it("classifies refresh HTTP 5xx as outcome unknown", async () => {
    stubResponses([jsonResponse({ message: "failed after token rotation" }, 503)]);

    const error = await refresh(env, "secret", "refresh-old").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({
      message: "failed after token rotation",
      upstreamStatus: 503
    });
  });

  it("classifies refresh transport loss as outcome unknown", async () => {
    stubResponses([new Error("connection reset after request")]);

    const error = await refresh(env, "secret", "refresh-old").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({
      message: "WorkOS refresh outcome is unknown: connection reset after request"
    });
  });

  it("classifies organization delete post-commit disconnect as outcome unknown", async () => {
    stubResponses([new Error("connection closed after commit")]);

    const error = await deleteOrg("secret", "org-deleted").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({
      message: "WorkOS organization deletion outcome is unknown: connection closed after commit"
    });
  });

  it("classifies organization delete HTTP 5xx as outcome unknown", async () => {
    stubResponses([jsonResponse({ message: "failed after delete" }, 503)]);

    const error = await deleteOrg("secret", "org-deleted").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({ message: "failed after delete", upstreamStatus: 503 });
  });

  it("classifies organization delete HTTP 429 as a definite transient rejection", async () => {
    stubResponses([jsonResponse({ message: "rate limited" }, 429)]);

    const error = await deleteOrg("secret", "org-deleted").then(
      () => undefined,
      (reason: unknown) => reason
    );

    expect(error).toBeInstanceOf(WorkOsTransientFailure);
    expect(error).not.toBeInstanceOf(WorkOsOutcomeUnknown);
    expect(error).toMatchObject({ message: "rate limited", upstreamStatus: 429 });
  });
});
