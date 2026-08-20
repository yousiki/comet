import { afterEach, describe, expect, it, vi } from "vitest";
import { handleAuthRoute } from "./auth-routes";
import type { Env } from "./env";

interface WireMembership {
  readonly id: string;
  readonly organization_id: string;
  readonly user_id?: string;
  readonly role: { readonly slug: string };
}

interface WorkOsCall {
  readonly method: string;
  readonly url: URL;
  readonly body: Record<string, unknown> | undefined;
}

const jsonResponse = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" }
  });

const testEnv = (): Env =>
  ({
    AUTH_MODE: "dev",
    WORKOS_API_KEY: "secret",
    WORKOS_CLIENT_ID: "client"
  }) as Env;

const member = (
  id: string,
  organizationId: string,
  role: "admin" | "member"
): WireMembership => ({
  id,
  organization_id: organizationId,
  user_id: id === "m-caller" ? "caller" : `user-${id}`,
  role: { slug: role }
});

const stubWorkOs = (
  membersByOrganization: Readonly<Record<string, readonly WireMembership[]>>,
  options: { readonly rejectAdminAdd?: boolean } = {}
): WorkOsCall[] => {
  const calls: WorkOsCall[] = [];
  const fetchMock = vi.fn(async (input: string, init?: RequestInit): Promise<Response> => {
    const url = new URL(input);
    const method = init?.method ?? "GET";
    const body =
      typeof init?.body === "string"
        ? (JSON.parse(init.body) as Record<string, unknown>)
        : undefined;
    calls.push({ method, url, body });

    if (url.pathname === "/user_management/organization_memberships" && method === "GET") {
      const organizationId = url.searchParams.get("organization_id") ?? "";
      if (url.searchParams.has("user_id")) {
        return jsonResponse({
          data: [
            {
              id: "m-caller",
              organization_id: organizationId,
              user_id: "caller",
              role: { slug: "admin" }
            }
          ]
        });
      }
      const members = membersByOrganization[organizationId] ?? [];
      const after = url.searchParams.get("after");
      const offset = after?.startsWith("cursor-") ? Number(after.slice("cursor-".length)) : 0;
      const end = Math.min(offset + 100, members.length);
      return jsonResponse({
        data: members.slice(offset, end),
        list_metadata: { after: end < members.length ? `cursor-${end}` : null }
      });
    }

    if (url.pathname === "/user_management/users" && method === "GET") {
      return jsonResponse({
        data: [
          {
            id: "user-invitee",
            email: url.searchParams.get("email") ?? "invitee@example.com",
            first_name: null,
            last_name: null
          }
        ]
      });
    }

    if (url.pathname.startsWith("/user_management/users/") && method === "GET") {
      const userId = url.pathname.slice("/user_management/users/".length);
      return jsonResponse({
        id: userId,
        email: `${userId}@example.com`,
        first_name: "Example",
        last_name: "Member"
      });
    }

    if (url.pathname === "/organizations" && method === "POST") {
      return jsonResponse({ id: "org-created" }, 201);
    }

    if (url.pathname.startsWith("/organizations/") && method === "DELETE") {
      return jsonResponse({});
    }

    if (url.pathname === "/user_management/organization_memberships" && method === "POST") {
      if (options.rejectAdminAdd && body?.role_slug === "admin") {
        return jsonResponse({ message: "admin role is not configured" }, 400);
      }
      return jsonResponse({ id: "m-new" }, 201);
    }

    if (
      url.pathname.startsWith("/user_management/organization_memberships/") &&
      (method === "PUT" || method === "DELETE")
    ) {
      return jsonResponse({});
    }

    throw new Error(`unexpected WorkOS request: ${method} ${url.pathname}${url.search}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  return calls;
};

const callRoute = async (
  path: string,
  method: "GET" | "POST" | "DELETE",
  body?: Record<string, unknown>
): Promise<Response> => {
  const url = new URL(`https://edge.example${path}`);
  const response = await handleAuthRoute(
    new Request(url, {
      method,
      headers: {
        authorization: "Bearer caller",
        ...(body ? { "content-type": "application/json" } : {})
      },
      body: body ? JSON.stringify(body) : undefined
    }),
    testEnv(),
    url
  );
  if (!response) throw new Error("request was not handled as an auth route");
  return response;
};

const mutations = (calls: readonly WorkOsCall[]): WorkOsCall[] =>
  calls.filter((call) => call.method === "PUT" || call.method === "DELETE");

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("Organization auth contract", () => {
  it("returns the canonical organizations payload from /auth/organizations", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          data: [
            {
              id: "membership-a",
              organization_id: "org-a",
              organization_name: "Example Organization",
              user_id: "caller",
              role: { slug: "admin" }
            }
          ],
          list_metadata: { after: null }
        })
      )
    );

    const response = await callRoute("/auth/organizations", "GET");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      organizations: [
        {
          id: "membership-a",
          organizationId: "org-a",
          name: "Example Organization",
          role: "admin"
        }
      ]
    });
  });

  it("preserves the legacy orgs list payload at /auth/orgs", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          data: [
            {
              id: "membership-a",
              organization_id: "org-a",
              organization_name: "Example Organization",
              user_id: "caller",
              role: { slug: "admin" }
            }
          ],
          list_metadata: { after: null }
        })
      )
    );

    const response = await callRoute("/auth/orgs", "GET");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      orgs: [
        {
          id: "membership-a",
          organizationId: "org-a",
          name: "Example Organization",
          role: "admin"
        }
      ]
    });
  });

  it("keeps create, roster, member mutations, and delete available through /auth/orgs", async () => {
    const members = {
      "org-a": [member("m-caller", "org-a", "admin"), member("m-local", "org-a", "member")]
    };

    let calls = stubWorkOs(members);
    const created = await callRoute("/auth/orgs", "POST", { name: "Legacy client" });
    expect(created.status).toBe(200);
    expect(await created.json()).toEqual({ organizationId: "org-created" });
    expect(calls.some((call) => call.method === "POST" && call.url.pathname === "/organizations"))
      .toBe(true);

    calls = stubWorkOs(members);
    const roster = await callRoute("/auth/orgs/org-a/members", "GET");
    expect(roster.status).toBe(200);
    expect((await roster.json()) as { members: unknown[] }).toMatchObject({
      members: [{ membershipId: "m-caller" }, { membershipId: "m-local" }]
    });

    calls = stubWorkOs(members);
    const invited = await callRoute("/auth/orgs/org-a/members", "POST", {
      email: "invitee@example.com",
      role: "member"
    });
    expect(invited.status).toBe(200);
    expect(await invited.json()).toEqual({ added: true, invited: false });

    calls = stubWorkOs(members);
    const promoted = await callRoute("/auth/orgs/org-a/members/m-local", "POST", {
      role: "admin"
    });
    expect(promoted.status).toBe(200);
    expect(await promoted.json()).toEqual({ ok: true });

    calls = stubWorkOs(members);
    const removed = await callRoute("/auth/orgs/org-a/members/m-local", "DELETE");
    expect(removed.status).toBe(200);
    expect(await removed.json()).toEqual({ ok: true });

    calls = stubWorkOs(members);
    const deleted = await callRoute("/auth/orgs/org-a", "DELETE");
    expect(deleted.status).toBe(200);
    expect(await deleted.json()).toEqual({ ok: true });
    expect(
      calls.some(
        (call) => call.method === "DELETE" && call.url.pathname === "/organizations/org-a"
      )
    ).toBe(true);
  });
});

describe("Organization member auth routes", () => {
  it("cannot promote a membership that belongs to another Organization", async () => {
    const calls = stubWorkOs({
      "org-a": [member("m-caller", "org-a", "admin"), member("m-local", "org-a", "member")],
      "org-b": [member("m-other-org", "org-b", "member")]
    });

    const response = await callRoute(
      "/auth/organizations/org-a/members/m-other-org",
      "POST",
      { role: "admin" }
    );

    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "no such member" });
    expect(mutations(calls)).toHaveLength(0);
    expect(
      calls.some(
        (call) =>
          call.method === "GET" &&
          call.url.searchParams.get("organization_id") === "org-a" &&
          !call.url.searchParams.has("user_id")
      )
    ).toBe(true);
  });

  it("rejects a nonexistent promotion target before calling the id-only WorkOS endpoint", async () => {
    const calls = stubWorkOs({
      "org-a": [member("m-caller", "org-a", "admin"), member("m-local", "org-a", "member")]
    });

    const response = await callRoute("/auth/organizations/org-a/members/m-missing", "POST", {
      role: "admin"
    });

    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "no such member" });
    expect(mutations(calls)).toHaveLength(0);
  });

  for (const operation of ["demote", "remove"] as const) {
    it(`does not ${operation} the last admin`, async () => {
      const calls = stubWorkOs({ "org-a": [member("m-caller", "org-a", "admin")] });

      const response = await callRoute(
        "/auth/organizations/org-a/members/m-caller",
        operation === "demote" ? "POST" : "DELETE",
        operation === "demote" ? { role: "member" } : undefined
      );

      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({
        error: "cannot remove or demote the last admin"
      });
      expect(mutations(calls)).toHaveLength(0);
    });
  }

  it("promotes a member only after finding it in the URL Organization", async () => {
    const calls = stubWorkOs({
      "org-a": [member("m-caller", "org-a", "admin"), member("m-local", "org-a", "member")]
    });

    const response = await callRoute("/auth/organizations/org-a/members/m-local", "POST", {
      role: "admin"
    });

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
    expect(mutations(calls)).toHaveLength(1);
    expect(mutations(calls)[0]).toMatchObject({ method: "PUT", body: { role_slug: "admin" } });
  });

  it("finds a second admin on the next page without enriching users during a demotion", async () => {
    const fillers = Array.from({ length: 99 }, (_, index) =>
      member(`m-member-${index}`, "org-a", "member")
    );
    const calls = stubWorkOs({
      "org-a": [
        member("m-caller", "org-a", "admin"),
        ...fillers,
        member("m-second-admin", "org-a", "admin")
      ]
    });

    const response = await callRoute("/auth/organizations/org-a/members/m-caller", "POST", {
      role: "member"
    });

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
    expect(mutations(calls)).toHaveLength(1);
    const rawMembershipReads = calls.filter(
      (call) =>
        call.method === "GET" &&
        call.url.pathname === "/user_management/organization_memberships" &&
        !call.url.searchParams.has("user_id")
    );
    expect(rawMembershipReads).toHaveLength(2);
    expect(rawMembershipReads.map((call) => call.url.searchParams.get("after"))).toEqual([
      null,
      "cursor-100"
    ]);
    expect(calls.some((call) => call.url.pathname.startsWith("/user_management/users/"))).toBe(
      false
    );
  });

  it("surfaces an admin-add failure instead of succeeding with a default member", async () => {
    const calls = stubWorkOs(
      { "org-a": [member("m-caller", "org-a", "admin")] },
      { rejectAdminAdd: true }
    );

    const response = await callRoute("/auth/organizations/org-a/members", "POST", {
      email: "invitee@example.com",
      role: "admin"
    });

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      error: "could not assign admin role: admin role is not configured",
      outcomeUnknown: false
    });
    const membershipPosts = calls.filter(
      (call) =>
        call.method === "POST" &&
        call.url.pathname === "/user_management/organization_memberships"
    );
    expect(membershipPosts).toHaveLength(1);
    expect(membershipPosts[0]?.body).toMatchObject({ role_slug: "admin" });
  });

  it("rejects unknown invite roles instead of silently treating them as member", async () => {
    const calls = stubWorkOs({ "org-a": [member("m-caller", "org-a", "admin")] });

    const response = await callRoute("/auth/organizations/org-a/members", "POST", {
      email: "invitee@example.com",
      role: "owner"
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ error: "role must be admin or member" });
    expect(calls.some((call) => call.method === "POST")).toBe(false);
  });
});

describe("WorkOS failure protocol", () => {
  const callRefresh = async (): Promise<Response> => {
    const url = new URL("https://edge.example/auth/refresh");
    const response = await handleAuthRoute(
      new Request(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ refreshToken: "refresh-old" })
      }),
      testEnv(),
      url
    );
    if (!response) throw new Error("refresh route was not handled");
    return response;
  };

  it("maps refresh HTTP 429 to a definite retryable rejection", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ message: "workos rate limited" }, 429))
    );

    const response = await callRefresh();

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error: "workos rate limited",
      transient: true,
      outcomeUnknown: false,
      upstreamStatus: 429
    });
    expect(warn).toHaveBeenCalledWith("auth/refresh failed", "unknown-ip", {
      refreshTokenLength: "refresh-old".length,
      failureClass: "transient"
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain("refresh-old");
  });

  it("marks refresh HTTP 5xx as outcome unknown", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ message: "failed after token rotation" }, 503))
    );

    const response = await callRefresh();

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      error: "failed after token rotation",
      transient: true,
      outcomeUnknown: true
    });
    expect(warn).toHaveBeenCalledWith("auth/refresh failed", "unknown-ip", {
      refreshTokenLength: "refresh-old".length,
      failureClass: "outcomeUnknown"
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain("refresh-old");
  });

  it("marks refresh transport failure as outcome unknown", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("connection reset after request");
      })
    );
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const response = await callRefresh();

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      error: "WorkOS refresh outcome is unknown: connection reset after request",
      transient: true,
      outcomeUnknown: true
    });
    expect(warn).toHaveBeenCalledWith("auth/refresh failed", "unknown-ip", {
      refreshTokenLength: "refresh-old".length,
      failureClass: "outcomeUnknown"
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain("refresh-old");
  });

  it("classifies an explicit refresh rejection without logging credential bytes", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ message: "refresh token rejected" }, 400))
    );

    const response = await callRefresh();

    expect(response.status).toBe(401);
    expect(warn).toHaveBeenCalledWith("auth/refresh failed", "unknown-ip", {
      refreshTokenLength: "refresh-old".length,
      failureClass: "auth"
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain("refresh-old");
  });

  it("marks organization delete post-commit disconnect as outcome unknown", async () => {
    let request = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        request += 1;
        if (request === 1) {
          return jsonResponse({
            data: [
              {
                id: "m-caller",
                organization_id: "org-a",
                user_id: "caller",
                role: { slug: "admin" }
              }
            ]
          });
        }
        throw new Error("connection closed after WorkOS committed delete");
      })
    );

    const response = await callRoute("/auth/organizations/org-a", "DELETE");

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      error:
        "WorkOS organization deletion outcome is unknown: connection closed after WorkOS committed delete",
      transient: true,
      outcomeUnknown: true
    });
  });

  it("marks organization delete HTTP 5xx as outcome unknown", async () => {
    let request = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        request += 1;
        if (request === 1) {
          return jsonResponse({
            data: [
              {
                id: "m-caller",
                organization_id: "org-a",
                user_id: "caller",
                role: { slug: "admin" }
              }
            ]
          });
        }
        return jsonResponse({ message: "failed after delete" }, 503);
      })
    );

    const response = await callRoute("/auth/organizations/org-a", "DELETE");

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      error: "failed after delete",
      transient: true,
      outcomeUnknown: true
    });
  });

  it("maps organization delete HTTP 429 to a definite retryable rejection", async () => {
    let request = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        request += 1;
        if (request === 1) {
          return jsonResponse({
            data: [
              {
                id: "m-caller",
                organization_id: "org-a",
                user_id: "caller",
                role: { slug: "admin" }
              }
            ]
          });
        }
        return jsonResponse({ message: "rate limited" }, 429);
      })
    );

    const response = await callRoute("/auth/organizations/org-a", "DELETE");

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error: "rate limited",
      transient: true,
      outcomeUnknown: false,
      upstreamStatus: 429
    });
  });
});
