/**
 * Minimal WorkOS User Management REST client — the fetch-based port of the
 * old apps/server `WorkOsAuth` service (which used @workos-inc/node; the
 * Worker keeps it SDK-free). This is the one place that holds the WorkOS
 * **API key** (a Worker secret). Device backends build the public authorize
 * URL themselves and delegate the secret-bearing steps here, so the key never
 * lands on a device.
 *
 * Without WORKOS_API_KEY configured the routes answer 501; in dev mode
 * backends use their userId as the bearer and never call these.
 */
import type { Env } from "./env";

const API = "https://api.workos.com";

/** Thrown for rejected WorkOS calls; routes map it to 401 (same as the old
 * server's WorkOsAuthFailed). */
export class WorkOsAuthFailed extends Error {}

export interface ExchangeResult {
  readonly user: {
    readonly id: string;
    readonly email: string;
    readonly firstName: string | null;
    readonly lastName: string | null;
  };
  readonly accessToken: string;
  readonly refreshToken: string;
}

export interface RefreshResult {
  readonly accessToken: string;
  readonly refreshToken: string;
}

export interface OrgMembership {
  readonly id: string;
  readonly organizationId: string;
  readonly name: string;
  /** The caller's role in this org ("admin" | "member"). */
  readonly role: string;
}

/** One member of an organization (the team-management surface). */
export interface OrgMember {
  readonly membershipId: string;
  readonly userId: string;
  readonly email: string;
  readonly name: string | null;
  readonly role: string;
}

interface WireUser {
  id: string;
  email: string;
  first_name: string | null;
  last_name: string | null;
}

interface WireAuthResponse {
  user: WireUser;
  access_token: string;
  refresh_token: string;
}

interface WireMembership {
  id: string;
  organization_id: string;
  organization_name?: string | null;
  user_id?: string;
  role?: { slug?: string } | null;
}

const failed = async (res: Response): Promise<never> => {
  let message = "authentication failed";
  try {
    const body = (await res.json()) as { message?: string; error_description?: string; error?: string };
    message = body.message ?? body.error_description ?? body.error ?? message;
  } catch {
    /* non-JSON error body */
  }
  throw new WorkOsAuthFailed(message);
};

const post = async (apiKey: string, path: string, body: unknown): Promise<Response> =>
  fetch(`${API}${path}`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json"
    },
    body: JSON.stringify(body)
  });

/** `authenticateWithCode`: WorkOS code → tokens + user. */
export const exchange = async (env: Env, apiKey: string, code: string): Promise<ExchangeResult> => {
  const res = await fetch(`${API}/user_management/authenticate`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      client_id: env.WORKOS_CLIENT_ID,
      client_secret: apiKey,
      grant_type: "authorization_code",
      code
    })
  });
  if (!res.ok) return failed(res);
  const r = (await res.json()) as WireAuthResponse;
  return {
    user: {
      id: r.user.id,
      email: r.user.email,
      firstName: r.user.first_name,
      lastName: r.user.last_name
    },
    accessToken: r.access_token,
    refreshToken: r.refresh_token
  };
};

/** `authenticateWithRefreshToken`; passing `organizationId` scopes the session
 * to that org (the next access token carries `org_id`). */
export const refresh = async (
  env: Env,
  apiKey: string,
  refreshToken: string,
  organizationId?: string
): Promise<RefreshResult> => {
  const res = await fetch(`${API}/user_management/authenticate`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      client_id: env.WORKOS_CLIENT_ID,
      client_secret: apiKey,
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      ...(organizationId ? { organization_id: organizationId } : {})
    })
  });
  if (!res.ok) return failed(res);
  const r = (await res.json()) as WireAuthResponse;
  return { accessToken: r.access_token, refreshToken: r.refresh_token };
};

/** The user's active organization memberships. */
export const listOrgs = async (apiKey: string, userId: string): Promise<OrgMembership[]> => {
  const params = new URLSearchParams({ user_id: userId, statuses: "active", limit: "100" });
  const res = await fetch(`${API}/user_management/organization_memberships?${params}`, {
    headers: { authorization: `Bearer ${apiKey}` }
  });
  if (!res.ok) return failed(res);
  const r = (await res.json()) as { data: WireMembership[] };
  return r.data.map((m) => ({
    id: m.id,
    organizationId: m.organization_id,
    name: m.organization_name ?? m.organization_id,
    role: m.role?.slug ?? "member"
  }));
};

const get = async (apiKey: string, path: string): Promise<Response> =>
  fetch(`${API}${path}`, { headers: { authorization: `Bearer ${apiKey}` } });

/** The caller's active membership in one org — the admin gate's evidence.
 * `undefined` = not a member. */
export const membershipOf = async (
  apiKey: string,
  orgId: string,
  userId: string
): Promise<{ membershipId: string; role: string } | undefined> => {
  const params = new URLSearchParams({
    user_id: userId,
    organization_id: orgId,
    statuses: "active",
    limit: "1"
  });
  const res = await get(apiKey, `/user_management/organization_memberships?${params}`);
  if (!res.ok) return failed(res);
  const r = (await res.json()) as { data: WireMembership[] };
  const m = r.data[0];
  return m ? { membershipId: m.id, role: m.role?.slug ?? "member" } : undefined;
};

/** Every active member of an org, with user identity resolved (the members
 * list is small — team scale — so the per-user lookups are fine). */
export const listMembers = async (apiKey: string, orgId: string): Promise<OrgMember[]> => {
  const params = new URLSearchParams({ organization_id: orgId, statuses: "active", limit: "100" });
  const res = await get(apiKey, `/user_management/organization_memberships?${params}`);
  if (!res.ok) return failed(res);
  const r = (await res.json()) as { data: WireMembership[] };
  const members = await Promise.all(
    r.data.map(async (m): Promise<OrgMember> => {
      let email = m.user_id ?? "";
      let name: string | null = null;
      if (m.user_id) {
        const userRes = await get(apiKey, `/user_management/users/${m.user_id}`);
        if (userRes.ok) {
          const u = (await userRes.json()) as WireUser;
          email = u.email;
          name = [u.first_name, u.last_name].filter(Boolean).join(" ") || null;
        }
      }
      return {
        membershipId: m.id,
        userId: m.user_id ?? "",
        email,
        name,
        role: m.role?.slug ?? "member"
      };
    })
  );
  return members;
};

/** Add a user to an org by EMAIL: an already-registered user gets an active
 * membership immediately; an unknown email gets a WorkOS invitation instead.
 * Role slugs are per-environment config — fall back to the default role
 * rather than failing (same recovery as createOrg). */
export const addMemberByEmail = async (
  apiKey: string,
  orgId: string,
  email: string,
  role: string
): Promise<{ added: boolean; invited: boolean }> => {
  const userRes = await get(
    apiKey,
    `/user_management/users?${new URLSearchParams({ email, limit: "1" })}`
  );
  if (!userRes.ok) return failed(userRes);
  const users = (await userRes.json()) as { data: WireUser[] };
  const user = users.data[0];
  if (user) {
    const withRole = await post(apiKey, "/user_management/organization_memberships", {
      user_id: user.id,
      organization_id: orgId,
      role_slug: role
    });
    if (!withRole.ok) {
      const fallback = await post(apiKey, "/user_management/organization_memberships", {
        user_id: user.id,
        organization_id: orgId
      });
      if (!fallback.ok) return failed(fallback);
    }
    return { added: true, invited: false };
  }
  const invite = await post(apiKey, "/user_management/invitations", {
    email,
    organization_id: orgId,
    ...(role === "admin" ? { role_slug: "admin" } : {})
  });
  if (!invite.ok) return failed(invite);
  return { added: false, invited: true };
};

/** Change a member's role ("admin" | "member"). */
export const setMemberRole = async (
  apiKey: string,
  membershipId: string,
  role: string
): Promise<void> => {
  const res = await fetch(`${API}/user_management/organization_memberships/${membershipId}`, {
    method: "PUT",
    headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
    body: JSON.stringify({ role_slug: role })
  });
  if (!res.ok) return failed(res);
};

/** Remove a member from an org. */
export const removeMember = async (apiKey: string, membershipId: string): Promise<void> => {
  const res = await fetch(`${API}/user_management/organization_memberships/${membershipId}`, {
    method: "DELETE",
    headers: { authorization: `Bearer ${apiKey}` }
  });
  if (!res.ok) return failed(res);
};

/** Delete an organization outright. */
export const deleteOrg = async (apiKey: string, orgId: string): Promise<void> => {
  const res = await fetch(`${API}/organizations/${orgId}`, {
    method: "DELETE",
    headers: { authorization: `Bearer ${apiKey}` }
  });
  if (!res.ok) return failed(res);
};

/** Create an organization and make the user its first (admin) member. */
export const createOrg = async (
  apiKey: string,
  userId: string,
  name: string
): Promise<{ organizationId: string }> => {
  const orgRes = await post(apiKey, "/organizations", { name });
  if (!orgRes.ok) return failed(orgRes);
  const org = (await orgRes.json()) as { id: string };
  // The creator administers their workspace. Role slugs are per-environment
  // config, so fall back to the default role if "admin" doesn't exist rather
  // than failing the whole onboarding.
  const withRole = await post(apiKey, "/user_management/organization_memberships", {
    user_id: userId,
    organization_id: org.id,
    role_slug: "admin"
  });
  if (!withRole.ok) {
    const fallback = await post(apiKey, "/user_management/organization_memberships", {
      user_id: userId,
      organization_id: org.id
    });
    if (!fallback.ok) return failed(fallback);
  }
  return { organizationId: org.id };
};
