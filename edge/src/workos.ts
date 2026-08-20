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

/** WorkOS explicitly rejected a request with a retryable 429, a read-only
 * request received 5xx, or a read-only request could not reach WorkOS. No
 * authentication decision was made. */
export class WorkOsTransientFailure extends Error {
  constructor(
    message: string,
    readonly upstreamStatus?: number
  ) {
    super(message);
  }
}

/** A mutating/single-use request had no definitive rejection: transport loss,
 * an unreadable success response, or a completed 5xx. WorkOS may already have
 * committed it, so callers must fail closed instead of restoring old state. */
export class WorkOsOutcomeUnknown extends WorkOsTransientFailure {}

/** A syntactically valid admin request that WorkOS could not honor. This is
 * deliberately distinct from a bad login: silently retrying without the role
 * would report success while creating an ordinary member (and, for a new
 * organization, leave an organization with nobody able to administer it). */
export class WorkOsRoleAssignmentFailed extends WorkOsAuthFailed {
  constructor(
    message: string,
    /** Present when createOrganization already allocated an organization. */
    readonly organizationId?: string,
    /** Present when automatic orphan cleanup also failed. */
    readonly rollbackFailure?: string
  ) {
    super(message);
  }
}

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

/** Comet domain model. WorkOS wire fields are translated at this adapter boundary. */
export interface OrganizationMembership {
  readonly id: string;
  readonly organizationId: string;
  readonly name: string;
  /** The caller's role in this organization ("admin" | "member"). */
  readonly role: string;
}

/** One member of a Comet Organization. */
export interface OrganizationMember {
  readonly membershipId: string;
  readonly userId: string;
  readonly email: string;
  readonly name: string | null;
  readonly role: string;
}

/** Active membership data that is sufficient for authorization and mutation
 * guards. Keeping this separate from `OrganizationMember` avoids an N-user enrichment
 * fan-out for operations that only need membership ids and roles. */
export interface RawOrganizationMember {
  readonly membershipId: string;
  readonly userId: string;
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

interface WireList<T> {
  data: T[];
  list_metadata?: {
    before?: string | null;
    after?: string | null;
  } | null;
}

const thrownFailureMessage = (error: unknown): string =>
  error instanceof Error && error.message ? error.message : "unknown transport failure";

const failureMessage = async (res: Response): Promise<string> => {
  let message = "authentication failed";
  try {
    const body = (await res.json()) as { message?: string; error_description?: string; error?: string };
    message = body.message ?? body.error_description ?? body.error ?? message;
  } catch {
    /* non-JSON error body */
  }
  return message;
};

const failed = async (res: Response): Promise<never> => {
  const message = await failureMessage(res);
  if (res.status === 429 || res.status >= 500) {
    throw new WorkOsTransientFailure(message, res.status);
  }
  throw new WorkOsAuthFailed(message);
};

/** Classify a completed response to a write or single-use-token request.
 * WorkOS documents 429 as a rate-limit rejection, so the operation may be
 * retried after backoff. A 5xx response does not prove that the write was
 * rolled back: the service may have committed before its internal failure. */
const mutatingFailed = async (res: Response): Promise<never> => {
  const message = await failureMessage(res);
  if (res.status === 429) {
    throw new WorkOsTransientFailure(message, res.status);
  }
  if (res.status >= 500) {
    throw new WorkOsOutcomeUnknown(message, res.status);
  }
  throw new WorkOsAuthFailed(message);
};

const adminRoleFailed = async (res: Response): Promise<never> => {
  if (res.status === 429 || res.status >= 500) return mutatingFailed(res);
  const detail = await failureMessage(res);
  throw new WorkOsRoleAssignmentFailed(`could not assign admin role: ${detail}`);
};

const mutatingFetch = async (
  input: string,
  init: RequestInit,
  operation: string
): Promise<Response> => {
  try {
    return await fetch(input, init);
  } catch (error) {
    throw new WorkOsOutcomeUnknown(`${operation}: ${thrownFailureMessage(error)}`);
  }
};

const readFetch = async (input: string, init: RequestInit, operation: string): Promise<Response> => {
  try {
    return await fetch(input, init);
  } catch (error) {
    throw new WorkOsTransientFailure(`${operation}: ${thrownFailureMessage(error)}`);
  }
};

const responseJson = async <T>(
  res: Response,
  operation: string,
  outcomeUnknown: boolean
): Promise<T> => {
  try {
    return (await res.json()) as T;
  } catch (error) {
    const message = `${operation}: could not read WorkOS response: ${thrownFailureMessage(error)}`;
    if (outcomeUnknown) throw new WorkOsOutcomeUnknown(message);
    throw new WorkOsTransientFailure(message);
  }
};

const post = async (apiKey: string, path: string, body: unknown): Promise<Response> =>
  mutatingFetch(
    `${API}${path}`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${apiKey}`,
        "content-type": "application/json"
      },
      body: JSON.stringify(body)
    },
    `WorkOS POST ${path} outcome is unknown`
  );

/** `authenticateWithCode`: WorkOS code → tokens + user. */
export const exchange = async (env: Env, apiKey: string, code: string): Promise<ExchangeResult> => {
  const res = await mutatingFetch(
    `${API}/user_management/authenticate`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        client_id: env.WORKOS_CLIENT_ID,
        client_secret: apiKey,
        grant_type: "authorization_code",
        code
      })
    },
    "WorkOS code exchange outcome is unknown"
  );
  if (!res.ok) return mutatingFailed(res);
  const r = await responseJson<WireAuthResponse>(res, "WorkOS code exchange", true);
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
 * to that Comet Organization (WorkOS returns the `org_id` wire claim). */
export const refresh = async (
  env: Env,
  apiKey: string,
  refreshToken: string,
  organizationId?: string
): Promise<RefreshResult> => {
  const res = await mutatingFetch(
    `${API}/user_management/authenticate`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        client_id: env.WORKOS_CLIENT_ID,
        client_secret: apiKey,
        grant_type: "refresh_token",
        refresh_token: refreshToken,
        ...(organizationId ? { organization_id: organizationId } : {})
      })
    },
    "WorkOS refresh outcome is unknown"
  );
  if (!res.ok) return mutatingFailed(res);
  const r = await responseJson<WireAuthResponse>(res, "WorkOS refresh", true);
  return { accessToken: r.access_token, refreshToken: r.refresh_token };
};

const get = async (apiKey: string, path: string): Promise<Response> =>
  readFetch(
    `${API}${path}`,
    { headers: { authorization: `Bearer ${apiKey}` } },
    `WorkOS GET ${path} unavailable`
  );

/** Fetch every WorkOS cursor page. A repeated `after` token is an upstream
 * protocol failure; stopping at that point would silently return an incomplete
 * authorization list, while following it forever would strand the Worker. */
const listActiveMemberships = async (
  apiKey: string,
  filters: Readonly<Record<string, string>>,
  operation: string
): Promise<WireMembership[]> => {
  const memberships: WireMembership[] = [];
  const seenAfter = new Set<string>();
  let after: string | undefined;
  for (;;) {
    const params = new URLSearchParams({ ...filters, statuses: "active", limit: "100" });
    if (after) params.set("after", after);
    const res = await get(apiKey, `/user_management/organization_memberships?${params}`);
    if (!res.ok) return failed(res);
    const page = await responseJson<WireList<WireMembership>>(res, operation, false);
    memberships.push(...page.data);

    const nextAfter = page.list_metadata?.after ?? undefined;
    if (!nextAfter) return memberships;
    if (seenAfter.has(nextAfter)) {
      throw new WorkOsTransientFailure(`${operation}: WorkOS repeated pagination cursor`);
    }
    seenAfter.add(nextAfter);
    after = nextAfter;
  }
};

/** The user's complete set of active Comet Organization memberships. */
export const listOrganizations = async (
  apiKey: string,
  userId: string
): Promise<OrganizationMembership[]> => {
  const memberships = await listActiveMemberships(
    apiKey,
    { user_id: userId },
    "WorkOS organization list"
  );
  return memberships.map((m) => ({
    id: m.id,
    organizationId: m.organization_id,
    name: m.organization_name ?? m.organization_id,
    role: m.role?.slug ?? "member"
  }));
};

/** The caller's active membership in one Organization — the admin gate's evidence.
 * `undefined` = not a member. */
export const organizationMembershipOf = async (
  apiKey: string,
  organizationId: string,
  userId: string
): Promise<{ membershipId: string; role: string } | undefined> => {
  const params = new URLSearchParams({
    user_id: userId,
    organization_id: organizationId,
    statuses: "active",
    limit: "1"
  });
  const res = await get(apiKey, `/user_management/organization_memberships?${params}`);
  if (!res.ok) return failed(res);
  const r = await responseJson<{ data: WireMembership[] }>(res, "WorkOS membership lookup", false);
  const m = r.data[0];
  return m ? { membershipId: m.id, role: m.role?.slug ?? "member" } : undefined;
};

/** Every active membership in an Organization without user-profile enrichment. Use
 * this for cross-Organization and last-admin guards so a mutation is O(pages), not
 * O(pages + members), in WorkOS subrequests. */
export const listRawOrganizationMembers = async (
  apiKey: string,
  organizationId: string
): Promise<RawOrganizationMember[]> => {
  const memberships = await listActiveMemberships(
    apiKey,
    { organization_id: organizationId },
    "WorkOS member list"
  );
  return memberships.map((membership) => ({
    membershipId: membership.id,
    userId: membership.user_id ?? "",
    role: membership.role?.slug ?? "member"
  }));
};

const mapWithConcurrency = async <T, U>(
  values: readonly T[],
  concurrency: number,
  map: (value: T) => Promise<U>
): Promise<U[]> => {
  const results = new Array<U>(values.length);
  let nextIndex = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const index = nextIndex++;
      if (index >= values.length) return;
      results[index] = await map(values[index]);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, () => worker())
  );
  return results;
};

/** Every active member of an Organization, with user identity resolved for display.
 * WorkOS user lookups are deliberately bounded so a large Organization cannot emit a
 * burst of hundreds of simultaneous subrequests. */
export const listOrganizationMembers = async (
  apiKey: string,
  organizationId: string
): Promise<OrganizationMember[]> => {
  const memberships = await listRawOrganizationMembers(apiKey, organizationId);
  return mapWithConcurrency(
    memberships,
    8,
    async (membership): Promise<OrganizationMember> => {
    let email = membership.userId;
    let name: string | null = null;
    if (membership.userId) {
      const userRes = await get(apiKey, `/user_management/users/${membership.userId}`);
      if (userRes.ok) {
        const u = await responseJson<WireUser>(userRes, "WorkOS user lookup", false);
        email = u.email;
        name = [u.first_name, u.last_name].filter(Boolean).join(" ") || null;
      } else if (userRes.status !== 404) {
        // A deleted user may race the active-membership list; retain the
        // stable user id as a display fallback for that one case. Rate limits,
        // service failures, and authorization failures must reach the UI so it
        // can retry instead of presenting the id as if it were an email.
        return failed(userRes);
      }
    }
      return {
        membershipId: membership.membershipId,
        userId: membership.userId,
        email,
        name,
        role: membership.role
      };
    }
  );
};

/** Add a user to an Organization by email: an already-registered user gets an active
 * membership immediately; an unknown email gets a WorkOS invitation instead.
 * A missing custom "member" slug can safely use WorkOS's default role. An
 * admin request must never take that fallback: success guarantees the role
 * the caller requested. */
export const addOrganizationMemberByEmail = async (
  apiKey: string,
  organizationId: string,
  email: string,
  role: string
): Promise<{ added: boolean; invited: boolean }> => {
  const userRes = await get(
    apiKey,
    `/user_management/users?${new URLSearchParams({ email, limit: "1" })}`
  );
  if (!userRes.ok) return failed(userRes);
  const users = await responseJson<{ data: WireUser[] }>(userRes, "WorkOS user lookup", false);
  const user = users.data[0];
  if (user) {
    const withRole = await post(apiKey, "/user_management/organization_memberships", {
      user_id: user.id,
      organization_id: organizationId,
      role_slug: role
    });
    if (!withRole.ok) {
      if (role === "admin") return adminRoleFailed(withRole);
      // The role-less retry is only compatibility for an explicit client-side
      // rejection of a customized `member` slug. Never duplicate a write whose
      // 5xx outcome is unknown, or bypass a rate-limit response.
      if (withRole.status === 429 || withRole.status >= 500) {
        return mutatingFailed(withRole);
      }
      const fallback = await post(apiKey, "/user_management/organization_memberships", {
        user_id: user.id,
        organization_id: organizationId
      });
      if (!fallback.ok) return mutatingFailed(fallback);
    }
    return { added: true, invited: false };
  }
  const invite = await post(apiKey, "/user_management/invitations", {
    email,
    organization_id: organizationId,
    ...(role === "admin" ? { role_slug: "admin" } : {})
  });
  if (!invite.ok) {
    if (role === "admin") return adminRoleFailed(invite);
    return mutatingFailed(invite);
  }
  return { added: false, invited: true };
};

/** Change a member's role ("admin" | "member"). */
export const setOrganizationMemberRole = async (
  apiKey: string,
  membershipId: string,
  role: string
): Promise<void> => {
  const res = await mutatingFetch(
    `${API}/user_management/organization_memberships/${membershipId}`,
    {
      method: "PUT",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({ role_slug: role })
    },
    "WorkOS role update outcome is unknown"
  );
  if (!res.ok) return mutatingFailed(res);
};

/** Remove a member from an Organization. */
export const removeOrganizationMember = async (
  apiKey: string,
  membershipId: string
): Promise<void> => {
  const res = await mutatingFetch(
    `${API}/user_management/organization_memberships/${membershipId}`,
    {
      method: "DELETE",
      headers: { authorization: `Bearer ${apiKey}` }
    },
    "WorkOS membership removal outcome is unknown"
  );
  if (!res.ok) return mutatingFailed(res);
};

/** Delete an organization outright. */
export const deleteOrganization = async (
  apiKey: string,
  organizationId: string
): Promise<void> => {
  const res = await mutatingFetch(
    `${API}/organizations/${organizationId}`,
    {
      method: "DELETE",
      headers: { authorization: `Bearer ${apiKey}` }
    },
    "WorkOS organization deletion outcome is unknown"
  );
  if (!res.ok) return mutatingFailed(res);
};

/** Finish a failed createOrganization transaction. Always preserves the first-admin
 * failure as the primary error; orphan cleanup is attached as recovery context. */
const rollbackFailedOrganizationCreation = async (
  apiKey: string,
  organizationId: string,
  assignmentFailure: string
): Promise<never> => {
  let rollbackFailure: string | undefined;
  try {
    await deleteOrganization(apiKey, organizationId);
  } catch (error) {
    rollbackFailure = thrownFailureMessage(error);
  }

  const primary = `could not assign admin role: ${assignmentFailure}`;
  if (rollbackFailure === undefined) {
    // Deliberately log only the public organization id and outcome. In
    // particular, never include apiKey or request headers in Worker logs.
    console.warn("workos createOrganization admin assignment failed; organization rolled back", {
      organizationId
    });
    throw new WorkOsRoleAssignmentFailed(
      `${primary} (organizationId=${organizationId}; created organization rolled back)`,
      organizationId
    );
  }

  console.error("workos createOrganization rollback failed after admin assignment failure", {
    organizationId,
    rollbackFailure
  });
  throw new WorkOsRoleAssignmentFailed(
    `${primary} (organizationId=${organizationId}; rollback failed: ${rollbackFailure}; manual cleanup required before retry)`,
    organizationId,
    rollbackFailure
  );
};

/** Create an organization and make the user its first (admin) member. */
export const createOrganization = async (
  apiKey: string,
  userId: string,
  name: string
): Promise<{ organizationId: string }> => {
  const organizationResponse = await post(apiKey, "/organizations", { name });
  if (!organizationResponse.ok) return mutatingFailed(organizationResponse);
  const organization = await responseJson<{ id: string }>(
    organizationResponse,
    "WorkOS organization creation",
    true
  );
  // The creator must administer their Organization. Never retry without the role:
  // that would turn an admin-assignment failure into a false 200 while leaving
  // the creator unable to manage the organization.
  let assignmentFailure: string;
  try {
    const withRole = await post(apiKey, "/user_management/organization_memberships", {
      user_id: userId,
      organization_id: organization.id,
      role_slug: "admin"
    });
    if (withRole.ok) return { organizationId: organization.id };
    if (withRole.status >= 500) await mutatingFailed(withRole);
    assignmentFailure = await failureMessage(withRole);
  } catch (error) {
    // Transport loss and 5xx are ambiguous: WorkOS may have committed the
    // membership before failing. Verify live state before deleting a valid
    // admin Organization or reporting a false failure.
    assignmentFailure = `membership request outcome is uncertain: ${thrownFailureMessage(error)}`;
    let verification: string;
    try {
      const membership = await organizationMembershipOf(apiKey, organization.id, userId);
      if (membership?.role === "admin") {
        console.warn("workos createOrganization membership request failed; admin membership verified", {
          organizationId: organization.id
        });
        return { organizationId: organization.id };
      }
      verification = membership ? `verified role=${membership.role}` : "membership not found";
    } catch (verificationError) {
      verification = `verification failed: ${thrownFailureMessage(verificationError)}`;
    }
    console.warn(
      "workos createOrganization admin membership not verified; rolling back organization",
      {
        organizationId: organization.id,
        verification
      }
    );
  }

  // Organization creation and first-membership assignment are separate WorkOS
  // calls. Treat them transactionally from Comet's perspective: otherwise a
  // Retry allocates another organization while the first one remains orphaned.
  return rollbackFailedOrganizationCreation(apiKey, organization.id, assignmentFailure);
};
