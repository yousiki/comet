import { describe, expect, it, vi } from "vitest";

vi.mock("./install.sh", () => ({ default: "#!/bin/sh" }));

import worker from "./index";
import { AUTH_ORGANIZATION_HEADER, AUTH_USER_HEADER, type Env } from "./env";

interface ForwardedRequest {
  room: string;
  request: Request;
}

const fakeNamespace = (forwarded: ForwardedRequest[]): DurableObjectNamespace =>
  ({
    idFromName(room: string): DurableObjectId {
      return { room } as unknown as DurableObjectId;
    },
    get(id: DurableObjectId): DurableObjectStub {
      const room = (id as unknown as { room: string }).room;
      return {
        async fetch(request: Request): Promise<Response> {
          forwarded.push({ room, request });
          return new Response(null, { status: 204 });
        }
      } as unknown as DurableObjectStub;
    }
  }) as unknown as DurableObjectNamespace;

const testEnv = (deviceRooms: DurableObjectNamespace): Env => ({
  AUTH_MODE: "dev",
  WORKOS_CLIENT_ID: "test",
  CHAT_ROOMS: {} as DurableObjectNamespace,
  DEVICE_ROOMS: deviceRooms,
  REGISTRY_ROOMS: {} as DurableObjectNamespace,
  BLOBS: {} as R2Bucket,
  RELEASES: {} as R2Bucket
});

const wsJoin = async (env: Env, query: string): Promise<void> => {
  const response = await worker.fetch(
    new Request(`https://edge.example/device/dev-1/ws?${query}`, {
      headers: { authorization: "Bearer alice@org-a", upgrade: "websocket" }
    }),
    env
  );
  expect(response.status).toBe(204);
};

// The bug this guards: the Worker rebuilt the DO-bound query from role+connId
// only, silently dropping the host's `shared` declaration — so member
// (client-role) admission, which reads the stored declaration, never opened.
describe("device ws forwarding", () => {
  it("forwards the host's sharing declaration to the DO", async () => {
    for (const shared of ["0", "1"] as const) {
      const forwarded: ForwardedRequest[] = [];
      await wsJoin(testEnv(fakeNamespace(forwarded)), `role=host&shared=${shared}`);
      const url = new URL(forwarded[0]!.request.url);
      expect(url.searchParams.get("shared")).toBe(shared);
      expect(url.searchParams.get("role")).toBe("host");
    }
  });

  it("drops an absent or malformed declaration instead of inventing one", async () => {
    for (const query of ["role=host", "role=host&shared=true", "role=host&shared="]) {
      const forwarded: ForwardedRequest[] = [];
      await wsJoin(testEnv(fakeNamespace(forwarded)), query);
      expect(new URL(forwarded[0]!.request.url).searchParams.get("shared")).toBeNull();
    }
  });

  it("never forwards a client-supplied declaration (hosts declare, clients don't)", async () => {
    const forwarded: ForwardedRequest[] = [];
    await wsJoin(testEnv(fakeNamespace(forwarded)), "role=client&connId=c-1&shared=1");
    const url = new URL(forwarded[0]!.request.url);
    expect(url.searchParams.get("shared")).toBeNull();
    expect(url.searchParams.get("role")).toBe("client");
    expect(forwarded[0]!.request.headers.get(AUTH_USER_HEADER)).toBe("alice");
    expect(forwarded[0]!.request.headers.get(AUTH_ORGANIZATION_HEADER)).toBe("org-a");
  });
});
