import { describe, expect, it, vi } from "vitest";

vi.mock("./install.sh", () => ({ default: "#!/bin/sh" }));

import worker from "./index";
import {
  AUTH_ORGANIZATION_HEADER,
  AUTH_USER_HEADER,
  LEGACY_ORGANIZATION_CHAT_ROOM_KIND,
  ROOM_KIND_HEADER,
  type Env
} from "./env";

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

const testEnv = (chatRooms: DurableObjectNamespace): Env => ({
  AUTH_MODE: "dev",
  WORKOS_CLIENT_ID: "test",
  CHAT_ROOMS: chatRooms,
  SESSION_ROOMS: {} as DurableObjectNamespace,
  DEVICE_ROOMS: {} as DurableObjectNamespace,
  REGISTRY_ROOMS: {} as DurableObjectNamespace,
  BLOBS: {} as R2Bucket,
  RELEASES: {} as R2Bucket
});

describe("chat3 HTTP rows routes", () => {
  for (const method of ["GET", "POST"] as const) {
    it(`forwards ${method} to the Organization-scoped chat3 room`, async () => {
      const forwarded: ForwardedRequest[] = [];
      const env = testEnv(fakeNamespace(forwarded));
      const response = await worker.fetch(
        new Request("https://edge.example/chat3/chat-id/rows?after=7&batchId=batch-1", {
          method,
          headers: { authorization: "Bearer alice@org-a" }
        }),
        env
      );

      expect(response.status).toBe(204);
      expect(forwarded).toHaveLength(1);
      const call = forwarded[0]!;
      expect(call.room).toBe("chat3/org-a/chat-id");
      expect(call.room).not.toContain("chat2");
      expect(call.request.method).toBe(method);
      const url = new URL(call.request.url);
      expect(url.pathname).toBe("/rows");
      expect(url.search).toBe("?after=7&batchId=batch-1");
      expect(call.request.headers.get(AUTH_USER_HEADER)).toBe("alice");
      expect(call.request.headers.get(ROOM_KIND_HEADER)).toBe(
        LEGACY_ORGANIZATION_CHAT_ROOM_KIND
      );
      expect(call.request.headers.get(AUTH_ORGANIZATION_HEADER)).toBe("org-a");
    });
  }

  it("rejects chat3 rows without a verified Organization claim before forwarding", async () => {
    const forwarded: ForwardedRequest[] = [];
    const response = await worker.fetch(
      new Request("https://edge.example/chat3/chat-id/rows", {
        headers: { authorization: "Bearer alice" }
      }),
      testEnv(fakeNamespace(forwarded))
    );

    expect(response.status).toBe(403);
    expect(forwarded).toHaveLength(0);
  });
});
