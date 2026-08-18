import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { decodeFrame, FRAME, type Frame } from "../../src/chat-frames";
import { AUTH_USER_HEADER, ROOM_KIND_HEADER } from "../../src/env";

const request = (
  path: string,
  userId: string,
  method: "GET" | "POST",
  options: { orgChat?: boolean; body?: Uint8Array } = {}
): Request => {
  const headers = new Headers({ [AUTH_USER_HEADER]: userId });
  if (options.orgChat) headers.set(ROOM_KIND_HEADER, "org-chat");
  return new Request(`https://room.test${path}`, {
    method,
    headers,
    body: options.body
  });
};

const decodeRowsBody = async (response: Response): Promise<Frame[]> => {
  const body = new Uint8Array(await response.arrayBuffer());
  const frames: Frame[] = [];
  let off = 0;
  while (off + 4 <= body.length) {
    const len = new DataView(body.buffer, body.byteOffset + off, 4).getUint32(0, true);
    off += 4;
    if (off + len > body.length) throw new Error("truncated rows body");
    const frame = decodeFrame(body.subarray(off, off + len));
    if (!frame) throw new Error("malformed rows frame");
    frames.push(frame);
    off += len;
  }
  if (off !== body.length) throw new Error("trailing rows bytes");
  return frames;
};

describe("ChatRoom HTTP rows authorization", () => {
  it("claims legacy chat2 rows on first HTTP contact, then keeps them owner-only", async () => {
    const stub = env.CHAT_ROOM.get(env.CHAT_ROOM.idFromName("legacy-rows-owner"));

    expect((await stub.fetch(request("/rows", "alice", "GET"))).status).toBe(200);
    expect(
      (
        await stub.fetch(
          request("/rows?device=alice-dev&batchId=after-pull-claim", "alice", "POST", {
            body: new Uint8Array([1])
          })
        )
      ).status
    ).toBe(200);

    expect((await stub.fetch(request("/rows", "bob", "GET"))).status).toBe(403);
    expect(
      (
        await stub.fetch(
          request("/rows?device=bob-dev&batchId=bob-1", "bob", "POST", {
            body: new Uint8Array([3])
          })
        )
      ).status
    ).toBe(403);

    const pushFirst = env.CHAT_ROOM.get(env.CHAT_ROOM.idFromName("legacy-rows-push-first"));
    expect(
      (
        await pushFirst.fetch(
          request("/rows?device=alice-dev&batchId=first-contact", "alice", "POST", {
            body: new Uint8Array([4])
          })
        )
      ).status
    ).toBe(200);
    expect((await pushFirst.fetch(request("/rows", "bob", "GET"))).status).toBe(403);
  });

  it("lets verified org members pull and push chat3 rows without a host claim", async () => {
    const stub = env.CHAT_ROOM.get(env.CHAT_ROOM.idFromName("org-rows-members"));

    for (const [userId, batchId, byte] of [
      ["alice", "alice-1", 1],
      ["bob", "bob-1", 2]
    ] as const) {
      const pushed = await stub.fetch(
        request(`/rows?device=${userId}-dev&batchId=${batchId}`, userId, "POST", {
          orgChat: true,
          body: new Uint8Array([byte])
        })
      );
      expect(pushed.status).toBe(200);
    }

    expect(
      (await stub.fetch(request("/rows?after=0", "carol", "GET", { orgChat: true }))).status
    ).toBe(200);
    const stats = await stub.fetch(request("/stats", "carol", "GET", { orgChat: true }));
    expect(stats.status).toBe(200);
    expect((await stats.json()) as { rowCount: number }).toMatchObject({ rowCount: 2 });
  });

  it("isolates HTTP push quota and outcomes by user even when device ids collide", async () => {
    const stub = env.CHAT_ROOM.get(env.CHAT_ROOM.idFromName("org-rows-attribution"));

    for (const [userId, device, batchId, byte] of [
      ["alice", "shared-device", "shared-a", 1],
      ["bob", "shared-device", "shared-b", 2],
      ["carol", "", "blank-c", 3],
      ["dave", "", "blank-d", 4]
    ] as const) {
      const deviceQuery = device === "" ? "" : `&device=${device}`;
      const response = await stub.fetch(
        request(`/rows?batchId=${batchId}${deviceQuery}`, userId, "POST", {
          orgChat: true,
          body: new Uint8Array([byte])
        })
      );
      expect(response.status).toBe(200);
    }

    const stats = await stub.fetch(request("/stats", "alice", "GET", { orgChat: true }));
    const { pushOutcomes } = (await stats.json()) as {
      pushOutcomes: Record<string, { ok: number; rejected: number }>;
    };
    expect(pushOutcomes).toMatchObject({
      "alice:shared-device": { ok: 1, rejected: 0 },
      "bob:shared-device": { ok: 1, rejected: 0 },
      "carol:(unknown)": { ok: 1, rejected: 0 },
      "dave:(unknown)": { ok: 1, rejected: 0 }
    });
    expect(pushOutcomes["shared-device"]).toBeUndefined();
    expect(pushOutcomes["(unknown)"]).toBeUndefined();
  });

  it("never excludes a chat3 member row just because its raw device id collides", async () => {
    const sharedDevice = "same-device";
    const orgStub = env.CHAT_ROOM.get(env.CHAT_ROOM.idFromName("org-rows-exclude-own"));
    expect(
      (
        await orgStub.fetch(
          request(`/rows?device=${sharedDevice}&batchId=bob-row`, "bob", "POST", {
            orgChat: true,
            body: new Uint8Array([7])
          })
        )
      ).status
    ).toBe(200);
    const orgFrames = await decodeRowsBody(
      await orgStub.fetch(
        request(`/rows?after=0&device=${sharedDevice}&excludeOwn=1`, "alice", "GET", {
          orgChat: true
        })
      )
    );
    expect(orgFrames.filter((frame) => frame.type === FRAME.row)).toHaveLength(1);

    // Legacy single-owner rooms retain the bandwidth optimization.
    const legacyStub = env.CHAT_ROOM.get(env.CHAT_ROOM.idFromName("legacy-rows-exclude-own"));
    expect(
      (
        await legacyStub.fetch(
          request("/checkpoint?seqCovered=0", "alice", "POST", {
            body: new Uint8Array([9])
          })
        )
      ).status
    ).toBe(200);
    expect(
      (
        await legacyStub.fetch(
          request(`/rows?device=${sharedDevice}&batchId=alice-row`, "alice", "POST", {
            body: new Uint8Array([8])
          })
        )
      ).status
    ).toBe(200);
    const legacyFrames = await decodeRowsBody(
      await legacyStub.fetch(
        request(`/rows?after=0&device=${sharedDevice}&excludeOwn=1`, "alice", "GET")
      )
    );
    expect(legacyFrames.filter((frame) => frame.type === FRAME.row)).toHaveLength(0);
  });
});
