import { describe, expect, test } from "bun:test";
import type { CloudflareWorkflowReceiptCleanupCandidate } from "@abshahin/workflows-sdk/cloudflare";
import { D1WorkflowReceiptStore } from "./workflow-receipts.js";

type FakeRow = {
  workflow_identity: string;
  workflow_name: string;
  instance_id: string;
  envelope_hash: string;
  state: "PENDING" | "ABSENCE_PROVEN" | "CREATED";
  owner: string | null;
  fence: number;
  lease_expires_at: number | null;
  created_at: number;
  updated_at: number;
  check_after: number;
};

class FakeD1 {
  readonly rows = new Map<string, FakeRow>();
  sessionConstraint: string | undefined;

  readonly database = {
    withSession: (constraint?: string) => {
      this.sessionConstraint = constraint;
      return { prepare: (query: string) => this.prepare(query) };
    },
  } as unknown as D1Database;

  private prepare(query: string) {
    let values: unknown[] = [];
    const bind = (...next: unknown[]) => {
      values = next;
      return statement;
    };
    const statement = {
      bind,
      run: async () => this.run(query, values),
      first: async <T>() => this.first(query, values) as T | null,
      all: async <T>() => ({
        success: true,
        meta: {},
        results: this.all(query, values) as T[],
      }),
    };
    return statement as unknown as D1PreparedStatement;
  }

  private async run(query: string, values: unknown[]) {
    if (query.includes("INSERT OR IGNORE")) {
      const [identity, name, id, hash, owner, lease, created, updated, checkAfter] =
        values as [
          string,
          string,
          string,
          string,
          string,
          number,
          number,
          number,
          number,
        ];
      const key = this.key(identity, id);
      if (this.rows.has(key)) return this.result(0);
      this.rows.set(key, {
        workflow_identity: identity,
        workflow_name: name,
        instance_id: id,
        envelope_hash: hash,
        state: "PENDING",
        owner,
        fence: 1,
        lease_expires_at: lease,
        created_at: created,
        updated_at: updated,
        check_after: checkAfter,
      });
      return this.result(1);
    }

    if (query.includes("SET state='ABSENCE_PROVEN'")) {
      const [updated, identity, id, hash, owner, fence] = values as [
        number,
        string,
        string,
        string,
        string,
        number,
      ];
      const row = this.rows.get(this.key(identity, id));
      if (!this.matchesTransition(row, "PENDING", hash, owner, fence)) {
        return this.result(0);
      }
      row!.state = "ABSENCE_PROVEN";
      row!.updated_at = updated;
      return this.result(1);
    }

    if (query.includes("SET state='CREATED'")) {
      const [updated, identity, id, hash, owner, fence] = values as [
        number,
        string,
        string,
        string,
        string,
        number,
      ];
      const row = this.rows.get(this.key(identity, id));
      if (!this.matchesTransition(row, "ABSENCE_PROVEN", hash, owner, fence)) {
        return this.result(0);
      }
      Object.assign(row!, {
        state: "CREATED",
        owner: null,
        lease_expires_at: null,
        updated_at: updated,
      });
      return this.result(1);
    }

    if (query.includes("SET lease_expires_at=?")) {
      const [lease, updated, identity, id, hash, owner, fence] = values as [
        number,
        number,
        string,
        string,
        string,
        string,
        number,
      ];
      const row = this.rows.get(this.key(identity, id));
      if (
        !row ||
        row.state === "CREATED" ||
        row.envelope_hash !== hash ||
        row.owner !== owner ||
        row.fence !== fence
      ) {
        return this.result(0);
      }
      row.lease_expires_at = lease;
      row.updated_at = updated;
      return this.result(1);
    }

    if (query.includes("DELETE FROM workflow_dispatch_receipts")) {
      const candidate = this.candidateFromSnapshot(values);
      const row = this.rows.get(
        this.key(candidate.workflowIdentity, candidate.instanceId),
      );
      if (!row || !this.matchesCandidate(row, candidate)) return this.result(0);
      this.rows.delete(this.key(candidate.workflowIdentity, candidate.instanceId));
      return this.result(1);
    }

    if (query.includes("SET check_after=?")) {
      const [checkAfter, updated, ...snapshot] = values;
      const candidate = this.candidateFromSnapshot(snapshot);
      const row = this.rows.get(
        this.key(candidate.workflowIdentity, candidate.instanceId),
      );
      if (!row || !this.matchesCandidate(row, candidate)) return this.result(0);
      row.check_after = checkAfter as number;
      row.updated_at = updated as number;
      return this.result(1);
    }

    throw new Error(`Unsupported fake D1 run: ${query}`);
  }

  private async first(query: string, values: unknown[]) {
    if (query.includes("SET owner=?, fence=fence + 1")) {
      const [owner, lease, now, identity, name, id, hash, leaseBefore] = values as [
        string,
        number,
        number,
        string,
        string,
        string,
        string,
        number,
      ];
      const row = this.rows.get(this.key(identity, id));
      if (
        !row ||
        row.workflow_name !== name ||
        row.envelope_hash !== hash ||
        row.state === "CREATED" ||
        (row.lease_expires_at ?? 0) > leaseBefore
      ) {
        return null;
      }
      row.owner = owner;
      row.fence += 1;
      row.lease_expires_at = lease;
      row.updated_at = now;
      return { ...row };
    }

    if (query.includes("SELECT workflow_name")) {
      const [identity, id] = values as [string, string];
      const row = this.rows.get(this.key(identity, id));
      return row ? { ...row } : null;
    }

    throw new Error(`Unsupported fake D1 first: ${query}`);
  }

  private all(query: string, values: unknown[]) {
    if (!query.includes("SELECT workflow_identity")) {
      throw new Error(`Unsupported fake D1 all: ${query}`);
    }
    const [checkBefore, leaseBefore, limit] = values as [number, number, number];
    return [...this.rows.values()]
      .filter(
        (row) =>
          row.check_after <= checkBefore &&
          (row.state === "CREATED" ||
            (row.lease_expires_at ?? 0) <= leaseBefore),
      )
      .sort(
        (left, right) =>
          left.check_after - right.check_after ||
          left.workflow_identity.localeCompare(right.workflow_identity) ||
          left.instance_id.localeCompare(right.instance_id),
      )
      .slice(0, limit)
      .map((row) => ({ ...row }));
  }

  private matchesTransition(
    row: FakeRow | undefined,
    state: FakeRow["state"],
    hash: string,
    owner: string,
    fence: number,
  ) {
    return (
      row?.state === state &&
      row.envelope_hash === hash &&
      row.owner === owner &&
      row.fence === fence
    );
  }

  private matchesCandidate(
    row: FakeRow,
    candidate: CloudflareWorkflowReceiptCleanupCandidate,
  ) {
    return (
      row.workflow_identity === candidate.workflowIdentity &&
      row.workflow_name === candidate.workflowName &&
      row.instance_id === candidate.instanceId &&
      row.envelope_hash === candidate.envelopeHash &&
      row.state === candidate.state &&
      row.owner === candidate.owner &&
      row.fence === candidate.fence &&
      row.lease_expires_at === candidate.leaseExpiresAt &&
      row.check_after === candidate.checkAfter
    );
  }

  private candidateFromSnapshot(
    values: unknown[],
  ): CloudflareWorkflowReceiptCleanupCandidate {
    const [identity, name, id, hash, state, owner, fence, lease, checkAfter] =
      values as [
        string,
        string,
        string,
        string,
        FakeRow["state"],
        string | null,
        number,
        number | null,
        number,
      ];
    return {
      workflowIdentity: identity,
      workflowName: name,
      instanceId: id,
      envelopeHash: hash,
      state,
      owner,
      fence,
      leaseExpiresAt: lease,
      checkAfter,
    };
  }

  private result(changes: number) {
    return { success: true, meta: { changes }, results: [] } as unknown as D1Result;
  }

  private key(identity: string, id: string) {
    return `${identity}\u0000${id}`;
  }
}

function claimInput(
  overrides: Partial<{
    workflowName: string;
    envelopeHash: string;
    owner: string;
    now: number;
    leaseExpiresAt: number;
    checkAfter: number;
  }> = {},
) {
  return {
    workflowIdentity: "manhali-workflow",
    workflowName: overrides.workflowName ?? "course/rebuild",
    instanceId: "wf_1",
    envelopeHash: overrides.envelopeHash ?? "a".repeat(64),
    owner: overrides.owner ?? "owner_1",
    now: overrides.now ?? 1_000,
    leaseExpiresAt: overrides.leaseExpiresAt ?? 2_000,
    checkAfter: overrides.checkAfter ?? 10_000,
  };
}

describe("D1WorkflowReceiptStore", () => {
  test("fences absence proof before creation and preserves it across stale claims", async () => {
    const fake = new FakeD1();
    const store = new D1WorkflowReceiptStore(fake.database);

    await expect(store.claim(claimInput())).resolves.toEqual({
      outcome: "claimed",
      fence: 1,
      previousState: "new",
    });
    await expect(
      store.markCreated({ ...claimInput(), fence: 1 }),
    ).resolves.toBe(false);
    await expect(
      store.markAbsenceProven({ ...claimInput(), fence: 1 }),
    ).resolves.toBe(true);
    await expect(
      store.claim(
        claimInput({ owner: "owner_2", now: 1_500, leaseExpiresAt: 2_500 }),
      ),
    ).resolves.toEqual({ outcome: "busy", retryAfter: 2_000 });
    await expect(
      store.claim(
        claimInput({ owner: "owner_3", now: 2_001, leaseExpiresAt: 3_001 }),
      ),
    ).resolves.toEqual({
      outcome: "claimed",
      fence: 2,
      previousState: "absence_proven",
    });

    await expect(
      store.markCreated({ ...claimInput(), owner: "owner_1", fence: 1 }),
    ).resolves.toBe(false);
    await expect(
      store.markCreated({
        ...claimInput(),
        owner: "owner_3",
        fence: 2,
        now: 2_100,
      }),
    ).resolves.toBe(true);
    await expect(
      store.claim(
        claimInput({ owner: "owner_4", now: 20_000, leaseExpiresAt: 21_000 }),
      ),
    ).resolves.toEqual({ outcome: "created" });
    await expect(
      store.claim(
        claimInput({
          envelopeHash: "b".repeat(64),
          owner: "owner_5",
          now: 20_000,
          leaseExpiresAt: 21_000,
        }),
      ),
    ).resolves.toEqual({ outcome: "conflict" });
    expect(fake.sessionConstraint).toBe("first-primary");
  });

  test("never reassigns an overdue ambiguous receipt by time alone", async () => {
    const fake = new FakeD1();
    const store = new D1WorkflowReceiptStore(fake.database);
    await store.claim(
      claimInput({ leaseExpiresAt: 1_100, checkAfter: 1_200 }),
    );

    await expect(
      store.claim(
        claimInput({
          envelopeHash: "b".repeat(64),
          owner: "conflicting_owner",
          now: 5_000,
          leaseExpiresAt: 6_000,
          checkAfter: 7_000,
        }),
      ),
    ).resolves.toEqual({ outcome: "conflict" });
    await expect(
      store.claim(
        claimInput({
          owner: "retry_owner",
          now: 5_000,
          leaseExpiresAt: 6_000,
          checkAfter: 7_000,
        }),
      ),
    ).resolves.toEqual({
      outcome: "claimed",
      fence: 2,
      previousState: "pending",
    });
    expect(fake.rows.get("manhali-workflow\u0000wf_1")?.check_after).toBe(1_200);
  });

  test("lists inactive candidates and conditionally deletes or defers exact snapshots", async () => {
    const fake = new FakeD1();
    const store = new D1WorkflowReceiptStore(fake.database);
    const seed = (
      id: string,
      state: FakeRow["state"],
      leaseExpiresAt: number | null,
      checkAfter = 900,
    ) => {
      fake.rows.set(`manhali-workflow\u0000${id}`, {
        workflow_identity: "manhali-workflow",
        workflow_name: "course/rebuild",
        instance_id: id,
        envelope_hash: id.padEnd(64, "x"),
        state,
        owner: state === "CREATED" ? null : `owner_${id}`,
        fence: 2,
        lease_expires_at: leaseExpiresAt,
        created_at: 1,
        updated_at: 1,
        check_after: checkAfter,
      });
    };
    seed("created", "CREATED", null);
    seed("pending", "PENDING", 800);
    seed("absence", "ABSENCE_PROVEN", 800);
    seed("active", "ABSENCE_PROVEN", 2_000);
    seed("future", "CREATED", null, 2_000);

    const candidates = await store.listCleanupCandidates({
      checkBefore: 1_000,
      limit: 10,
    });
    expect(candidates.map((candidate) => candidate.instanceId)).toEqual([
      "absence",
      "created",
      "pending",
    ]);

    const staleSnapshot = candidates[0]!;
    fake.rows.get("manhali-workflow\u0000absence")!.fence += 1;
    await expect(store.deleteCleanupCandidate(staleSnapshot)).resolves.toBe(false);
    await expect(
      store.deferCleanupCandidate({
        candidate: staleSnapshot,
        now: 1_000,
        checkAfter: 5_000,
      }),
    ).resolves.toBe(false);

    await expect(store.deleteCleanupCandidate(candidates[1]!)).resolves.toBe(true);
    await expect(
      store.deferCleanupCandidate({
        candidate: candidates[2]!,
        now: 1_000,
        checkAfter: 5_000,
      }),
    ).resolves.toBe(true);
    expect(fake.rows.has("manhali-workflow\u0000created")).toBe(false);
    expect(fake.rows.get("manhali-workflow\u0000pending")?.check_after).toBe(
      5_000,
    );
    expect(fake.rows.get("manhali-workflow\u0000active")?.check_after).toBe(900);
  });

  test("migration stores workflow identity, absence proof, and cleanup ordering", async () => {
    const migration = await Bun.file(
      new URL(
        "../../migrations/20260827_0001_workflow_dispatch_receipts.sql",
        import.meta.url,
      ),
    ).text();
    expect(migration).toContain("workflow_name TEXT NOT NULL");
    expect(migration).toContain(
      "CHECK (state IN ('PENDING', 'ABSENCE_PROVEN', 'CREATED'))",
    );
    expect(migration).toContain("check_after INTEGER NOT NULL");
    expect(migration).toContain("workflow_dispatch_receipts_cleanup_idx");
    expect(migration).not.toContain("  expires_at INTEGER");
  });
});
