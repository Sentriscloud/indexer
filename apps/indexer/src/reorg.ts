// Reorg detection + rewind. Pre-Tier-3 sync only checked the immediate
// parentHash of the new block — that catches a 1-block reorg but silently
// loses data on any reorg that lands a different block at a height we've
// already indexed. BFT chains rarely reorg deep but the indexer should be
// correct against the worst case (validator re-org during binary swap +
// chain.db rsync).
//
// Algorithm:
//   1. Periodically (every CHECK_INTERVAL_BLOCKS new tip events) re-fetch
//      the canonical hash for each height in [synced - DEPTH, synced]
//      from the chain RPC.
//   2. Compare against blocks.hash already in the DB.
//   3. First height where the hash differs is the reorg point. Everything
//      from that height onward gets deleted (FK cascade clears txs, logs,
//      token_transfers) and last_synced_height is rewound so the tail
//      loop re-indexes the canonical chain.
//
// Cost: one chain.getBlockNumber + DEPTH chain.getBlock + DEPTH local
// SELECT per check. With viem batch transport (Tier 1) the DEPTH
// getBlock calls collapse to a single HTTP round-trip.

import { eq, gte, sql } from "drizzle-orm";
import type { Logger } from "pino";

import {
  blocks as blocksTable,
  meta,
  type DbClient,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

export interface ReorgCheckArgs {
  db: DbClient;
  chain: SentrixClient;
  log: Logger;
  /** How many blocks back from synced tip to verify. Default 16. BFT
   * finalisation is single-slot so anything beyond ~3 blocks is paranoia,
   * but the cost is one batched RPC so we err on the safe side. */
  depth?: number;
}

export interface ReorgResult {
  /** True if a reorg was detected and rewound. */
  rewound: boolean;
  /** The height at which the local chain and canonical chain diverged.
   * Null if no reorg detected. */
  divergedAt: bigint | null;
  /** New last_synced_height after rewind. Equals synced if no rewind. */
  newSynced: bigint;
}

const DEFAULT_DEPTH = 16;

export async function checkAndRewindReorg(
  args: ReorgCheckArgs,
): Promise<ReorgResult> {
  const { db, chain, log } = args;
  const depth = args.depth ?? DEFAULT_DEPTH;

  // Read current synced height. If we haven't indexed anything yet,
  // there's nothing to verify.
  const syncedRows = await db
    .select({ value: meta.value })
    .from(meta)
    .where(eq(meta.key, "last_synced_height"))
    .limit(1);
  if (!syncedRows[0]) return { rewound: false, divergedAt: null, newSynced: 0n };
  const synced = BigInt(syncedRows[0].value);
  if (synced === 0n) return { rewound: false, divergedAt: null, newSynced: 0n };

  // Window to re-verify. Start at max(1, synced - depth + 1).
  const start = synced - BigInt(depth) + 1n > 0n ? synced - BigInt(depth) + 1n : 1n;

  // Fetch local hashes for the window.
  const localRows = await db
    .select({ height: blocksTable.height, hash: blocksTable.hash })
    .from(blocksTable)
    .where(gte(blocksTable.height, start))
    .orderBy(blocksTable.height);
  const localByHeight = new Map<string, string>(
    localRows.map((r) => [r.height.toString(), r.hash]),
  );

  // Fetch canonical hashes from the chain in parallel — viem batch
  // transport coalesces these into a single HTTP request.
  const heights: bigint[] = [];
  for (let h = start; h <= synced; h++) heights.push(h);
  const canonical = await Promise.all(
    heights.map(async (h) => {
      const block = await chain.getBlock(h);
      return { height: h, hash: block.hash?.toLowerCase() ?? null };
    }),
  );

  // Walk forward and find the first divergence.
  let divergedAt: bigint | null = null;
  for (const { height, hash } of canonical) {
    const local = localByHeight.get(height.toString());
    if (!local) {
      // We have a gap in the local DB inside the verification window —
      // shouldn't happen under normal operation, but if it does the
      // cleanest recovery is to rewind to before the gap and let the
      // tail loop re-index forward.
      divergedAt = height;
      break;
    }
    if (hash && local !== hash) {
      divergedAt = height;
      break;
    }
  }

  if (divergedAt === null) {
    return { rewound: false, divergedAt: null, newSynced: synced };
  }

  // Rewind: delete every block at or after divergedAt. The FK cascade
  // on transactions / logs (declared in schema.ts via
  // references onDelete: "cascade") removes child rows automatically.
  // token_transfers has no FK (intentional — see schema comment) so
  // we delete by block_height too.
  log.warn(
    {
      diverged_at: divergedAt.toString(),
      synced_before: synced.toString(),
      depth,
    },
    "reorg detected — rewinding indexer state",
  );

  await db.transaction(async (tx) => {
    await tx.execute(
      sql`DELETE FROM token_transfers WHERE block_height >= ${divergedAt}`,
    );
    await tx
      .delete(blocksTable)
      .where(gte(blocksTable.height, divergedAt!));
    // Reset cursor so the tail loop re-indexes from the canonical chain.
    const newSyncedVal = (divergedAt! - 1n).toString();
    await tx
      .insert(meta)
      .values({
        key: "last_synced_height",
        value: newSyncedVal,
        updatedAt: BigInt(Math.floor(Date.now() / 1000)),
      })
      .onConflictDoUpdate({
        target: meta.key,
        set: {
          value: sql`excluded.value`,
          updatedAt: sql`excluded.updated_at`,
        },
      });
    // Bump observability counter.
    const cnt = await tx
      .select({ value: meta.value })
      .from(meta)
      .where(eq(meta.key, "reorg_count"))
      .limit(1);
    const next = ((cnt[0]?.value ? Number(cnt[0].value) : 0) + 1).toString();
    await tx
      .insert(meta)
      .values({
        key: "reorg_count",
        value: next,
        updatedAt: BigInt(Math.floor(Date.now() / 1000)),
      })
      .onConflictDoUpdate({
        target: meta.key,
        set: {
          value: sql`excluded.value`,
          updatedAt: sql`excluded.updated_at`,
        },
      });
  });

  return { rewound: true, divergedAt, newSynced: divergedAt - 1n };
}
