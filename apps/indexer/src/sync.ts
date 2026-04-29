// Block-level sync. Both backfill and tail funnel through `indexBlock`.
//
// Phase 1 scope:
//   - Persist block + transactions + logs.
//   - Decode ERC-20/721/1155 Transfer logs into token_transfers.
//   - Update _meta.last_synced_height in the same SQL transaction.
//
// Out of Phase 1 scope (will land later):
//   - Reorg detection beyond a single-block parentHash check.
//   - Address balance materialisation.
//   - Validator / epoch denormalisation (those come from the native REST
//     endpoints, not from EVM logs, so they live in a separate worker).

import { eq, sql } from "drizzle-orm";
import type { Logger } from "pino";

import {
  type DbClient,
  blocks as blocksTable,
  logs as logsTable,
  meta,
  tokenTransfers,
  transactions as txsTable,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

const ERC20_TRANSFER =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const ERC1155_SINGLE =
  "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
const ERC1155_BATCH =
  "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

interface SyncOnceArgs {
  db: DbClient;
  chain: SentrixClient;
  target: bigint;
  log: Logger;
}

/** Run one backfill batch from `last_synced_height + 1` up to `target`. */
export async function syncOnce(args: SyncOnceArgs): Promise<bigint> {
  const { db, chain, target, log } = args;
  const lastSynced = await readLastSynced(db);
  const start = lastSynced + 1n;
  if (start > target) return lastSynced;

  // Cap each pass to keep memory + transaction size bounded.
  const BATCH = 50n;
  const end = start + BATCH > target ? target : start + BATCH - 1n;

  log.info({ from: start.toString(), to: end.toString() }, "backfill batch");

  for (let h = start; h <= end; h++) {
    await indexBlock({ db, chain, height: h, log });
  }
  return end;
}

interface IndexBlockArgs {
  db: DbClient;
  chain: SentrixClient;
  height: bigint;
  log: Logger;
}

export async function indexBlock(args: IndexBlockArgs) {
  const { db, chain, height, log } = args;
  const block = await chain.getBlock(height);

  // We do all writes inside a single transaction so a partial block never
  // ends up in the DB on crash.
  await db.transaction(async (tx) => {
    await tx
      .insert(blocksTable)
      .values({
        height,
        hash: block.hash ?? "0x",
        parentHash: block.parentHash,
        timestamp: block.timestamp,
        validator: block.miner ?? "0x0000000000000000000000000000000000000000",
        gasUsed: block.gasUsed ?? 0n,
        gasLimit: block.gasLimit ?? 0n,
        baseFee: block.baseFeePerGas?.toString() ?? null,
        txCount: block.transactions.length,
        stateRoot: block.stateRoot ?? null,
      })
      .onConflictDoNothing();

    for (const t of block.transactions) {
      if (typeof t === "string") continue; // we asked for full txs, but be defensive
      await tx
        .insert(txsTable)
        .values({
          hash: t.hash,
          blockHeight: height,
          txIndex: t.transactionIndex ?? 0,
          fromAddr: t.from,
          toAddr: t.to ?? null,
          value: t.value.toString(),
          gasLimit: t.gas ?? 0n,
          gasPrice: t.gasPrice?.toString() ?? null,
          fee: 0n.toString(), // filled in later when we read receipts
          nonce: BigInt(t.nonce),
          data: t.input,
          status: 1,
          contractAddress: null,
          txType: t.to == null ? "evm" : "evm",
        })
        .onConflictDoNothing();
    }

    // Pull all logs in this block in one shot.
    const evmLogs = await chain.getLogsRange(height, height);
    for (const l of evmLogs) {
      if (
        l.blockNumber == null ||
        l.transactionHash == null ||
        l.logIndex == null
      ) {
        continue;
      }
      await tx
        .insert(logsTable)
        .values({
          blockHeight: l.blockNumber,
          txHash: l.transactionHash,
          logIndex: l.logIndex,
          address: l.address,
          topic0: l.topics[0] ?? null,
          topic1: l.topics[1] ?? null,
          topic2: l.topics[2] ?? null,
          topic3: l.topics[3] ?? null,
          data: l.data,
        })
        .onConflictDoNothing();

      // Decoded transfer materialisation.
      const t0 = l.topics[0];
      if (t0 === ERC20_TRANSFER && l.topics.length === 3) {
        // ERC-20 Transfer(from indexed, to indexed, uint256 value)
        await tx
          .insert(tokenTransfers)
          .values({
            blockHeight: l.blockNumber,
            txHash: l.transactionHash,
            logIndex: l.logIndex,
            contract: l.address,
            standard: "erc20",
            fromAddr: topicToAddress(l.topics[1]!),
            toAddr: topicToAddress(l.topics[2]!),
            tokenId: null,
            amount: BigInt(l.data || "0x0").toString(),
          })
          .onConflictDoNothing();
      } else if (t0 === ERC20_TRANSFER && l.topics.length === 4) {
        // ERC-721 Transfer(from indexed, to indexed, tokenId indexed)
        await tx
          .insert(tokenTransfers)
          .values({
            blockHeight: l.blockNumber,
            txHash: l.transactionHash,
            logIndex: l.logIndex,
            contract: l.address,
            standard: "erc721",
            fromAddr: topicToAddress(l.topics[1]!),
            toAddr: topicToAddress(l.topics[2]!),
            tokenId: BigInt(l.topics[3]!).toString(),
            amount: "1",
          })
          .onConflictDoNothing();
      } else if (t0 === ERC1155_SINGLE) {
        // ERC-1155 TransferSingle(operator indexed, from indexed, to indexed, id, value)
        const data = l.data.replace(/^0x/, "");
        const id = BigInt("0x" + data.slice(0, 64));
        const value = BigInt("0x" + data.slice(64, 128));
        await tx
          .insert(tokenTransfers)
          .values({
            blockHeight: l.blockNumber,
            txHash: l.transactionHash,
            logIndex: l.logIndex,
            contract: l.address,
            standard: "erc1155",
            fromAddr: topicToAddress(l.topics[2]!),
            toAddr: topicToAddress(l.topics[3]!),
            tokenId: id.toString(),
            amount: value.toString(),
          })
          .onConflictDoNothing();
      } else if (t0 === ERC1155_BATCH) {
        // Decoding a batch transfer from raw log data is non-trivial (two
        // dynamic arrays). Defer to Phase 2 — record the raw log here, the
        // worker that materialises balances can re-decode at its own pace.
      }
    }

    await tx
      .insert(meta)
      .values({
        key: "last_synced_height",
        value: height.toString(),
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

  log.debug({ h: height.toString() }, "indexed");
}

async function readLastSynced(db: DbClient): Promise<bigint> {
  const rows = await db
    .select({ value: meta.value })
    .from(meta)
    .where(eq(meta.key, "last_synced_height"))
    .limit(1);
  if (!rows[0]) return 0n;
  return BigInt(rows[0].value);
}

function topicToAddress(topic: string): string {
  // Topics are 32-byte right-padded; addresses are the right-most 20 bytes.
  return "0x" + topic.slice(-40).toLowerCase();
}
