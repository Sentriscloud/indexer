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
  addresses as addressesTable,
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

  // Cap each pass to keep memory + transaction size bounded. Operator
  // can bump via env when running against an internal RPC endpoint
  // that doesn't enforce the public rate limit.
  const BATCH = BigInt(process.env.INDEXER_BATCH_SIZE ?? 50);
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

// Concurrency cap for the per-tx native fetch fan-out. Pure HTTP
// limit — avoids hammering the chain REST with thousands of concurrent
// connections on high-tx blocks (chain max is 5000 tx/block). 25 is
// chosen to keep latency low while staying well under the public edge's
// per-IP connection cap. Tunable via env for deployments that point at
// an internal RPC endpoint with no rate limit.
const TX_FETCH_CONCURRENCY = Number(
  process.env.INDEXER_TX_FETCH_CONCURRENCY ?? 25,
);

/** Fan-out N async tasks with a concurrency cap. Returns results in
 * input order. */
async function mapWithConcurrency<I, O>(
  items: readonly I[],
  limit: number,
  fn: (item: I, index: number) => Promise<O>,
): Promise<O[]> {
  const results: O[] = new Array(items.length);
  let cursor = 0;
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (true) {
        const i = cursor++;
        if (i >= items.length) return;
        results[i] = await fn(items[i]!, i);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

export async function indexBlock(args: IndexBlockArgs) {
  const { db, chain, height, log } = args;
  const block = await chain.getBlock(height);

  // ── PHASE 1: pre-fetch all per-tx native bodies in parallel. Out of
  // the SQL transaction so the long-held write lock only covers the
  // actual INSERTs, not the N HTTP round-trips. eth_getBlockByNumber on
  // Sentrix returns hash-only entries (chain doesn't honor
  // includeTransactions=true), and eth_getTransactionByHash returns the
  // native `{transaction: {…}}` wrapper instead of the EVM-spec shape
  // viem expects — that's why each tx needs its own native REST fetch.
  // Coinbase sentinel maps to the all-zero address + tx_type='coinbase'
  // so consumers can filter rewards out of address-history queries.
  // Sentri → wei conversion: 1 sentri = 1e10 wei (chain native is
  // 8-decimal, EVM rail is 18-decimal).
  const ZERO = "0x0000000000000000000000000000000000000000" as const;
  const SENTRI_TO_WEI = 10_000_000_000n;

  const txEntries = block.transactions.map((entry, i) => ({
    index: i,
    hash: typeof entry === "string" ? entry : entry.hash,
  }));

  const natives = await mapWithConcurrency(
    txEntries,
    TX_FETCH_CONCURRENCY,
    async (e) => ({ entry: e, native: await chain.getNativeTransaction(e.hash) }),
  );

  // ── PHASE 2: build batch INSERT row arrays.
  type TxRow = typeof txsTable.$inferInsert;
  type AddrRow = typeof addressesTable.$inferInsert;
  const txRows: TxRow[] = [];
  const addrRows: AddrRow[] = [];
  const heightBig = BigInt(height);
  for (const { entry, native } of natives) {
    if (!native) {
      // Either a true 404 (chain pruned / never had it) or persistent
      // transient failure after the client's 4 retries. Tx permanently
      // missing from this block's indexed set — log loud enough that
      // an operator can grep journalctl after the fact.
      log.warn(
        { hash: entry.hash, height: height.toString() },
        "getNativeTransaction returned null — tx skipped",
      );
      continue;
    }
    const inner = native.transaction;
    const isCoinbase = inner.from_address === "COINBASE";
    const fromAddr = isCoinbase ? ZERO : inner.from_address.toLowerCase();
    const toAddr = inner.to_address ? inner.to_address.toLowerCase() : null;
    const txHash = entry.hash.startsWith("0x")
      ? entry.hash.toLowerCase()
      : `0x${entry.hash.toLowerCase()}`;
    txRows.push({
      hash: txHash,
      blockHeight: height,
      txIndex: entry.index,
      fromAddr,
      toAddr,
      value: (BigInt(inner.amount) * SENTRI_TO_WEI).toString(),
      gasLimit: 0n,
      gasUsed: 0n,
      gasPrice: null,
      fee: (BigInt(inner.fee) * SENTRI_TO_WEI).toString(),
      nonce: BigInt(inner.nonce),
      data: inner.data,
      status: 1,
      contractAddress: null,
      txType: isCoinbase ? "coinbase" : "native",
    });
    // Coinbase sentinel skipped on the from side — the all-zero
    // address shouldn't claim a balance row from validator rewards.
    if (!isCoinbase) {
      addrRows.push({
        address: fromAddr,
        firstSeenBlock: heightBig,
        lastSeenBlock: heightBig,
      });
    }
    if (toAddr) {
      addrRows.push({
        address: toAddr,
        firstSeenBlock: heightBig,
        lastSeenBlock: heightBig,
      });
    }
  }

  // Dedupe addresses within the batch — the upsert below uses
  // ON CONFLICT (address) DO UPDATE, and Postgres rejects multiple
  // affected rows with the same conflict target inside one batch
  // (cardinality_violation). Keeping the first occurrence is safe;
  // first_seen_block is identical across rows for the same block and
  // last_seen_block uses GREATEST in the UPSERT clause.
  const addrDeduped = Array.from(
    new Map(addrRows.map((r) => [r.address, r])).values(),
  );

  // ── PHASE 3: log fetch + decode. eth_getLogs goes through viem
  // (covered by the batch transport from Tier 1 perf PR), so this is
  // already coalesced with other concurrent calls.
  const evmLogs = await chain.getLogsRange(height, height);

  type LogRow = typeof logsTable.$inferInsert;
  type TransferRow = typeof tokenTransfers.$inferInsert;
  const logRows: LogRow[] = [];
  const transferRows: TransferRow[] = [];
  for (const l of evmLogs) {
    if (
      l.blockNumber == null ||
      l.transactionHash == null ||
      l.logIndex == null
    ) {
      continue;
    }
    // Normalize address + topics to lowercase. Downstream consumers
    // query with lowercase WHERE; mixed-case rows silently miss JOINs.
    const logAddr = l.address.toLowerCase();
    const lower = (s: string | undefined) => (s ? s.toLowerCase() : null);
    const txHashLower = l.transactionHash.toLowerCase();
    logRows.push({
      blockHeight: l.blockNumber,
      txHash: txHashLower,
      logIndex: l.logIndex,
      address: logAddr,
      topic0: lower(l.topics[0]),
      topic1: lower(l.topics[1]),
      topic2: lower(l.topics[2]),
      topic3: lower(l.topics[3]),
      data: l.data,
    });
    const t0 = l.topics[0];
    if (t0 === ERC20_TRANSFER && l.topics.length === 3) {
      transferRows.push({
        blockHeight: l.blockNumber,
        txHash: txHashLower,
        logIndex: l.logIndex,
        contract: logAddr,
        standard: "erc20",
        fromAddr: topicToAddress(l.topics[1]!),
        toAddr: topicToAddress(l.topics[2]!),
        tokenId: null,
        amount: BigInt(l.data || "0x0").toString(),
      });
    } else if (t0 === ERC20_TRANSFER && l.topics.length === 4) {
      transferRows.push({
        blockHeight: l.blockNumber,
        txHash: txHashLower,
        logIndex: l.logIndex,
        contract: logAddr,
        standard: "erc721",
        fromAddr: topicToAddress(l.topics[1]!),
        toAddr: topicToAddress(l.topics[2]!),
        tokenId: BigInt(l.topics[3]!).toString(),
        amount: "1",
      });
    } else if (t0 === ERC1155_SINGLE) {
      const data = l.data.replace(/^0x/, "");
      const id = BigInt("0x" + data.slice(0, 64));
      const value = BigInt("0x" + data.slice(64, 128));
      transferRows.push({
        blockHeight: l.blockNumber,
        txHash: txHashLower,
        logIndex: l.logIndex,
        contract: logAddr,
        standard: "erc1155",
        fromAddr: topicToAddress(l.topics[2]!),
        toAddr: topicToAddress(l.topics[3]!),
        tokenId: id.toString(),
        amount: value.toString(),
      });
    } else if (t0 === ERC1155_BATCH) {
      // Two dynamic arrays — defer batch decode. Raw log still inserted
      // so the materialiser can re-decode at its own pace.
    }
  }

  // ── PHASE 4: single SQL transaction with all batch INSERTs. The
  // write-lock window now covers milliseconds (just the inserts)
  // instead of seconds (inserts + N HTTP round-trips). Pre-batch sync
  // could hold a transaction open for ~10 s on a 5000-tx block; this
  // version finishes in ~50 ms.
  await db.transaction(async (tx) => {
    await tx
      .insert(blocksTable)
      .values({
        height,
        hash: block.hash?.toLowerCase() ?? "0x",
        parentHash: block.parentHash.toLowerCase(),
        timestamp: block.timestamp,
        validator: (block.miner ?? ZERO).toLowerCase(),
        gasUsed: block.gasUsed ?? 0n,
        gasLimit: block.gasLimit ?? 0n,
        baseFee: block.baseFeePerGas?.toString() ?? null,
        txCount: block.transactions.length,
        stateRoot: block.stateRoot?.toLowerCase() ?? null,
      })
      .onConflictDoNothing();

    if (txRows.length > 0) {
      await tx.insert(txsTable).values(txRows).onConflictDoNothing();
    }
    if (addrDeduped.length > 0) {
      await tx
        .insert(addressesTable)
        .values(addrDeduped)
        .onConflictDoUpdate({
          target: addressesTable.address,
          set: {
            lastSeenBlock: sql`GREATEST(${addressesTable.lastSeenBlock}, EXCLUDED.last_seen_block)`,
          },
        });
    }
    if (logRows.length > 0) {
      await tx.insert(logsTable).values(logRows).onConflictDoNothing();
    }
    if (transferRows.length > 0) {
      await tx
        .insert(tokenTransfers)
        .values(transferRows)
        .onConflictDoNothing();
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
