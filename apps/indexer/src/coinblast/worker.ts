// CoinBlast worker — separate cursor, runs in parallel with the chain-wide
// block-by-block sync. Why separate:
//
//   The chain-wide indexer is at h≈7,621 (genesis-up backfill at ~10
//   blocks/min) while the CoinBlast factory only deployed at h=1,178,667.
//   Waiting for the chain-wide cursor to reach the factory would cost ~80
//   days at current throughput. CoinBlast events are sparse (one factory +
//   N small curves), so a topic-filtered scan over the same RPC catches up
//   in minutes regardless of the chain-wide backfill state.
//
// Cursor: `_meta` key `last_synced_coinblast_height`. Backfill starts at
// COINBLAST_DEPLOY_BLOCK[network] - 1 and walks forward in chunks. Tail
// piggy-backs on the chain-wide newHeads subscription.
//
// Idempotency: cb_trades has a unique (tx_hash, log_index) index, so
// re-running any chunk is safe. cb_tokens uses curve_address as PK with
// onConflictDoNothing on inserts. Aggregate updates (volume, count, last
// price) live in the same SQL transaction as the trade insert and are
// gated by the SELECT-FOR-UPDATE lock pattern, so a re-run of the same
// log produces no double-count (the row already exists, transaction skips
// the insert and skips the update — see runChunk).

import { eq, sql } from "drizzle-orm";
import type { Logger } from "pino";
import { decodeEventLog, type Log } from "viem";

import {
  type DbClient,
  cbTokens,
  cbTrades,
  meta,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

import {
  COINBLAST_DEPLOY_BLOCK,
  COINBLAST_FACTORY_ADDRESS,
  TOPIC_BUY,
  TOPIC_GRADUATED,
  TOPIC_SELL,
  curveEvents,
  factoryEvents,
} from "./events.js";

// CurveCreated dispatch goes through the typed `event:` filter on the
// factory getLogs call (pass 1 in runChunk), not via topic0 comparison —
// that's why we don't import TOPIC_CURVE_CREATED here. The constant
// stays exported from events.ts for any future caller that wants it.

const META_KEY = "last_synced_coinblast_height";

// SAFE_LAG mirrors the chain-wide indexer — stay this many blocks short of
// tip so we never partial-read a block whose justification is still arriving.
const SAFE_LAG = BigInt(process.env.INDEXER_COINBLAST_SAFE_LAG ?? 5);

// Per-chunk block span. CoinBlast events are sparse so a wide chunk is fine,
// but Sentrix RPC caps getLogs at 5000 blocks per call. Stay below that.
const CHUNK = BigInt(process.env.INDEXER_COINBLAST_CHUNK ?? 4000);

// Throttle for the tight backfill loop. The retry-on-429 wrapper in the
// chain client absorbs rate-limit pressure too, but pacing the loop avoids
// burning CPU when there's nothing to do.
const TICK_MS = Number(process.env.INDEXER_COINBLAST_TICK_MS ?? 500);

interface WorkerArgs {
  db: DbClient;
  chain: SentrixClient;
  network: "mainnet" | "testnet";
  log: Logger;
}

export async function runCoinblastWorker(args: WorkerArgs) {
  const { db, chain, network, log } = args;
  const factory = COINBLAST_FACTORY_ADDRESS[network].toLowerCase() as `0x${string}`;

  // Hydrate known curves from cb_tokens. After backfill catches up this is
  // the source of truth for which addresses to filter on. New curves get
  // appended to this set as we process CurveCreated events.
  const knownCurves = new Set<string>();
  const existing = await db.select({ a: cbTokens.curveAddress }).from(cbTokens);
  for (const row of existing) knownCurves.add(row.a.toLowerCase());
  log.info({ count: knownCurves.size }, "coinblast: hydrated known curves");

  let cursor = await readCursor(db, network);
  log.info({ cursor: cursor.toString() }, "coinblast: starting backfill");

  // Backfill loop — walks forward until cursor >= tip - SAFE_LAG, then
  // continues in tail mode. Same code path; the upper bound is just
  // recomputed each iteration.
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const tip = await chain.getBlockNumber();
    const target = tip - SAFE_LAG;
    if (cursor >= target) {
      // Caught up. Sleep a bit then re-check.
      await sleep(TICK_MS * 4);
      continue;
    }

    const from = cursor + 1n;
    const to = from + CHUNK - 1n > target ? target : from + CHUNK - 1n;

    try {
      await runChunk({ db, chain, log, factory, knownCurves, from, to });
      cursor = to;
      await writeCursor(db, cursor);
      log.info(
        { from: from.toString(), to: to.toString(), curves: knownCurves.size },
        "coinblast: chunk done",
      );
    } catch (err) {
      log.error(
        { err: String(err), from: from.toString(), to: to.toString() },
        "coinblast: chunk failed; retrying after backoff",
      );
      await sleep(2000);
    }

    // Tiny pacing breath. Keeps us off the rate-limiter when the chunk
    // was effectively empty (common: most 4000-block chunks have no
    // CoinBlast activity).
    await sleep(TICK_MS);
  }
}

interface ChunkArgs {
  db: DbClient;
  chain: SentrixClient;
  log: Logger;
  factory: `0x${string}`;
  knownCurves: Set<string>;
  from: bigint;
  to: bigint;
}

async function runChunk(args: ChunkArgs) {
  const { db, chain, log, factory, knownCurves, from, to } = args;

  // Pass 1: factory CurveCreated logs in [from, to]. Scoping by address
  // here avoids any topic0 collision with unrelated contracts that happen
  // to define a same-shape Buy/Sell/Graduated event.
  const factoryLogs = await chain.http.getLogs({
    address: factory,
    fromBlock: from,
    toBlock: to,
    event: factoryEvents[0],
  });

  // Process CurveCreated first so the curve set is complete before we fetch
  // Buy/Sell/Graduated. If any of these logs are for already-known curves
  // (re-running the chunk), the insert hits the PK conflict and is skipped.
  for (const l of factoryLogs) {
    await applyCurveCreated(db, l, log);
    if (l.address) knownCurves.add(l.address.toLowerCase());
  }

  // Pass 2: curve-side Buy/Sell/Graduated. Filter to known curves only.
  // viem accepts an address array. Empty set short-circuits — no curves yet,
  // no point in calling getLogs.
  if (knownCurves.size === 0) return;

  const curveAddrs = Array.from(knownCurves) as `0x${string}`[];

  // viem caps the address array length silently in some RPCs; chunk in
  // batches of 100 to stay safe.
  const BATCH = 100;
  for (let i = 0; i < curveAddrs.length; i += BATCH) {
    const batch = curveAddrs.slice(i, i + BATCH);
    const curveLogs = await chain.http.getLogs({
      address: batch,
      fromBlock: from,
      toBlock: to,
      // Don't pass `event` — we want all three event types in one call.
      // We dispatch by topic0 below.
    });

    for (const l of curveLogs) {
      const t0 = l.topics[0];
      if (t0 === TOPIC_BUY) await applyTrade(db, l, "buy");
      else if (t0 === TOPIC_SELL) await applyTrade(db, l, "sell");
      else if (t0 === TOPIC_GRADUATED) await applyGraduated(db, l);
      // CurveCreated also has the curve as `topic1` indexed, but the LOG
      // is emitted from the factory address — we already handled it in
      // pass 1. Anything else is a topic we don't care about (no-op).
    }
  }
}

// ── Event handlers ────────────────────────────────────────────────────────

async function applyCurveCreated(db: DbClient, l: Log, log: Logger) {
  if (l.blockNumber == null || l.transactionHash == null) return;
  const decoded = decodeEventLog({
    abi: factoryEvents,
    data: l.data,
    topics: l.topics,
  });
  if (decoded.eventName !== "CurveCreated") return;
  const a = decoded.args;
  await db
    .insert(cbTokens)
    .values({
      curveAddress: a.curve.toLowerCase(),
      tokenAddress: a.token.toLowerCase(),
      ownerAddress: a.owner.toLowerCase(),
      name: a.name,
      symbol: a.symbol,
      curveSupply: a.curveSupply.toString(),
      graduationThreshold: a.graduationSrxThreshold.toString(),
      isGraduated: false,
      createdBlock: l.blockNumber,
      createdTxHash: l.transactionHash,
      totalVolumeSrx: "0",
      tradeCount: 0,
      lastPriceSrx: "0",
    })
    .onConflictDoNothing();
  log.info(
    { curve: a.curve, symbol: a.symbol, block: l.blockNumber.toString() },
    "coinblast: curve created",
  );
}

async function applyTrade(db: DbClient, l: Log, type: "buy" | "sell") {
  if (
    l.blockNumber == null ||
    l.transactionHash == null ||
    l.logIndex == null ||
    l.address == null
  ) {
    return;
  }
  const decoded = decodeEventLog({
    abi: curveEvents,
    data: l.data,
    topics: l.topics,
  });
  if (decoded.eventName !== "Buy" && decoded.eventName !== "Sell") return;

  const curve = l.address.toLowerCase();
  const args = decoded.args as
    | { buyer: `0x${string}`; srxIn: bigint; fee: bigint; tokensOut: bigint }
    | { seller: `0x${string}`; tokensIn: bigint; fee: bigint; srxOut: bigint };

  const trader = (
    "buyer" in args ? args.buyer : args.seller
  ).toLowerCase();
  const srxAmount = ("srxIn" in args ? args.srxIn : args.srxOut).toString();
  const tokenAmount = (
    "tokensOut" in args ? args.tokensOut : args.tokensIn
  ).toString();
  const fee = args.fee.toString();

  // Atomic: insert trade + update aggregates. ON CONFLICT on the unique
  // (tx_hash, log_index) lets us re-run the chunk safely — both the insert
  // AND the aggregate update are skipped on replay.
  await db.transaction(async (tx) => {
    const inserted = await tx
      .insert(cbTrades)
      .values({
        curveAddress: curve,
        tokenAddress: null,
        type,
        traderAddress: trader,
        srxAmount,
        tokenAmount,
        fee,
        blockNumber: l.blockNumber!,
        txHash: l.transactionHash!,
        logIndex: l.logIndex!,
      })
      .onConflictDoNothing()
      .returning({ id: cbTrades.id });

    if (inserted.length === 0) return; // replay; skip aggregates.

    // last_price = srx / tokens (integer division acceptable — we'll round
    // up when rendering). Skip if tokenAmount is zero (shouldn't happen,
    // but defensively avoid divide-by-zero).
    const lastPriceExpr =
      tokenAmount === "0"
        ? sql`${cbTokens.lastPriceSrx}`
        : sql`(${srxAmount}::numeric * 1e18 / ${tokenAmount}::numeric)::numeric(78,0)`;

    await tx
      .update(cbTokens)
      .set({
        totalVolumeSrx: sql`${cbTokens.totalVolumeSrx} + ${srxAmount}::numeric`,
        tradeCount: sql`${cbTokens.tradeCount} + 1`,
        lastPriceSrx: lastPriceExpr,
      })
      .where(eq(cbTokens.curveAddress, curve));
  });
}

async function applyGraduated(db: DbClient, l: Log) {
  if (
    l.blockNumber == null ||
    l.transactionHash == null ||
    l.logIndex == null ||
    l.address == null
  ) {
    return;
  }
  const decoded = decodeEventLog({
    abi: curveEvents,
    data: l.data,
    topics: l.topics,
  });
  if (decoded.eventName !== "Graduated") return;

  const curve = l.address.toLowerCase();
  const a = decoded.args as {
    pair: `0x${string}`;
    srxLiquidity: bigint;
    tokenLiquidity: bigint;
    lpBurned: bigint;
  };

  await db.transaction(async (tx) => {
    await tx
      .insert(cbTrades)
      .values({
        curveAddress: curve,
        tokenAddress: null,
        type: "graduated",
        traderAddress: a.pair.toLowerCase(),
        srxAmount: a.srxLiquidity.toString(),
        tokenAmount: a.tokenLiquidity.toString(),
        fee: "0",
        blockNumber: l.blockNumber!,
        txHash: l.transactionHash!,
        logIndex: l.logIndex!,
      })
      .onConflictDoNothing();

    await tx
      .update(cbTokens)
      .set({ isGraduated: true })
      .where(eq(cbTokens.curveAddress, curve));
  });
}

// ── Cursor helpers ────────────────────────────────────────────────────────

async function readCursor(
  db: DbClient,
  network: "mainnet" | "testnet",
): Promise<bigint> {
  const rows = await db
    .select({ value: meta.value })
    .from(meta)
    .where(eq(meta.key, META_KEY))
    .limit(1);
  if (rows[0]) return BigInt(rows[0].value);
  // First run — start one block before deploy so the very first chunk
  // includes the factory's deployment-tx events.
  return COINBLAST_DEPLOY_BLOCK[network] - 1n;
}

async function writeCursor(db: DbClient, cursor: bigint) {
  await db
    .insert(meta)
    .values({
      key: META_KEY,
      value: cursor.toString(),
      updatedAt: BigInt(Math.floor(Date.now() / 1000)),
    })
    .onConflictDoUpdate({
      target: meta.key,
      set: {
        value: sql`excluded.value`,
        updatedAt: sql`excluded.updated_at`,
      },
    });
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
