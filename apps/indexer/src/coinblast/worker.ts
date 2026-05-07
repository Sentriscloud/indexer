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
  // appended to this set either as we process CurveCreated events, or
  // when pass-2's global topic-scan adopts a direct-deployed orphan.
  const knownCurves = new Set<string>();
  const existing = await db.select({ a: cbTokens.curveAddress }).from(cbTokens);
  for (const row of existing) knownCurves.add(row.a.toLowerCase());
  log.info({ count: knownCurves.size }, "coinblast: hydrated known curves");

  // Memo of addresses that emitted a Buy/Sell/Graduated topic but failed
  // the on-chain CoinBlastCurve probe — saves us from re-validating them
  // on every chunk pass. Bounded by the number of unrelated contracts
  // chain-wide that happen to use the same event signatures, which in
  // practice is small; clearing the set on a long-running worker isn't
  // worth the bookkeeping.
  const knownNonCurves = new Set<string>();

  let cursor = await readCursor(db, network);
  log.info({ cursor: cursor.toString() }, "coinblast: starting backfill");

  // Backfill loop — walks forward until cursor >= tip - SAFE_LAG, then
  // continues in tail mode. Same code path; the upper bound is just
  // recomputed each iteration.
  //
  // The outer try/catch covers getBlockNumber() too, not just runChunk —
  // pre-fix a chain blip during the tip read crashed the worker out of
  // the while-loop into the caller's `.catch` in index.ts which logs
  // 'coinblast worker exited unexpectedly' and never restarts it.
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      const tip = await chain.getBlockNumber();
      const target = tip - SAFE_LAG;
      if (cursor >= target) {
        // Caught up. Sleep a bit then re-check.
        await sleep(TICK_MS * 4);
        continue;
      }

      const from = cursor + 1n;
      const to = from + CHUNK - 1n > target ? target : from + CHUNK - 1n;

      await runChunk({
        db,
        chain,
        log,
        factory,
        knownCurves,
        knownNonCurves,
        from,
        to,
      });
      cursor = to;
      await writeCursor(db, cursor);
      log.info(
        { from: from.toString(), to: to.toString(), curves: knownCurves.size },
        "coinblast: chunk done",
      );
    } catch (err) {
      log.error(
        { err: String(err), cursor: cursor.toString() },
        "coinblast: iteration failed; retrying after backoff",
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
  knownNonCurves: Set<string>;
  from: bigint;
  to: bigint;
}

async function runChunk(args: ChunkArgs) {
  const { db, chain, log, factory, knownCurves, knownNonCurves, from, to } =
    args;

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

  // Pass 2: scan globally by event signature, no address filter. Picks up
  // direct-deployed curves (CoinBlast Genesis was launched this way before
  // the factory route shipped) — we lazy-create their cb_tokens rows on
  // first sighting so future events are tracked normally. Topic0 collision
  // defense: validate any unknown emitter is a real CoinBlastCurve via an
  // on-chain `token()` read before we touch the DB; cache the result so we
  // don't pay for the lookup more than once per address.
  const curveLogs = await chain.http.getLogs({
    fromBlock: from,
    toBlock: to,
    events: curveEvents,
  });

  for (const l of curveLogs) {
    if (!l.address) continue;
    const addr = l.address.toLowerCase();

    if (!knownCurves.has(addr)) {
      if (knownNonCurves.has(addr)) continue; // collision, already vetted out
      const isCurve = await tryAdoptOrphanCurve(db, chain, l, log);
      if (!isCurve) {
        knownNonCurves.add(addr);
        continue;
      }
      knownCurves.add(addr);
    }

    const t0 = l.topics[0];
    if (t0 === TOPIC_BUY) await applyTrade(db, l, "buy");
    else if (t0 === TOPIC_SELL) await applyTrade(db, l, "sell");
    else if (t0 === TOPIC_GRADUATED) await applyGraduated(db, l);
  }
}

// Validate an unknown event emitter is a real CoinBlastCurve and, if so,
// insert a cb_tokens row for it so the rest of the worker's pipeline can
// process its events normally. Returns true on adopt, false if the
// address turns out not to be one of ours (topic0 collision with an
// unrelated contract).
//
// Owner is recorded as the zero address — we don't have CurveCreated to
// pull it from, and chasing the contract-creation tx isn't worth the
// extra RPC round-trip per orphan. Operators can backfill the owner
// manually from the deploy tx if it ever matters for a UI surface.
async function tryAdoptOrphanCurve(
  db: DbClient,
  chain: SentrixClient,
  l: Log,
  log: Logger,
): Promise<boolean> {
  if (!l.address || !l.blockNumber || !l.transactionHash) return false;
  const addr = l.address.toLowerCase() as `0x${string}`;

  // Probe the curve's view surface. A real CoinBlastCurve answers all
  // three; an unrelated contract that happens to emit a same-shape Buy
  // event won't, and the readContract call reverts. Run them in parallel
  // and bail on the first reject.
  let token: `0x${string}`;
  let curveSupply: bigint;
  let graduationThreshold: bigint;
  try {
    [token, curveSupply, graduationThreshold] = await Promise.all([
      chain.http.readContract({
        address: addr,
        abi: CURVE_VIEWS_ABI,
        functionName: "token",
      }) as Promise<`0x${string}`>,
      chain.http.readContract({
        address: addr,
        abi: CURVE_VIEWS_ABI,
        functionName: "curveSupply",
      }) as Promise<bigint>,
      chain.http.readContract({
        address: addr,
        abi: CURVE_VIEWS_ABI,
        functionName: "graduationSrxThreshold",
      }) as Promise<bigint>,
    ]);
  } catch {
    return false;
  }

  // Read name + symbol off the underlying token. These are decoration —
  // if either fails (token contract is non-standard) we fall back to
  // placeholders rather than refusing to adopt the curve, since the
  // trade data is the part that matters.
  const [name, symbol] = await Promise.all([
    chain.http
      .readContract({
        address: token,
        abi: ERC20_META_ABI,
        functionName: "name",
      })
      .then((v) => v as string)
      .catch(() => "Unknown"),
    chain.http
      .readContract({
        address: token,
        abi: ERC20_META_ABI,
        functionName: "symbol",
      })
      .then((v) => v as string)
      .catch(() => "???"),
  ]);

  await db
    .insert(cbTokens)
    .values({
      curveAddress: addr,
      tokenAddress: token.toLowerCase(),
      ownerAddress: "0x0000000000000000000000000000000000000000",
      name,
      symbol,
      curveSupply: curveSupply.toString(),
      graduationThreshold: graduationThreshold.toString(),
      isGraduated: false,
      createdBlock: l.blockNumber,
      createdTxHash: l.transactionHash,
      totalVolumeSrx: "0",
      tradeCount: 0,
      lastPriceSrx: "0",
    })
    .onConflictDoNothing();

  log.info(
    { curve: addr, symbol, block: l.blockNumber.toString() },
    "coinblast: adopted orphan curve (direct-deploy, no factory event)",
  );
  return true;
}

// Minimal ABIs for the orphan-adoption probe. Kept local so events.ts
// stays focused on signatures the worker decodes; these are only used
// for `readContract` calls.
const CURVE_VIEWS_ABI = [
  {
    type: "function",
    stateMutability: "view",
    name: "token",
    inputs: [],
    outputs: [{ type: "address" }],
  },
  {
    type: "function",
    stateMutability: "view",
    name: "curveSupply",
    inputs: [],
    outputs: [{ type: "uint256" }],
  },
  {
    type: "function",
    stateMutability: "view",
    name: "graduationSrxThreshold",
    inputs: [],
    outputs: [{ type: "uint256" }],
  },
] as const;

const ERC20_META_ABI = [
  {
    type: "function",
    stateMutability: "view",
    name: "name",
    inputs: [],
    outputs: [{ type: "string" }],
  },
  {
    type: "function",
    stateMutability: "view",
    name: "symbol",
    inputs: [],
    outputs: [{ type: "string" }],
  },
] as const;

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
