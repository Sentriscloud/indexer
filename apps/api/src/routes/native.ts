// Native Sentrix-shaped REST endpoints. Reads from the Postgres indexer DB.
// Etherscan-compatible shapes live in routes/etherscan.ts so we can keep this
// file focused on the natural shape (no `?module=...` cruft).

import { and, asc, desc, eq, lte, or, sql } from "drizzle-orm";
import type { FastifyInstance } from "fastify";

import {
  addresses,
  blocks,
  logs,
  tokenTransfers,
  transactions,
  type DbClient,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

const MAX_PAGE = 100;

// Safe BigInt parse — returns undefined on malformed input rather than
// throwing. Pre-fix sites used raw `BigInt(req.query.before)` which threw
// SyntaxError on non-numeric strings, surfaced as a Fastify 500 with no
// actionable error message. This wraps the throw so endpoints can return
// 400 with a clear "invalid cursor" message and stay alerter-quiet.
function parseBigIntOrThrow(raw: string, field: string): bigint {
  try {
    return BigInt(raw);
  } catch {
    throw new InvalidQueryError(`invalid ${field}: must be a non-negative integer`);
  }
}

class InvalidQueryError extends Error {
  constructor(msg: string) {
    super(msg);
    this.name = "InvalidQueryError";
  }
}

// /stats/daily moved off the chain native API in 2026-05-05 — at h~1.55M the
// on-chain handler scanned every block from genesis under the state read lock
// and hung the LB. Indexer side: a single GROUP BY against the timestamp-indexed
// `blocks` table runs in tens of ms over the full history, so the response
// covers the full chain (no 14-day cap) and we cache for 5 min to absorb burst.
const STATS_DAILY_TTL_MS = 5 * 60_000;
let statsDailyCache: { at: number; data: Array<{ date: string; blocks: number; transactions: number }> } | null = null;

export function registerNativeRoutes(
  app: FastifyInstance,
  ctx: { db: DbClient; chain: SentrixClient }
) {
  // ── /blocks ───────────────────────────────────────────────
  app.get<{ Querystring: { limit?: string; before?: string } }>(
    "/blocks",
    async (req, reply) => {
      const limit = clampLimit(req.query.limit);
      let before: bigint | undefined;
      if (req.query.before) {
        try { before = parseBigIntOrThrow(req.query.before, "before"); }
        catch (e) { return reply.code(400).send({ error: (e as Error).message }); }
      }
      const rows = await ctx.db
        .select()
        .from(blocks)
        .where(before !== undefined ? lte(blocks.height, before) : undefined)
        .orderBy(desc(blocks.height))
        .limit(limit);
      return { blocks: rows.map(serialiseBlock) };
    }
  );

  // ── /blocks/:height ───────────────────────────────────────
  app.get<{ Params: { height: string } }>("/blocks/:height", async (req, reply) => {
    let h: bigint;
    try { h = parseBigIntOrThrow(req.params.height, "height"); }
    catch (e) { return reply.code(400).send({ error: (e as Error).message }); }
    const row = await ctx.db.select().from(blocks).where(eq(blocks.height, h)).limit(1);
    if (!row[0]) return reply.code(404).send({ error: "block not found" });
    const txs = await ctx.db
      .select()
      .from(transactions)
      .where(eq(transactions.blockHeight, h))
      .orderBy(asc(transactions.txIndex));
    return { block: { ...serialiseBlock(row[0]), transactions: txs.map(serialiseTx) } };
  });

  // ── /tx/:hash ─────────────────────────────────────────────
  app.get<{ Params: { hash: string } }>("/tx/:hash", async (req, reply) => {
    const row = await ctx.db
      .select()
      .from(transactions)
      .where(eq(transactions.hash, req.params.hash.toLowerCase()))
      .limit(1);
    if (!row[0]) return reply.code(404).send({ error: "tx not found" });
    const ls = await ctx.db
      .select()
      .from(logs)
      .where(eq(logs.txHash, req.params.hash.toLowerCase()))
      .orderBy(asc(logs.logIndex));
    return { tx: serialiseTx(row[0]), logs: ls.map(serialiseLog) };
  });

  // ── /address/:addr/txs ────────────────────────────────────
  app.get<{
    Params: { addr: string };
    Querystring: { limit?: string };
  }>("/address/:addr/txs", async (req) => {
    const limit = clampLimit(req.query.limit);
    const a = req.params.addr.toLowerCase();
    const rows = await ctx.db
      .select()
      .from(transactions)
      .where(or(eq(transactions.fromAddr, a), eq(transactions.toAddr, a)))
      .orderBy(desc(transactions.blockHeight))
      .limit(limit);
    return { transactions: rows.map(serialiseTx) };
  });

  // ── /address/:addr/transfers ──────────────────────────────
  app.get<{
    Params: { addr: string };
    Querystring: { limit?: string; standard?: string };
  }>("/address/:addr/transfers", async (req) => {
    const limit = clampLimit(req.query.limit);
    const a = req.params.addr.toLowerCase();
    const std = req.query.standard;
    const where = std
      ? and(
          or(eq(tokenTransfers.fromAddr, a), eq(tokenTransfers.toAddr, a)),
          eq(tokenTransfers.standard, std)
        )
      : or(eq(tokenTransfers.fromAddr, a), eq(tokenTransfers.toAddr, a));
    const rows = await ctx.db
      .select()
      .from(tokenTransfers)
      .where(where)
      .orderBy(desc(tokenTransfers.blockHeight))
      .limit(limit);
    return { transfers: rows };
  });

  // ── /stats/daily ──────────────────────────────────────────
  // All-time daily activity (blocks + tx count). Used by scan analytics
  // page. Same JSON shape as the chain native handler so scan can swap
  // upstream without code change.
  app.get("/stats/daily", async () => {
    if (statsDailyCache && Date.now() - statsDailyCache.at < STATS_DAILY_TTL_MS) {
      return statsDailyCache.data;
    }
    const rows = await ctx.db.execute<{ date: string; blocks: string; transactions: string }>(
      sql`
        SELECT to_char(to_timestamp(timestamp::bigint), 'YYYY-MM-DD') AS date,
               count(*)::text AS blocks,
               COALESCE(sum(tx_count), 0)::text AS transactions
        FROM ${blocks}
        GROUP BY 1
        ORDER BY 1
      `
    );
    const data = (rows as unknown as Array<{ date: string; blocks: string; transactions: string }>).map(
      (r) => ({
        date: r.date,
        blocks: Number(r.blocks),
        transactions: Number(r.transactions),
      })
    );
    statsDailyCache = { at: Date.now(), data };
    return data;
  });

  // ── /accounts/active ──────────────────────────────────────
  // Most-active senders over all-time history (count tx by from_addr).
  // Distinct path from chain native /accounts/top (which sorts by
  // balance) so the Caddy edge can route this one to the indexer
  // without colliding with the existing richlist endpoint.
  app.get<{ Querystring: { limit?: string } }>(
    "/accounts/active",
    async (req) => {
      const limit = clampLimit(req.query.limit);
      const rows = await ctx.db.execute<{ address: string; tx_count: string }>(
        sql`
          SELECT from_addr AS address, count(*)::text AS tx_count
          FROM ${transactions}
          GROUP BY from_addr
          ORDER BY count(*) DESC
          LIMIT ${limit}
        `
      );
      return {
        accounts: (rows as unknown as Array<{ address: string; tx_count: string }>).map(
          (r, i) => ({ rank: i + 1, address: r.address, tx_count: Number(r.tx_count) })
        ),
      };
    }
  );

  // ── /contracts/stats ──────────────────────────────────────
  // Sort modes: `calls` (count tx with to_addr = contract), `gas_used`
  // (sum gas_used per contract). Reads `addresses.is_contract` to scope
  // the join — the indexer marks an address contract on first-deploy
  // detection (sync.ts).
  app.get<{ Querystring: { limit?: string; sort?: "calls" | "gas_used" } }>(
    "/contracts/stats",
    async (req) => {
      const limit = clampLimit(req.query.limit);
      const sort = req.query.sort === "gas_used" ? "gas_used" : "calls";
      if (sort === "gas_used") {
        const rows = await ctx.db.execute<{ address: string; gas_used: string; calls: string }>(
          sql`
            SELECT t.to_addr AS address,
                   COALESCE(sum(t.gas_used), 0)::text AS gas_used,
                   count(*)::text AS calls
            FROM ${transactions} t
            JOIN ${addresses} a ON a.address = t.to_addr AND a.is_contract = true
            WHERE t.to_addr IS NOT NULL
            GROUP BY t.to_addr
            ORDER BY sum(t.gas_used) DESC NULLS LAST
            LIMIT ${limit}
          `
        );
        return {
          contracts: (rows as unknown as Array<{ address: string; gas_used: string; calls: string }>).map(
            (r, i) => ({
              rank: i + 1,
              address: r.address,
              gas_used: Number(r.gas_used),
              calls: Number(r.calls),
            })
          ),
        };
      }
      const rows = await ctx.db.execute<{ address: string; calls: string; gas_used: string }>(
        sql`
          SELECT t.to_addr AS address,
                 count(*)::text AS calls,
                 COALESCE(sum(t.gas_used), 0)::text AS gas_used
          FROM ${transactions} t
          JOIN ${addresses} a ON a.address = t.to_addr AND a.is_contract = true
          WHERE t.to_addr IS NOT NULL
          GROUP BY t.to_addr
          ORDER BY count(*) DESC
          LIMIT ${limit}
        `
      );
      return {
        contracts: (rows as unknown as Array<{ address: string; calls: string; gas_used: string }>).map(
          (r, i) => ({
            rank: i + 1,
            address: r.address,
            calls: Number(r.calls),
            gas_used: Number(r.gas_used),
          })
        ),
      };
    }
  );

  // ── /contracts/recent ─────────────────────────────────────
  // List user-deployed contracts ordered by deployment height (most recent
  // first). Doesn't depend on the transactions table — works the moment
  // contract-detect.ts marks an address as is_contract=true, which happens
  // within ~4s of the address landing in the addresses table.
  //
  // Why a separate endpoint from /contracts/stats: stats does INNER JOIN
  // with transactions to count calls, so a freshly-deployed contract with
  // zero indexed calls (eg backfill not caught up yet) wouldn't show. This
  // endpoint is the "list everything we know is a contract" surface.
  app.get<{ Querystring: { limit?: string } }>(
    "/contracts/recent",
    async (req) => {
      const limit = clampLimit(req.query.limit);
      const rows = await ctx.db.execute<{
        address: string;
        first_seen_block: string;
        last_seen_block: string;
        code_hash: string | null;
      }>(
        sql`
          SELECT address,
                 first_seen_block::text,
                 last_seen_block::text,
                 code_hash
          FROM ${addresses}
          WHERE is_contract = true
          ORDER BY first_seen_block DESC
          LIMIT ${limit}
        `
      );
      return {
        contracts: (rows as unknown as Array<{
          address: string;
          first_seen_block: string;
          last_seen_block: string;
          code_hash: string | null;
        }>).map((r, i) => ({
          rank: i + 1,
          address: r.address,
          first_seen_block: Number(r.first_seen_block),
          last_seen_block: Number(r.last_seen_block),
          code_hash: r.code_hash,
        })),
      };
    }
  );

  // ── /whale/tx ─────────────────────────────────────────────
  // Whale transfers: top tx by `value` (native SRX in wei via numeric).
  // Used by scan leaderboard /whale/recent. Default threshold is the
  // top-N by value across all transactions; ?threshold=X filters to
  // tx with value >= X (in raw wei, 18-decimal). Scan side converts.
  app.get<{ Querystring: { limit?: string; threshold?: string } }>(
    "/whale/tx",
    async (req) => {
      const limit = clampLimit(req.query.limit);
      const threshold = req.query.threshold;
      const rows = threshold
        ? await ctx.db.execute<{
            hash: string;
            from_addr: string;
            to_addr: string | null;
            value: string;
            block_height: string;
            timestamp: string;
          }>(
            sql`
              SELECT t.hash, t.from_addr, t.to_addr, t.value::text,
                     t.block_height::text, b.timestamp::text
              FROM ${transactions} t
              JOIN ${blocks} b ON b.height = t.block_height
              WHERE t.value::numeric >= ${threshold}::numeric
              ORDER BY t.value::numeric DESC, t.block_height DESC
              LIMIT ${limit}
            `
          )
        : await ctx.db.execute<{
            hash: string;
            from_addr: string;
            to_addr: string | null;
            value: string;
            block_height: string;
            timestamp: string;
          }>(
            sql`
              SELECT t.hash, t.from_addr, t.to_addr, t.value::text,
                     t.block_height::text, b.timestamp::text
              FROM ${transactions} t
              JOIN ${blocks} b ON b.height = t.block_height
              ORDER BY t.value::numeric DESC, t.block_height DESC
              LIMIT ${limit}
            `
          );
      return {
        transfers: (rows as unknown as Array<{
          hash: string;
          from_addr: string;
          to_addr: string | null;
          value: string;
          block_height: string;
          timestamp: string;
        }>).map((r) => ({
          hash: r.hash,
          from: r.from_addr,
          to: r.to_addr,
          value: r.value,
          block_height: Number(r.block_height),
          timestamp: Number(r.timestamp),
        })),
      };
    }
  );
}

function clampLimit(raw: string | undefined): number {
  const n = raw ? parseInt(raw, 10) : 25;
  if (!Number.isFinite(n) || n <= 0) return 25;
  return Math.min(n, MAX_PAGE);
}

function serialiseBlock(b: typeof blocks.$inferSelect) {
  return {
    height: b.height.toString(),
    hash: b.hash,
    parent_hash: b.parentHash,
    timestamp: b.timestamp.toString(),
    validator: b.validator,
    gas_used: b.gasUsed.toString(),
    gas_limit: b.gasLimit.toString(),
    base_fee: b.baseFee,
    tx_count: b.txCount,
    state_root: b.stateRoot,
    round: b.round,
  };
}

function serialiseTx(t: typeof transactions.$inferSelect) {
  return {
    hash: t.hash,
    block_height: t.blockHeight.toString(),
    tx_index: t.txIndex,
    from: t.fromAddr,
    to: t.toAddr,
    value: t.value,
    gas_limit: t.gasLimit.toString(),
    gas_used: t.gasUsed?.toString() ?? null,
    gas_price: t.gasPrice,
    fee: t.fee,
    nonce: t.nonce.toString(),
    data: t.data,
    status: t.status,
    contract_address: t.contractAddress,
    tx_type: t.txType,
  };
}

function serialiseLog(l: typeof logs.$inferSelect) {
  return {
    block_height: l.blockHeight.toString(),
    tx_hash: l.txHash,
    log_index: l.logIndex,
    address: l.address,
    topics: [l.topic0, l.topic1, l.topic2, l.topic3].filter(Boolean),
    data: l.data,
  };
}
