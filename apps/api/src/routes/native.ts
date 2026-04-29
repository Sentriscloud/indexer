// Native Sentrix-shaped REST endpoints. Reads from the Postgres indexer DB.
// Etherscan-compatible shapes live in routes/etherscan.ts so we can keep this
// file focused on the natural shape (no `?module=...` cruft).

import { and, asc, desc, eq, lte, or } from "drizzle-orm";
import type { FastifyInstance } from "fastify";

import {
  blocks,
  logs,
  tokenTransfers,
  transactions,
  type DbClient,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

const MAX_PAGE = 100;

export function registerNativeRoutes(
  app: FastifyInstance,
  ctx: { db: DbClient; chain: SentrixClient }
) {
  // ── /blocks ───────────────────────────────────────────────
  app.get<{ Querystring: { limit?: string; before?: string } }>(
    "/blocks",
    async (req) => {
      const limit = clampLimit(req.query.limit);
      const before = req.query.before ? BigInt(req.query.before) : undefined;
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
    const h = BigInt(req.params.height);
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
