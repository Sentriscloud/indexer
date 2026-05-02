// CoinBlast launchpad endpoints. Reads cb_tokens / cb_trades populated by
// apps/indexer/src/coinblast/worker.ts. Surface mirrors what the launchpad
// frontend currently fetches client-side via getLogs — the goal is for the
// frontend to drop its useDeployedCurves / useCurveTradeStats hooks and
// hit these endpoints instead, once the launchpad outgrows direct RPC scan.

import { and, asc, desc, eq, lt } from "drizzle-orm";
import type { FastifyInstance } from "fastify";

import {
  cbTokens,
  cbTrades,
  type DbClient,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

const MAX_PAGE = 100;

export function registerCoinblastRoutes(
  app: FastifyInstance,
  ctx: { db: DbClient; chain: SentrixClient },
) {
  // ── /coinblast/tokens ──────────────────────────────────────
  // Filters: graduated=true|false, owner=0x..., before=<created_block>
  // Pagination: cursor-based on created_block (descending = newest first).
  app.get<{
    Querystring: {
      limit?: string;
      graduated?: string;
      owner?: string;
      before?: string;
    };
  }>("/coinblast/tokens", async (req) => {
    const limit = clampLimit(req.query.limit);
    const conds = [];
    if (req.query.graduated === "true") {
      conds.push(eq(cbTokens.isGraduated, true));
    } else if (req.query.graduated === "false") {
      conds.push(eq(cbTokens.isGraduated, false));
    }
    if (req.query.owner) {
      conds.push(eq(cbTokens.ownerAddress, req.query.owner.toLowerCase()));
    }
    if (req.query.before) {
      conds.push(lt(cbTokens.createdBlock, BigInt(req.query.before)));
    }
    const rows = await ctx.db
      .select()
      .from(cbTokens)
      .where(conds.length ? and(...conds) : undefined)
      .orderBy(desc(cbTokens.createdBlock))
      .limit(limit);
    return { tokens: rows.map(serialiseToken) };
  });

  // ── /coinblast/tokens/:curve ───────────────────────────────
  app.get<{ Params: { curve: string } }>(
    "/coinblast/tokens/:curve",
    async (req, reply) => {
      const row = await ctx.db
        .select()
        .from(cbTokens)
        .where(eq(cbTokens.curveAddress, req.params.curve.toLowerCase()))
        .limit(1);
      if (!row[0]) return reply.code(404).send({ error: "curve not found" });
      return { token: serialiseToken(row[0]) };
    },
  );

  // ── /coinblast/trades ──────────────────────────────────────
  // Filters: curve=0x..., trader=0x..., type=buy|sell|graduated
  // Order: most recent first (block desc, log_index desc).
  app.get<{
    Querystring: {
      limit?: string;
      curve?: string;
      trader?: string;
      type?: string;
      before?: string; // block number
    };
  }>("/coinblast/trades", async (req) => {
    const limit = clampLimit(req.query.limit);
    const conds = [];
    if (req.query.curve) {
      conds.push(eq(cbTrades.curveAddress, req.query.curve.toLowerCase()));
    }
    if (req.query.trader) {
      conds.push(eq(cbTrades.traderAddress, req.query.trader.toLowerCase()));
    }
    if (req.query.type) {
      conds.push(eq(cbTrades.type, req.query.type));
    }
    if (req.query.before) {
      conds.push(lt(cbTrades.blockNumber, BigInt(req.query.before)));
    }
    const rows = await ctx.db
      .select()
      .from(cbTrades)
      .where(conds.length ? and(...conds) : undefined)
      .orderBy(desc(cbTrades.blockNumber), desc(cbTrades.logIndex))
      .limit(limit);
    return { trades: rows.map(serialiseTrade) };
  });

  // ── /coinblast/trades/by-curve/:curve ──────────────────────
  // Convenience for the per-token activity feed. Same as /coinblast/trades
  // with curve= but ordered ascending so a paginated chart plots left→right.
  app.get<{
    Params: { curve: string };
    Querystring: { limit?: string; after?: string };
  }>("/coinblast/trades/by-curve/:curve", async (req) => {
    const limit = clampLimit(req.query.limit);
    const conds = [eq(cbTrades.curveAddress, req.params.curve.toLowerCase())];
    if (req.query.after) {
      conds.push(eq(cbTrades.blockNumber, BigInt(req.query.after)));
    }
    const rows = await ctx.db
      .select()
      .from(cbTrades)
      .where(and(...conds))
      .orderBy(asc(cbTrades.blockNumber), asc(cbTrades.logIndex))
      .limit(limit);
    return { trades: rows.map(serialiseTrade) };
  });
}

function clampLimit(raw: string | undefined): number {
  const n = raw ? parseInt(raw, 10) : 25;
  if (!Number.isFinite(n) || n <= 0) return 25;
  return Math.min(n, MAX_PAGE);
}

function serialiseToken(t: typeof cbTokens.$inferSelect) {
  return {
    curve_address: t.curveAddress,
    token_address: t.tokenAddress,
    owner_address: t.ownerAddress,
    name: t.name,
    symbol: t.symbol,
    curve_supply: t.curveSupply,
    graduation_threshold: t.graduationThreshold,
    is_graduated: t.isGraduated,
    created_block: t.createdBlock.toString(),
    created_tx_hash: t.createdTxHash,
    total_volume_srx: t.totalVolumeSrx,
    trade_count: t.tradeCount,
    last_price_srx: t.lastPriceSrx,
  };
}

function serialiseTrade(t: typeof cbTrades.$inferSelect) {
  return {
    id: t.id.toString(),
    curve_address: t.curveAddress,
    token_address: t.tokenAddress,
    type: t.type,
    trader_address: t.traderAddress,
    srx_amount: t.srxAmount,
    token_amount: t.tokenAmount,
    fee: t.fee,
    block_number: t.blockNumber.toString(),
    tx_hash: t.txHash,
    log_index: t.logIndex,
  };
}
