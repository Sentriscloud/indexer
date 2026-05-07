// CoinBlast launchpad endpoints. Reads cb_tokens / cb_trades populated by
// apps/indexer/src/coinblast/worker.ts. Surface mirrors what the launchpad
// frontend currently fetches client-side via getLogs — the goal is for the
// frontend to drop its useDeployedCurves / useCurveTradeStats hooks and
// hit these endpoints instead, once the launchpad outgrows direct RPC scan.

import { and, asc, desc, eq, gte, lt, ne, sql } from "drizzle-orm";
import type { FastifyInstance } from "fastify";
import { recoverMessageAddress, isAddress, type Hex } from "viem";

import {
  cbTokens,
  cbTrades,
  type DbClient,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

const MAX_PAGE = 100;

// Safe BigInt parse — returns parsed value or throws InvalidQueryError so
// the route handler can convert to a 400 response. Pre-fix sites used
// raw BigInt(req.query.*) which threw SyntaxError on non-numeric input,
// surfacing as Fastify 500 instead of an actionable 400.
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

// Reasonable upper-bound caps on metadata field lengths. The DB schema
// uses unbounded `text` so the limit lives at the API edge.
const MAX_IMAGE_URL = 256;
const MAX_DESCRIPTION = 1024;
const MAX_LINK_URL = 256;
// 5-minute window — sig replay window. Stamp older than this is rejected.
const MAX_AGE_MS = 5 * 60 * 1000;

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
  }>("/coinblast/tokens", async (req, reply) => {
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
      try { conds.push(lt(cbTokens.createdBlock, parseBigIntOrThrow(req.query.before, "before"))); }
      catch (e) { return reply.code(400).send({ error: (e as Error).message }); }
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
  }>("/coinblast/trades", async (req, reply) => {
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
      try { conds.push(lt(cbTrades.blockNumber, parseBigIntOrThrow(req.query.before, "before"))); }
      catch (e) { return reply.code(400).send({ error: (e as Error).message }); }
    }
    const rows = await ctx.db
      .select()
      .from(cbTrades)
      .where(conds.length ? and(...conds) : undefined)
      .orderBy(desc(cbTrades.blockNumber), desc(cbTrades.logIndex))
      .limit(limit);
    return { trades: rows.map(serialiseTrade) };
  });

  // ── /coinblast/whales ───────────────────────────────────────
  // Buys + sells whose srx_amount crosses a threshold (default 100 SRX).
  // Threshold is a decimal SRX value; we convert to wei (×1e18) inside
  // the query against numeric(78,0) `cb_trades.srx_amount`. Graduations
  // are excluded — they're one-shot supply migrations, not user trades,
  // and their srx_amount is the total bonded liquidity which would
  // dominate the panel.
  //
  // Order: srx_amount desc tie-broken by block desc, so the panel
  // surfaces the biggest single trade in the window first and falls
  // back to recency for equal-size whales.
  app.get<{
    Querystring: { limit?: string; threshold?: string };
  }>("/coinblast/whales", async (req) => {
    const limit = clampLimit(req.query.limit);
    const thresholdSrx = parseFloat(req.query.threshold ?? "100");
    if (!Number.isFinite(thresholdSrx) || thresholdSrx <= 0) {
      return { trades: [] };
    }
    // Multiply by 1e18 in pg-side numeric to keep precision. JS floats
    // would silently round above ~2^53.
    const thresholdWei = sql<string>`(${thresholdSrx}::numeric * 1e18::numeric)`;
    const rows = await ctx.db
      .select()
      .from(cbTrades)
      .where(
        and(
          ne(cbTrades.type, "graduated"),
          gte(cbTrades.srxAmount, thresholdWei),
        ),
      )
      .orderBy(desc(cbTrades.srxAmount), desc(cbTrades.blockNumber))
      .limit(limit);
    return { trades: rows.map(serialiseTrade) };
  });

  // ── POST /coinblast/metadata ───────────────────────────────
  // Owner-only metadata update for a curve. Auth via EIP-191 signed
  // message: the curve's `owner_address` (recovered from the supplied
  // signature) must match what the indexer recorded on CurveCreated.
  // Without a signature anyone could overwrite anyone else's icon to
  // grief the launchpad.
  //
  // Message shape (kept simple — the curve address + nonce-ish stamp
  // are enough; the body itself is in the request, replay-windowed by
  // the stamp):
  //
  //   sentrix:cb-meta:<curve_lower>:<stamp_ms>
  //
  // Stamp is a unix-ms timestamp. Server rejects if it's outside a
  // 5-minute window (forward + back) so a leaked sig can't be replayed
  // a day later by a third party.
  app.post<{
    Body: {
      curve_address?: string;
      stamp_ms?: number;
      signature?: string;
      image_url?: string | null;
      description?: string | null;
      twitter_url?: string | null;
      telegram_url?: string | null;
      website_url?: string | null;
    };
  }>("/coinblast/metadata", async (req, reply) => {
    const body = req.body ?? {};
    const curve = body.curve_address?.toLowerCase();
    if (!curve || !isAddress(curve)) {
      return reply.code(400).send({ error: "invalid curve_address" });
    }
    if (typeof body.stamp_ms !== "number" || !Number.isFinite(body.stamp_ms)) {
      return reply.code(400).send({ error: "missing stamp_ms" });
    }
    const ageMs = Math.abs(Date.now() - body.stamp_ms);
    if (ageMs > MAX_AGE_MS) {
      return reply
        .code(400)
        .send({ error: "stamp_ms outside 5-minute window" });
    }
    if (!body.signature || !/^0x[0-9a-fA-F]+$/.test(body.signature)) {
      return reply.code(400).send({ error: "missing signature" });
    }

    // Length-cap each field. Null is OK (clears the field); strings get
    // trimmed at the cap. We don't strip HTML — current frontend renders
    // these as plain text — but URL fields are still scheme-checked at
    // the edge so a future consumer can't get sandbagged by a stored
    // `javascript:` href.
    const imageUrl = clampUrl(body.image_url, MAX_IMAGE_URL);
    const description = clampField(body.description, MAX_DESCRIPTION);
    const twitterUrl = clampUrl(body.twitter_url, MAX_LINK_URL);
    const telegramUrl = clampUrl(body.telegram_url, MAX_LINK_URL);
    const websiteUrl = clampUrl(body.website_url, MAX_LINK_URL);
    if (
      (body.image_url != null && imageUrl === null && body.image_url !== "") ||
      (body.twitter_url != null && twitterUrl === null && body.twitter_url !== "") ||
      (body.telegram_url != null && telegramUrl === null && body.telegram_url !== "") ||
      (body.website_url != null && websiteUrl === null && body.website_url !== "")
    ) {
      return reply
        .code(400)
        .send({ error: "url fields must start with http:// or https://" });
    }

    // Look up the curve to get its on-chain owner. If it's not in
    // cb_tokens the indexer hasn't seen the CurveCreated event yet —
    // either it was just deployed (transient) or it's not real.
    const row = await ctx.db
      .select()
      .from(cbTokens)
      .where(eq(cbTokens.curveAddress, curve))
      .limit(1);
    if (!row[0]) {
      return reply.code(404).send({ error: "curve not indexed yet" });
    }

    const message = `sentrix:cb-meta:${curve}:${body.stamp_ms}`;
    let recovered: string;
    try {
      recovered = (
        await recoverMessageAddress({
          message,
          signature: body.signature as Hex,
        })
      ).toLowerCase();
    } catch (err) {
      return reply
        .code(400)
        .send({ error: "signature recovery failed: " + String(err) });
    }
    if (recovered !== row[0].ownerAddress.toLowerCase()) {
      return reply
        .code(403)
        .send({ error: "signer is not curve owner" });
    }

    await ctx.db
      .update(cbTokens)
      .set({
        imageUrl,
        description,
        twitterUrl,
        telegramUrl,
        websiteUrl,
        metadataUpdatedAt: BigInt(Date.now()),
      })
      .where(eq(cbTokens.curveAddress, curve));

    return { ok: true, curve_address: curve };
  });

  // ── /coinblast/trades/by-curve/:curve ──────────────────────
  // Convenience for the per-token activity feed. Same as /coinblast/trades
  // with curve= but ordered ascending so a paginated chart plots left→right.
  app.get<{
    Params: { curve: string };
    Querystring: { limit?: string; after?: string };
  }>("/coinblast/trades/by-curve/:curve", async (req, reply) => {
    const limit = clampLimit(req.query.limit);
    const conds = [eq(cbTrades.curveAddress, req.params.curve.toLowerCase())];
    if (req.query.after) {
      try { conds.push(eq(cbTrades.blockNumber, parseBigIntOrThrow(req.query.after, "after"))); }
      catch (e) { return reply.code(400).send({ error: (e as Error).message }); }
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

// Treat both null and "" as "clear this field". Trim whitespace on
// non-empty strings + cap to maxLen so a single huge POST can't blow
// up the row size beyond what the launchpad UI is built to render.
function clampField(
  raw: string | null | undefined,
  maxLen: number,
): string | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (trimmed.length === 0) return null;
  return trimmed.slice(0, maxLen);
}

// Same as clampField but enforces an http(s) scheme on top — blocks
// `javascript:`, `data:`, `file:`, `vbscript:` and friends. The route
// caller treats `null` on a non-empty input as a validation error so
// users see a 400 instead of a silent clear.
function clampUrl(
  raw: string | null | undefined,
  maxLen: number,
): string | null {
  const v = clampField(raw, maxLen);
  if (v === null) return null;
  if (!/^https?:\/\//i.test(v)) return null;
  return v;
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
    image_url: t.imageUrl ?? null,
    description: t.description ?? null,
    twitter_url: t.twitterUrl ?? null,
    telegram_url: t.telegramUrl ?? null,
    website_url: t.websiteUrl ?? null,
    metadata_updated_at: t.metadataUpdatedAt
      ? t.metadataUpdatedAt.toString()
      : null,
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
