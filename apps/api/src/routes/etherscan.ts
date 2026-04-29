// Etherscan-API-compatible shape. Lots of dApp tooling (ethers, hardhat-etherscan,
// blockscan, sourcify) speaks this dialect, so this is the "drop-in for any
// existing tool" entry point. Only a Phase 1 subset is implemented; unknown
// modules return a friendly 400 instead of 404 so consumers see what's missing.

import { desc, eq, or } from "drizzle-orm";
import type { FastifyInstance } from "fastify";

import {
  tokenTransfers,
  transactions,
  type DbClient,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

interface EsQuery {
  module?: string;
  action?: string;
  address?: string;
  startblock?: string;
  endblock?: string;
  page?: string;
  offset?: string;
  sort?: "asc" | "desc";
  contractaddress?: string;
}

const HARD_CAP_SRX_WEI = "315000000000000000000000000"; // 315M * 1e18

export function registerEtherscanCompat(
  app: FastifyInstance,
  ctx: { db: DbClient; chain: SentrixClient }
) {
  app.get<{ Querystring: EsQuery }>("/api", async (req, reply) => {
    const { module, action } = req.query;
    if (!module || !action) {
      return reply
        .code(400)
        .send({ status: "0", message: "missing module/action", result: null });
    }
    if (module === "account") return handleAccount(ctx, req.query);
    if (module === "stats") return handleStats(ctx, req.query);
    if (module === "block") return handleBlock(ctx, req.query);
    return reply
      .code(400)
      .send({ status: "0", message: `module ${module} not implemented`, result: null });
  });
}

async function handleAccount(
  ctx: { db: DbClient; chain: SentrixClient },
  q: EsQuery
) {
  const addr = (q.address ?? "").toLowerCase();
  if (!addr.startsWith("0x"))
    return { status: "0", message: "missing address", result: null };

  switch (q.action) {
    case "balance": {
      const wei = await ctx.chain.http.getBalance({ address: addr as `0x${string}` });
      return { status: "1", message: "OK", result: wei.toString() };
    }
    case "txlist": {
      const limit = clampLimit(q.offset);
      const order = q.sort === "asc" ? "asc" : "desc";
      const rows = await ctx.db
        .select()
        .from(transactions)
        .where(or(eq(transactions.fromAddr, addr), eq(transactions.toAddr, addr)))
        .orderBy(
          order === "asc" ? transactions.blockHeight : desc(transactions.blockHeight)
        )
        .limit(limit);
      return {
        status: rows.length ? "1" : "0",
        message: rows.length ? "OK" : "No transactions found",
        result: rows.map(toEtherscanTx),
      };
    }
    case "tokentx": {
      const limit = clampLimit(q.offset);
      const rows = await ctx.db
        .select()
        .from(tokenTransfers)
        .where(or(eq(tokenTransfers.fromAddr, addr), eq(tokenTransfers.toAddr, addr)))
        .orderBy(desc(tokenTransfers.blockHeight))
        .limit(limit);
      return {
        status: rows.length ? "1" : "0",
        message: rows.length ? "OK" : "No transfers found",
        result: rows.map(toEtherscanTransfer),
      };
    }
  }
  return { status: "0", message: `account/${q.action} not implemented`, result: null };
}

async function handleStats(
  ctx: { db: DbClient; chain: SentrixClient },
  q: EsQuery
) {
  switch (q.action) {
    case "ethsupply":
    case "srxsupply":
      return { status: "1", message: "OK", result: HARD_CAP_SRX_WEI };
    case "ethsupplyExt":
      return {
        status: "1",
        message: "OK",
        result: { TotalSupply: HARD_CAP_SRX_WEI, Burnt: "0" },
      };
    case "ethprice":
      return { status: "0", message: "no oracle wired", result: null };
  }
  return { status: "0", message: `stats/${q.action} not implemented`, result: null };
}

async function handleBlock(
  ctx: { db: DbClient; chain: SentrixClient },
  q: EsQuery
) {
  switch (q.action) {
    case "getblocknobytime":
      return { status: "0", message: "not implemented", result: null };
  }
  return { status: "0", message: `block/${q.action} not implemented`, result: null };
}

function clampLimit(raw: string | undefined): number {
  const n = raw ? parseInt(raw, 10) : 25;
  if (!Number.isFinite(n) || n <= 0) return 25;
  return Math.min(n, 100);
}

function toEtherscanTx(t: typeof transactions.$inferSelect) {
  return {
    blockNumber: t.blockHeight.toString(),
    hash: t.hash,
    from: t.fromAddr,
    to: t.toAddr ?? "",
    value: t.value,
    gas: t.gasLimit.toString(),
    gasPrice: t.gasPrice ?? "0",
    isError: t.status === 0 ? "1" : "0",
    txreceipt_status: t.status.toString(),
    input: t.data ?? "0x",
    nonce: t.nonce.toString(),
    transactionIndex: t.txIndex.toString(),
    contractAddress: t.contractAddress ?? "",
  };
}

function toEtherscanTransfer(tt: typeof tokenTransfers.$inferSelect) {
  return {
    blockNumber: tt.blockHeight.toString(),
    hash: tt.txHash,
    contractAddress: tt.contract,
    from: tt.fromAddr,
    to: tt.toAddr,
    value: tt.amount,
    tokenID: tt.tokenId ?? "",
    tokenStandard: tt.standard,
  };
}
