// Indexer worker entry point. Two phases at startup:
//   1. Backfill — pull history from `last_synced_height + 1` to `tip - SAFE_LAG`
//      in batches. We stop short of the tip so we never race the BFT finalizer
//      writing a block whose justification we'd partial-read.
//   2. Tail — subscribe to newHeads, refetch each block by number, persist.
//      Reorg detection: if `block.parentHash !== lastSeenBlock.hash`, walk
//      back N blocks and re-sync.
//
// A tiny Fastify health endpoint runs alongside so docker compose / Caddy
// can probe the container.

import Fastify from "fastify";
import pino from "pino";

import { createDb, blocks, meta } from "@sentriscloud/indexer-db";
import { SentrixClient } from "@sentriscloud/indexer-chain";
import { eq, sql } from "drizzle-orm";

import { syncOnce, indexBlock } from "./sync.js";

const log = pino({ name: "indexer", level: process.env.LOG_LEVEL ?? "info" });

const DB_URL =
  process.env.INDEXER_DATABASE_URL ??
  "postgres://indexer:indexer@localhost:5432/sentrix_indexer";

const NETWORK = (process.env.INDEXER_NETWORK ?? "mainnet") as "mainnet" | "testnet";
const HEALTH_PORT = Number(process.env.INDEXER_HEALTH_PORT ?? 8082);
const SAFE_LAG = BigInt(process.env.INDEXER_SAFE_LAG ?? 5);

async function main() {
  const db = createDb(DB_URL);
  const chain = new SentrixClient({ network: NETWORK });

  // Health server.
  const app = Fastify({ logger: false });
  app.get("/health", async () => {
    const tip = await chain.getBlockNumber().catch(() => null);
    const synced = await db
      .select({ value: meta.value })
      .from(meta)
      .where(eq(meta.key, "last_synced_height"))
      .limit(1)
      .then((rows) => (rows[0]?.value ? BigInt(rows[0].value) : 0n));
    const lag = tip !== null ? tip - synced : null;
    return {
      status: tip !== null && lag !== null && lag < 50n ? "ok" : "lagging",
      network: NETWORK,
      tip: tip?.toString() ?? null,
      synced: synced.toString(),
      lag: lag?.toString() ?? null,
    };
  });
  await app.listen({ host: "0.0.0.0", port: HEALTH_PORT });
  log.info({ port: HEALTH_PORT }, "health server listening");

  // Phase 1 — backfill.
  log.info("starting backfill phase");
  while (true) {
    const tip = await chain.getBlockNumber();
    const target = tip - SAFE_LAG;
    const synced = await syncOnce({ db, chain, target, log });
    if (synced >= target) break;
  }
  log.info("backfill caught up to tip - SAFE_LAG; entering tail phase");

  // Phase 2 — tail.
  const unwatch = chain.watchBlocks((n) => {
    indexBlock({ db, chain, height: n, log }).catch((err) => {
      log.error({ err: String(err), height: n.toString() }, "tail indexBlock failed");
    });
  });

  // Graceful shutdown.
  const shutdown = async (sig: string) => {
    log.info({ sig }, "shutting down");
    try {
      unwatch();
    } catch {
      /* ignore */
    }
    await app.close().catch(() => {});
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  log.fatal({ err: String(err) }, "indexer crashed");
  process.exit(1);
});

// ---- helpers used to silence lint of unused imports in skeleton state ----
void blocks;
void sql;
