// Indexer worker entry point. Two phases at startup:
//   1. Backfill — pull history from `last_synced_height + 1` to `tip - SAFE_LAG`
//      in batches via JSON-RPC eth_getBlockByNumber. gRPC GetBlock has a
//      ~1000-block in-memory window so it's not usable for historical reads;
//      JSON-RPC stays the canonical path for backfill.
//   2. Tail — gRPC `GetBlock {latest:true}` polled at INDEXER_TIP_INTERVAL_MS
//      (default 200 ms) detects new tips with sub-200 ms latency. We refetch
//      the full block via JSON-RPC for tx + log bodies (gRPC v0.2 returns
//      empty `transactions`; that lands in v0.3 alongside StreamEvents).
//
// A tiny Fastify health endpoint runs alongside so docker compose / Caddy
// can probe the container.

import Fastify from "fastify";
import pino from "pino";

import { createDb, blocks, meta } from "@sentriscloud/indexer-db";
import { SentrixClient } from "@sentriscloud/indexer-chain";
import { eq, sql } from "drizzle-orm";

import { syncOnce, indexBlock } from "./sync.js";
import { runCoinblastWorker } from "./coinblast/worker.js";

const log = pino({ name: "indexer", level: process.env.LOG_LEVEL ?? "info" });

// Module-scope: also used by the gRPC tip watcher to compute lag without
// re-implementing the same SELECT inline.
async function readMeta(
  db: ReturnType<typeof createDb>,
  key: string,
): Promise<bigint> {
  const rows = await db
    .select({ value: meta.value })
    .from(meta)
    .where(eq(meta.key, key))
    .limit(1);
  return rows[0]?.value ? BigInt(rows[0].value) : 0n;
}

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
    const synced = await readMeta(db, "last_synced_height");
    const cbSynced = await readMeta(db, "last_synced_coinblast_height");
    const lag = tip !== null ? tip - synced : null;
    const cbLag = tip !== null ? tip - cbSynced : null;
    return {
      status: tip !== null && lag !== null && lag < 50n ? "ok" : "lagging",
      network: NETWORK,
      tip: tip?.toString() ?? null,
      synced: synced.toString(),
      lag: lag?.toString() ?? null,
      coinblast: {
        synced: cbSynced.toString(),
        lag: cbLag?.toString() ?? null,
      },
    };
  });
  await app.listen({ host: "0.0.0.0", port: HEALTH_PORT });
  log.info({ port: HEALTH_PORT }, "health server listening");

  // CoinBlast worker — kicked off BEFORE the chain-wide Phase 1 backfill so
  // it runs in parallel. Without this, the chain-wide `while(true)` below
  // would block until the genesis-up backfill catches up to tip-SAFE_LAG
  // (~80 days at current 10 blocks/min), and the CoinBlast cursor would
  // sit at 0 the whole time. Independent cursor + own retry path means a
  // crash here doesn't tear down the chain-wide worker.
  runCoinblastWorker({ db, chain, network: NETWORK, log }).catch((err) => {
    log.error({ err: String(err) }, "coinblast worker exited unexpectedly");
  });

  // Phase 1 — backfill.
  log.info("starting backfill phase");
  while (true) {
    const tip = await chain.getBlockNumber();
    const target = tip - SAFE_LAG;
    const synced = await syncOnce({ db, chain, target, log });
    if (synced >= target) break;
  }
  log.info("backfill caught up to tip - SAFE_LAG; entering tail phase");

  // Phase 2 — tail (gRPC tip watcher).
  // Single-flight gate: only one indexBlock at a time. If the tip advances
  // by N during a slow indexBlock, we'll catch up sequentially on the next
  // tick (the watcher only fires on advance, not on each interval).
  let inflight: Promise<void> | null = null;
  const tipIntervalMs = Number(process.env.INDEXER_TIP_INTERVAL_MS ?? 200);

  const startTail = Date.now();
  let dashTicks = 0;

  const watcher = chain.watchTipGrpc(
    async (tipHeight, latencyMs) => {
      // Dashboard line: [tip] [synced] [lag] [latency_ms] — one per advance.
      const synced = await readMeta(db, "last_synced_height");
      const lag = tipHeight - synced;
      log.info(
        {
          tip: tipHeight.toString(),
          synced: synced.toString(),
          lag: lag.toString(),
          latency_ms: latencyMs,
          ticks: ++dashTicks,
          uptime_s: Math.floor((Date.now() - startTail) / 1000),
        },
        "tip",
      );

      // Index every block from synced+1 to the new tip. Single-flight so
      // overlapping ticks don't double-write.
      if (inflight) return;
      inflight = (async () => {
        try {
          let h = synced + 1n;
          while (h <= tipHeight) {
            await indexBlock({ db, chain, height: h, log });
            h++;
          }
        } catch (err) {
          log.error({ err: String(err) }, "tail indexBlock failed");
        } finally {
          inflight = null;
        }
      })();
    },
    {
      intervalMs: tipIntervalMs,
      onError: (err) => log.warn({ err: String(err) }, "grpc tip watcher error (backing off)"),
    },
  );

  // Graceful shutdown.
  const shutdown = async (sig: string) => {
    log.info({ sig }, "shutting down");
    try {
      watcher.stop();
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
