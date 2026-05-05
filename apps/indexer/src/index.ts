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
import { startContractDetector } from "./contract-detect.js";
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

  // Contract-detect worker — flips addresses.is_contract=true for addresses
  // with non-empty bytecode. Runs in parallel to backfill + tip on a slow
  // cadence so it never starves the hot path.
  const stopContractDetector = startContractDetector({ db, chain, log });

  // Phase 1 — backfill.
  log.info("starting backfill phase");
  while (true) {
    const tip = await chain.getBlockNumber();
    const target = tip - SAFE_LAG;
    const synced = await syncOnce({ db, chain, target, log });
    if (synced >= target) break;
  }
  log.info("backfill caught up to tip - SAFE_LAG; entering tail phase");

  // Phase 2 — tail (gRPC server-streaming push, v2.1.71+).
  // Push architecture: server's EventBus.new_heads broadcast → gRPC
  // BlockFinalized → this callback. No polling, no setInterval. On stream
  // drop (process restart / network blip), the client reconnects with
  // exponential backoff; on a Lagged sentinel (consumer 1024+ events
  // behind), we trigger a JSON-RPC backfill catch-up rather than silently
  // missing blocks.
  let inflight: Promise<void> | null = null;
  const startTail = Date.now();
  let dashTicks = 0;

  const watcher = chain.streamBlocks(
    async (ev) => {
      if (ev.kind === "lagged") {
        log.warn({ skipped: ev.skipped.toString() }, "stream lagged — resyncing via JSON-RPC backfill");
        // Drain everything from synced+1 to the current tip via JSON-RPC.
        // Single-flight gate also covers this path.
        if (!inflight) {
          inflight = (async () => {
            try {
              const tip = await chain.getBlockNumber();
              const synced = await readMeta(db, "last_synced_height");
              for (let h = synced + 1n; h <= tip; h++) {
                await indexBlock({ db, chain, height: h, log });
              }
            } catch (err) {
              log.error({ err: String(err) }, "lagged-resync failed");
            } finally {
              inflight = null;
            }
          })();
        }
        return;
      }

      const synced = await readMeta(db, "last_synced_height");
      const lag = ev.height - synced;
      log.info(
        {
          tip: ev.height.toString(),
          synced: synced.toString(),
          lag: lag.toString(),
          latency_ms: ev.latencyMs,
          ticks: ++dashTicks,
          uptime_s: Math.floor((Date.now() - startTail) / 1000),
        },
        "tip",
      );

      // Index every block from synced+1 to the new tip. Single-flight so
      // overlapping pushes don't double-write.
      if (inflight) return;
      inflight = (async () => {
        try {
          let h = synced + 1n;
          while (h <= ev.height) {
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
      onError: (err) => log.warn({ err: String(err) }, "grpc stream error (reconnecting)"),
      onReconnect: (attempt) => log.info({ attempt }, "grpc stream reconnect"),
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
    try {
      stopContractDetector();
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
