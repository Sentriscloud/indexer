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
import { runMigrations } from "@sentriscloud/indexer-db/migrate";
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
  // Apply any pending migrations BEFORE we open the long-lived db pool +
  // start the sync loop. Drizzle's __drizzle_migrations table makes this
  // idempotent so a restart on an up-to-date schema is a no-op (~10 ms).
  // Without this every deploy of new SQL needed a separate manual
  // `pnpm db:migrate` step that was easy to forget.
  log.info("running migrations");
  await runMigrations(DB_URL);
  log.info("migrations applied");

  const db = createDb(DB_URL);
  const chain = new SentrixClient({ network: NETWORK });

  // Health server.
  const app = Fastify({ logger: false });
  app.get("/health", async () => {
    // Cap the chain probe at 3 s so /health stays under Docker's 5 s
    // healthcheck timeout when the chain RPC is unreachable. Same fix
    // applied to apps/api in PR #14 — restart loop during chain
    // outage is exactly what we don't want from the indexer worker.
    const tip = await Promise.race([
      chain.getBlockNumber(),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 3000)),
    ]).catch(() => null);
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
  // Errors here (5xx burst, network blip, mid-block fetch failure) used
  // to bubble up out of main and crash the process — Docker restart
  // recovers but loses warm caches and re-runs already-indexed blocks
  // from scratch. Retry-with-backoff inside the loop keeps the worker
  // alive through transient chain wobble. Idempotent inserts on the
  // sync.ts side mean re-running the same height after partial failure
  // is safe.
  log.info("starting backfill phase");
  let backoffMs = 1_000;
  while (true) {
    try {
      const tip = await chain.getBlockNumber();
      const target = tip - SAFE_LAG;
      const synced = await syncOnce({ db, chain, target, log });
      if (synced >= target) break;
      backoffMs = 1_000;
    } catch (err) {
      log.warn(
        { err: String(err), backoff_ms: backoffMs },
        "backfill iteration failed — retrying after backoff",
      );
      await new Promise((r) => setTimeout(r, backoffMs));
      backoffMs = Math.min(backoffMs * 2, 30_000);
    }
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
  // Pending-tip queue. When a new event arrives while inflight is busy,
  // we used to drop it ('if (inflight) return') and rely on the *next*
  // chain block to re-trigger the catch-up loop. That assumption fails
  // if the chain halts right after a burst — missed blocks stay
  // unindexed until either a new chain block lands or the indexer is
  // restarted into Phase 1. Track the latest dropped height instead so
  // the in-flight loop drains to it before releasing the gate.
  let pendingTip: bigint | null = null;
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
      // overlapping pushes don't double-write — but record the latest
      // pending tip so the in-flight loop drains to it before releasing.
      if (inflight) {
        if (pendingTip === null || ev.height > pendingTip) {
          pendingTip = ev.height;
        }
        return;
      }
      inflight = (async () => {
        try {
          let target = ev.height;
          while (true) {
            const cur = await readMeta(db, "last_synced_height");
            let h = cur + 1n;
            while (h <= target) {
              await indexBlock({ db, chain, height: h, log });
              h++;
            }
            // Did a later event come in while we were running? Drain it
            // before letting go of the gate, otherwise a burst tail (eg
            // chain halts right after) leaves the dropped heights stuck
            // until the next chain block triggers another callback.
            if (pendingTip !== null && pendingTip > target) {
              target = pendingTip;
              pendingTip = null;
              continue;
            }
            pendingTip = null;
            break;
          }
        } catch (err) {
          log.error({ err: String(err) }, "tail indexBlock failed");
        } finally {
          inflight = null;
        }
      })();
    },
    {
      // Log throttling: during a long chain outage the stream can fail
      // hundreds of times (each retry = ~8 s at the cap). Without these
      // throttles we'd dump 1000+ identical lines into the journal.
      // Errors: log first occurrence, then once per minute.
      // Reconnect attempts: log at powers of 2 (2, 4, 8, 16, 32, …) so
      // the curve stays visible without spamming.
      onError: (() => {
        let lastLog = 0;
        return (err: unknown) => {
          const now = Date.now();
          if (now - lastLog > 60_000) {
            log.warn({ err: String(err) }, "grpc stream error (reconnecting)");
            lastLog = now;
          }
        };
      })(),
      onReconnect: (attempt) => {
        if (attempt <= 4 || (attempt & (attempt - 1)) === 0) {
          log.info({ attempt }, "grpc stream reconnect");
        }
      },
    },
  );

  // ── stats_daily_mv refresh ────────────────────────────────────
  // The materialised view backing /stats/daily must be refreshed for
  // the API to see new blocks/transactions. CONCURRENTLY refresh
  // doesn't block readers (requires the unique index on date, present
  // since migration 0005). 5 min cadence balances freshness vs. PG
  // load — the API also caches the view-read result for 60 s at the
  // edge (see apps/api/src/cache-control.ts), so the worst-case
  // freshness gap a user sees is ~6 min.
  const STATS_REFRESH_INTERVAL_MS = Number(
    process.env.INDEXER_STATS_REFRESH_INTERVAL_MS ?? 5 * 60_000,
  );
  // Initial seed so the view is non-empty before the first interval fires.
  // Uses non-CONCURRENT path because the unique index isn't valid until
  // the first non-concurrent populate completes.
  try {
    await db.execute(sql`REFRESH MATERIALIZED VIEW stats_daily_mv`);
    log.info("stats_daily_mv initial refresh ok");
  } catch (err) {
    log.warn({ err: String(err) }, "stats_daily_mv initial refresh failed (view may not exist yet — run migrations)");
  }
  const statsRefreshTimer = setInterval(async () => {
    try {
      await db.execute(sql`REFRESH MATERIALIZED VIEW CONCURRENTLY stats_daily_mv`);
    } catch (err) {
      log.warn({ err: String(err) }, "stats_daily_mv refresh failed");
    }
  }, STATS_REFRESH_INTERVAL_MS);

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
    clearInterval(statsRefreshTimer);
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
