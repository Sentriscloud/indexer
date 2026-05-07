import { eq } from "drizzle-orm";
import type { FastifyInstance } from "fastify";

import { meta, type DbClient } from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

export function registerHealthRoutes(
  app: FastifyInstance,
  ctx: { db: DbClient; chain: SentrixClient; network: string }
) {
  app.get("/health", async () => {
    // Cap the chain probe at 3 s so /health responds within Docker's 5 s
    // healthcheck timeout even when the chain RPC is unreachable (mid-
    // migration, network blip). Without this the route hangs on viem's
    // ~10 s default × retry429 = ~22 s, container goes unhealthy →
    // restart loop right when we least want it.
    const tip = await Promise.race([
      ctx.chain.getBlockNumber(),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 3000)),
    ]).catch(() => null);
    const synced = await ctx.db
      .select({ value: meta.value })
      .from(meta)
      .where(eq(meta.key, "last_synced_height"))
      .limit(1)
      .then((rows) => (rows[0]?.value ? BigInt(rows[0].value) : 0n));

    const lag = tip !== null ? tip - synced : null;
    const ok = tip !== null && lag !== null && lag < 50n;
    return {
      status: ok ? "ok" : "lagging",
      network: ctx.network,
      tip: tip?.toString() ?? null,
      synced: synced.toString(),
      lag: lag?.toString() ?? null,
    };
  });
}
