import { eq } from "drizzle-orm";
import type { FastifyInstance } from "fastify";

import { meta, type DbClient } from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

export function registerHealthRoutes(
  app: FastifyInstance,
  ctx: { db: DbClient; chain: SentrixClient; network: string }
) {
  app.get("/health", async () => {
    const tip = await ctx.chain.getBlockNumber().catch(() => null);
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
