// REST API for the indexer DB. Phase 1 endpoints — see README.
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import Fastify from "fastify";

import { createDb } from "@sentriscloud/indexer-db";
import { SentrixClient } from "@sentriscloud/indexer-chain";

import { registerNativeRoutes } from "./routes/native.js";
import { registerEtherscanCompat } from "./routes/etherscan.js";
import { registerHealthRoutes } from "./routes/health.js";

const PORT = Number(process.env.API_PORT ?? 8081);
const HOST = process.env.API_HOST ?? "0.0.0.0";
const DB_URL =
  process.env.INDEXER_DATABASE_URL ??
  "postgres://indexer:indexer@localhost:5432/sentrix_indexer";
const NETWORK = (process.env.INDEXER_NETWORK ?? "mainnet") as
  | "mainnet"
  | "testnet";

async function main() {
  const app = Fastify({
    logger: { level: process.env.LOG_LEVEL ?? "info" },
  });

  await app.register(cors, { origin: true });
  await app.register(rateLimit, {
    max: Number(process.env.API_RATE_LIMIT ?? 120),
    timeWindow: "1 minute",
    cache: 10_000,
  });

  const db = createDb(DB_URL);
  const chain = new SentrixClient({ network: NETWORK });

  registerHealthRoutes(app, { db, chain, network: NETWORK });
  registerNativeRoutes(app, { db, chain });
  registerEtherscanCompat(app, { db, chain });

  await app.listen({ host: HOST, port: PORT });
  app.log.info({ port: PORT, network: NETWORK }, "indexer api up");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
