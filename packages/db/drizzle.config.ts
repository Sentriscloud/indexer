import type { Config } from "drizzle-kit";

export default {
  schema: "./src/schema.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: {
    url:
      process.env.INDEXER_DATABASE_URL ??
      "postgres://indexer:indexer@localhost:5432/sentrix_indexer",
  },
  strict: true,
  verbose: true,
} satisfies Config;
