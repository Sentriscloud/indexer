// Run all pending migrations against the DB. Two entry points:
//   1) CLI:       `pnpm --filter @sentriscloud/indexer-db migrate`
//   2) Embedded:  `import { runMigrations } from "@sentriscloud/indexer-db/migrate"`
//                 used from apps/indexer/src/index.ts at container boot
//                 so a fresh image with new SQL applies it before the
//                 sync loop starts. Comment in the original file claimed
//                 this entrypoint already wired it; in practice the
//                 indexer never imported it, so a `docker compose up -d`
//                 with new migrations did nothing — operator had to
//                 remember `pnpm db:migrate` separately every deploy.
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { fileURLToPath } from "node:url";
import path from "node:path";

// Resolve the drizzle/ folder relative to this file rather than CWD so the
// embedded path works no matter where the calling process is rooted (vs.
// the CLI which always runs from packages/db).
const HERE = path.dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_FOLDER = path.resolve(HERE, "..", "drizzle");

export async function runMigrations(url: string): Promise<void> {
  const sql = postgres(url, { max: 1, prepare: false });
  const db = drizzle(sql);
  try {
    await migrate(db, { migrationsFolder: MIGRATIONS_FOLDER });
  } finally {
    await sql.end();
  }
}

// CLI entry — run only when invoked directly, not when imported.
const isCli =
  process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isCli) {
  const url =
    process.env.INDEXER_DATABASE_URL ??
    "postgres://indexer:indexer@localhost:5432/sentrix_indexer";
  runMigrations(url).catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
