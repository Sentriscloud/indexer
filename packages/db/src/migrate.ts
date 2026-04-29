// Run all pending migrations against the DB. Used in `pnpm db:migrate` and
// in the indexer entrypoint at boot before the sync loop starts.
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";

const url =
  process.env.INDEXER_DATABASE_URL ??
  "postgres://indexer:indexer@localhost:5432/sentrix_indexer";

const sql = postgres(url, { max: 1, prepare: false });
const db = drizzle(sql);

async function main() {
  await migrate(db, { migrationsFolder: "./drizzle" });
  await sql.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
