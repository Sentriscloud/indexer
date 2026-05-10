-- Composite indexes for paginated address-history queries.
--
-- Drizzle wraps a migration in a transaction, so we use plain `CREATE
-- INDEX IF NOT EXISTS` rather than `CONCURRENTLY` (the latter can't run
-- inside a transaction). On a quiescent indexer this is fine. On a
-- write-active production indexer with multi-million-row tables, the
-- operator should pre-create each index manually first via psql with
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS …` to avoid blocking writes;
-- the IF NOT EXISTS guard then makes this migration a no-op when run.

CREATE INDEX IF NOT EXISTS "transfers_from_block_idx" ON "token_transfers" USING btree ("from_addr","block_height");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "transfers_to_block_idx" ON "token_transfers" USING btree ("to_addr","block_height");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_from_block_idx" ON "transactions" USING btree ("from_addr","block_height");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_to_block_idx" ON "transactions" USING btree ("to_addr","block_height");
