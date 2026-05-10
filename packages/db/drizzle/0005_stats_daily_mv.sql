-- Materialised view for /stats/daily — replaces the per-process 5-min
-- in-memory cache that was lost on every restart. Stored on disk, shared
-- across api processes, refreshable concurrently without blocking reads.
--
-- The unique index on (date) is mandatory for REFRESH MATERIALIZED VIEW
-- CONCURRENTLY — Postgres requires at least one unique index on the view.
--
-- Refresh cadence is owned by the indexer worker (apps/indexer/src/index.ts
-- triggers REFRESH every N blocks). On a quiescent indexer the view stays
-- whatever it was on the last refresh; the API never blocks on freshness.

CREATE MATERIALIZED VIEW IF NOT EXISTS stats_daily_mv AS
  SELECT to_char(to_timestamp(timestamp::bigint), 'YYYY-MM-DD') AS date,
         count(*)::bigint AS blocks,
         COALESCE(sum(tx_count), 0)::bigint AS transactions
    FROM blocks
   GROUP BY 1
   ORDER BY 1;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS stats_daily_mv_date_uniq ON stats_daily_mv(date);
