# Table Partitioning Runbook

The indexer's three append-heavy tables — `transactions`, `logs`,
`token_transfers` — grow unbounded with chain height. At ~50M+ rows
each, query plans degrade in ways the existing single-column indexes
can't fully mitigate (sequential scans on selective filters, autovacuum
falling behind, page cache misses on cold partitions).

This doc captures the partitioning strategy + the migration recipe.
**Nothing here is auto-applied** — partitioning is a one-shot data
migration with a brief read-only window, so the operator runs it
deliberately when row counts cross the threshold.

## When to migrate

Trigger when ANY of:

- `transactions` row count > 50M (current mainnet ~1.7M blocks ≈ ?
  `SELECT count(*) FROM transactions`)
- p95 query latency on `/address/:addr/txs` > 200ms
- Autovacuum can't keep up — visible as growing `n_dead_tup` in
  `pg_stat_user_tables`

## Strategy: range partition by `block_height`

`block_height` is the natural partition key for all three tables —
chain history is append-only and queries are overwhelmingly
height-bounded (recent N blocks, address activity within a range,
tx by hash which still resolves to a height via the WHERE).

**Partition size:** 1M blocks per partition. Sentrix at 1s blocks =
~11.5 days per partition. Manageable index sizes (~5GB / partition at
current write rates), reasonable PG planner constant-folding cost,
and aligns roughly with monthly ops review cadence.

## Tables to partition

| Table | Partition key | Approx threshold |
|---|---|---|
| `transactions` | `block_height` | 50M rows |
| `logs` | `block_height` | 100M rows |
| `token_transfers` | `block_height` | 50M rows |

`addresses` and `blocks` stay non-partitioned — `addresses` is bounded
by unique address count (millions, not billions), and `blocks` is one
row per height (small).

## Migration recipe (per table)

This is the recipe for `transactions`. Repeat for `logs` and
`token_transfers` with the obvious substitutions.

```sql
-- 1. Brief read-only window. Block writes via revoke + reload
--    indexer worker so it backs off cleanly.
BEGIN;
LOCK TABLE transactions IN ACCESS EXCLUSIVE MODE;

-- 2. Rename existing table out of the way.
ALTER TABLE transactions RENAME TO transactions_legacy;

-- 3. Create the new partitioned root with the same schema.
CREATE TABLE transactions (
  hash             VARCHAR(66) NOT NULL,
  block_height     BIGINT      NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
  tx_index         INTEGER     NOT NULL,
  from_addr        VARCHAR(42) NOT NULL,
  to_addr          VARCHAR(42),
  value            NUMERIC(78, 0) NOT NULL DEFAULT '0',
  gas_limit        BIGINT      NOT NULL DEFAULT 0,
  gas_used         BIGINT      DEFAULT 0,
  gas_price        NUMERIC(78, 0),
  fee              NUMERIC(78, 0) NOT NULL DEFAULT '0',
  nonce            BIGINT      NOT NULL DEFAULT 0,
  data             TEXT,
  status           SMALLINT    NOT NULL DEFAULT 1,
  contract_address VARCHAR(42),
  tx_type          VARCHAR(24) NOT NULL DEFAULT 'native',
  PRIMARY KEY (hash, block_height)  -- partition key must be in PK
) PARTITION BY RANGE (block_height);

-- 4. Pre-create partitions for all of history + the next 10M blocks.
--    Adjust BACKFILL_END to current chain tip + slack.
DO $$
DECLARE
  start_height BIGINT := 0;
  end_height   BIGINT := 10000000;  -- adjust per current tip
  p_start      BIGINT;
  p_end        BIGINT;
  p_name       TEXT;
BEGIN
  FOR p_start IN SELECT generate_series(start_height, end_height - 1, 1000000) LOOP
    p_end := p_start + 1000000;
    p_name := format('transactions_p%s_%s', p_start, p_end);
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF transactions FOR VALUES FROM (%s) TO (%s)',
      p_name, p_start, p_end
    );
  END LOOP;
END $$;

-- 5. Copy data from legacy. INSERT routes each row to the right
--    partition automatically.
INSERT INTO transactions SELECT * FROM transactions_legacy;

-- 6. Recreate indexes on the partitioned root. Postgres propagates
--    these to every partition automatically.
CREATE INDEX txs_block_height_idx  ON transactions (block_height);
CREATE INDEX txs_from_idx          ON transactions (from_addr);
CREATE INDEX txs_to_idx            ON transactions (to_addr);
CREATE INDEX txs_contract_idx      ON transactions (contract_address);
CREATE INDEX txs_value_desc_idx    ON transactions (value);
CREATE INDEX txs_from_block_idx    ON transactions (from_addr, block_height);
CREATE INDEX txs_to_block_idx      ON transactions (to_addr, block_height);

-- 7. Verify the row count matches.
DO $$
DECLARE
  legacy_count BIGINT;
  new_count    BIGINT;
BEGIN
  SELECT count(*) INTO legacy_count FROM transactions_legacy;
  SELECT count(*) INTO new_count FROM transactions;
  IF legacy_count != new_count THEN
    RAISE EXCEPTION 'row count mismatch: legacy=% new=%', legacy_count, new_count;
  END IF;
END $$;

-- 8. Drop the legacy table.
DROP TABLE transactions_legacy;

COMMIT;
```

## Adding new partitions over time

Each partition holds 1M blocks ≈ 11.5 days. A weekly ops job creates
the next 4 partitions ahead of time so writes never block on a
missing partition:

```sql
-- Run weekly via cron in the indexer worker. Auto-creates the next
-- four 1M-block partitions if they don't already exist.
DO $$
DECLARE
  current_max BIGINT;
  next_p_start BIGINT;
  p_end BIGINT;
  p_name TEXT;
BEGIN
  SELECT COALESCE(max(end_value), 0) INTO current_max
  FROM (
    SELECT (regexp_match(relname, 'transactions_p\d+_(\d+)$'))[1]::BIGINT AS end_value
    FROM pg_class WHERE relname LIKE 'transactions_p%'
  ) t;

  FOR i IN 0..3 LOOP
    next_p_start := current_max + (i * 1000000);
    p_end := next_p_start + 1000000;
    p_name := format('transactions_p%s_%s', next_p_start, p_end);
    BEGIN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF transactions FOR VALUES FROM (%s) TO (%s)',
        p_name, next_p_start, p_end
      );
    EXCEPTION WHEN duplicate_table THEN
      -- Already exists, skip.
      NULL;
    END;
  END LOOP;
END $$;
```

## Drizzle compatibility

Drizzle's schema reflection reads the partitioned root as a normal
table — queries work unchanged because the planner handles partition
pruning via the `WHERE block_height = …` clauses every read endpoint
already produces. No Drizzle code changes needed.

The migration above uses raw SQL because Drizzle doesn't model
`PARTITION BY` declaratively. Mark it applied to
`drizzle.__drizzle_migrations` after running, or add a no-op Drizzle
migration that records the partition state in `_meta`.

## Rollback

If something breaks mid-migration, the legacy table is still there
(steps 5–8 are inside the same transaction). Roll back by:

```sql
ROLLBACK;
ALTER TABLE transactions_legacy RENAME TO transactions;
```

Outside the transaction (steps 8+ committed): `transactions_legacy`
is gone and the partitioned table is canonical. To revert from a
clean snapshot, restore from the most recent PG dump (operator
should run `pg_dump` before step 1 — adds ~10 min on a 50M-row table).

## What this PR does NOT do

- **No auto-migration** — the SQL above runs manually when the
  operator decides the threshold is hit. Auto-running on container
  boot would lock production for an unbounded window.
- **No Drizzle schema change** — the existing schema.ts continues to
  describe a non-partitioned table. Drizzle reads it as such; the
  planner does the partition routing at the SQL layer.
- **No pg_partman dependency** — the recipe uses vanilla Postgres
  declarative partitioning. pg_partman would automate the weekly
  partition-create job but adds an extension to the deploy. Worth
  evaluating if multiple operators need self-serve growth.
