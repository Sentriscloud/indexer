// Drizzle schema — single source of truth for indexer DB.
//
// Notes on type choices:
//   - `numeric(78, 0)` for u256 wei-scale values: native SRX is 8-decimal
//     (1 SRX = 10^8 sentri) but EVM token contracts wrap that into 18-decimal
//     amounts inside revm. We carry the EVM 18-decimal shape as the wire
//     format and let the API layer translate when we render native SRX.
//   - `bytea` for hashes / topics / addresses — keeps the on-disk size
//     half of what hex strings would, and Drizzle exposes them as `Buffer`.
//     The query helpers (`packages/db/src/queries.ts`) take + return hex.
//   - `bigint` for block heights / timestamps / nonces — Sentrix's u64 fits.

import { sql } from "drizzle-orm";
import {
  bigint,
  boolean,
  index,
  integer,
  jsonb,
  numeric,
  pgTable,
  primaryKey,
  smallint,
  text,
  uniqueIndex,
  varchar,
} from "drizzle-orm/pg-core";

export const blocks = pgTable(
  "blocks",
  {
    height: bigint("height", { mode: "bigint" }).primaryKey(),
    hash: varchar("hash", { length: 66 }).notNull().unique(),
    parentHash: varchar("parent_hash", { length: 66 }).notNull(),
    timestamp: bigint("timestamp", { mode: "bigint" }).notNull(),
    validator: varchar("validator", { length: 42 }).notNull(),
    gasUsed: bigint("gas_used", { mode: "bigint" }).notNull().default(sql`0`),
    gasLimit: bigint("gas_limit", { mode: "bigint" }).notNull().default(sql`0`),
    baseFee: numeric("base_fee", { precision: 78, scale: 0 }),
    txCount: integer("tx_count").notNull().default(0),
    stateRoot: varchar("state_root", { length: 66 }),
    round: integer("round").notNull().default(0),
    justificationSigners: jsonb("justification_signers")
      .$type<string[]>()
      .default(sql`'[]'::jsonb`),
  },
  (t) => ({
    validatorIdx: index("blocks_validator_idx").on(t.validator),
    timestampIdx: index("blocks_timestamp_idx").on(t.timestamp),
  })
);

export const transactions = pgTable(
  "transactions",
  {
    hash: varchar("hash", { length: 66 }).primaryKey(),
    blockHeight: bigint("block_height", { mode: "bigint" })
      .notNull()
      .references(() => blocks.height, { onDelete: "cascade" }),
    txIndex: integer("tx_index").notNull(),
    fromAddr: varchar("from_addr", { length: 42 }).notNull(),
    toAddr: varchar("to_addr", { length: 42 }),
    value: numeric("value", { precision: 78, scale: 0 }).notNull().default("0"),
    gasLimit: bigint("gas_limit", { mode: "bigint" }).notNull().default(sql`0`),
    gasUsed: bigint("gas_used", { mode: "bigint" }).default(sql`0`),
    gasPrice: numeric("gas_price", { precision: 78, scale: 0 }),
    fee: numeric("fee", { precision: 78, scale: 0 }).notNull().default("0"),
    nonce: bigint("nonce", { mode: "bigint" }).notNull().default(sql`0`),
    data: text("data"),
    status: smallint("status").notNull().default(1), // 1 = success, 0 = failed
    contractAddress: varchar("contract_address", { length: 42 }),
    txType: varchar("tx_type", { length: 24 }).notNull().default("native"),
    // "native" | "evm" | "system" | "coinbase"
  },
  (t) => ({
    blockHeightIdx: index("txs_block_height_idx").on(t.blockHeight),
    fromIdx: index("txs_from_idx").on(t.fromAddr),
    toIdx: index("txs_to_idx").on(t.toAddr),
    contractIdx: index("txs_contract_idx").on(t.contractAddress),
    // /whale/tx — ORDER BY value DESC (numeric(78,0) column).
    // Default B-tree handles DESC fine via index scan in reverse.
    valueDescIdx: index("txs_value_desc_idx").on(t.value),
  })
);

export const logs = pgTable(
  "logs",
  {
    blockHeight: bigint("block_height", { mode: "bigint" })
      .notNull()
      .references(() => blocks.height, { onDelete: "cascade" }),
    txHash: varchar("tx_hash", { length: 66 })
      .notNull()
      .references(() => transactions.hash, { onDelete: "cascade" }),
    logIndex: integer("log_index").notNull(),
    address: varchar("address", { length: 42 }).notNull(),
    topic0: varchar("topic0", { length: 66 }),
    topic1: varchar("topic1", { length: 66 }),
    topic2: varchar("topic2", { length: 66 }),
    topic3: varchar("topic3", { length: 66 }),
    data: text("data"),
  },
  (t) => ({
    pk: primaryKey({ columns: [t.blockHeight, t.logIndex] }),
    addressIdx: index("logs_address_idx").on(t.address),
    topic0Idx: index("logs_topic0_idx").on(t.topic0),
    txIdx: index("logs_tx_idx").on(t.txHash),
  })
);

// Decoded token transfers — materialised from logs via worker.
// Carries ERC-20 (Transfer), ERC-721 (Transfer), ERC-1155 (TransferSingle / TransferBatch).
export const tokenTransfers = pgTable(
  "token_transfers",
  {
    id: bigint("id", { mode: "bigint" })
      .primaryKey()
      .generatedAlwaysAsIdentity(),
    blockHeight: bigint("block_height", { mode: "bigint" }).notNull(),
    txHash: varchar("tx_hash", { length: 66 }).notNull(),
    logIndex: integer("log_index").notNull(),
    contract: varchar("contract", { length: 42 }).notNull(),
    standard: varchar("standard", { length: 12 }).notNull(), // erc20 | erc721 | erc1155
    fromAddr: varchar("from_addr", { length: 42 }).notNull(),
    toAddr: varchar("to_addr", { length: 42 }).notNull(),
    tokenId: numeric("token_id", { precision: 78, scale: 0 }), // null for erc20
    amount: numeric("amount", { precision: 78, scale: 0 }).notNull(),
  },
  (t) => ({
    contractIdx: index("transfers_contract_idx").on(t.contract),
    fromIdx: index("transfers_from_idx").on(t.fromAddr),
    toIdx: index("transfers_to_idx").on(t.toAddr),
    blockIdx: index("transfers_block_idx").on(t.blockHeight),
  })
);

export const addresses = pgTable(
  "addresses",
  {
    address: varchar("address", { length: 42 }).primaryKey(),
    firstSeenBlock: bigint("first_seen_block", { mode: "bigint" }).notNull(),
    lastSeenBlock: bigint("last_seen_block", { mode: "bigint" }).notNull(),
    balanceCached: numeric("balance_cached", { precision: 78, scale: 0 }).default("0"),
    nonce: bigint("nonce", { mode: "bigint" }).default(sql`0`),
    isContract: boolean("is_contract").notNull().default(false),
    codeHash: varchar("code_hash", { length: 66 }),
  },
  (t) => ({
    // Composite for /contracts/recent — WHERE is_contract = true
    // ORDER BY first_seen_block DESC. Leading is_contract narrows fast,
    // first_seen_block sorts inside the narrowed slice.
    contractRecentIdx: index("addresses_contract_recent_idx").on(
      t.isContract,
      t.firstSeenBlock,
    ),
  }),
);

export const validators = pgTable("validators", {
  address: varchar("address", { length: 42 }).primaryKey(),
  moniker: varchar("moniker", { length: 64 }),
  commissionBp: integer("commission_bp"),
  selfStake: numeric("self_stake", { precision: 78, scale: 0 }).default("0"),
  totalDelegated: numeric("total_delegated", { precision: 78, scale: 0 }).default("0"),
  blocksProposed: bigint("blocks_proposed", { mode: "bigint" }).default(sql`0`),
  lastActiveBlock: bigint("last_active_block", { mode: "bigint" }),
  isJailed: boolean("is_jailed").notNull().default(false),
  jailUntil: bigint("jail_until", { mode: "bigint" }),
});

export const epochs = pgTable("epochs", {
  epochNumber: bigint("epoch_number", { mode: "bigint" }).primaryKey(),
  startHeight: bigint("start_height", { mode: "bigint" }).notNull(),
  endHeight: bigint("end_height", { mode: "bigint" }).notNull(),
  validatorSet: jsonb("validator_set").$type<string[]>().notNull(),
  totalStaked: numeric("total_staked", { precision: 78, scale: 0 }).default("0"),
  totalBlocksProduced: bigint("total_blocks_produced", { mode: "bigint" }).default(sql`0`),
});

// Indexer-internal state — last_synced_height, last_reorg_block, etc.
export const meta = pgTable("_meta", {
  key: varchar("key", { length: 64 }).primaryKey(),
  value: text("value").notNull(),
  updatedAt: bigint("updated_at", { mode: "bigint" }).notNull(),
});

// ── CoinBlast launchpad ────────────────────────────────────────────────────
//
// Materialised view of CoinBlast — one row per curve in `cb_tokens`, one row
// per Buy/Sell/Graduate event in `cb_trades`. Populated by a separate worker
// (apps/indexer/src/coinblast/) that uses its own cursor (`_meta` key
// `last_synced_coinblast_height`) and scans logs filtered by factory + curve
// addresses. Independent of the chain-wide block-by-block sync — needed
// because the chain-wide indexer is still backfilling from genesis while
// the CoinBlast factory only deployed at block 1,178,667.
//
// `curve_address` is the natural primary key for cb_tokens (one curve per
// launch, immutable, gas-derived → unique). No surrogate UUID — it would
// double the row width without buying anything.

export const cbTokens = pgTable(
  "cb_tokens",
  {
    curveAddress: varchar("curve_address", { length: 42 }).primaryKey(),
    tokenAddress: varchar("token_address", { length: 42 }).notNull().unique(),
    ownerAddress: varchar("owner_address", { length: 42 }).notNull(),
    name: text("name").notNull(),
    symbol: text("symbol").notNull(),
    curveSupply: numeric("curve_supply", { precision: 78, scale: 0 }).notNull(),
    graduationThreshold: numeric("graduation_threshold", {
      precision: 78,
      scale: 0,
    }).notNull(),
    isGraduated: boolean("is_graduated").notNull().default(false),
    // Block height of the CurveCreated event. Lets us order chronologically
    // without joining `blocks` for the timestamp.
    createdBlock: bigint("created_block", { mode: "bigint" }).notNull(),
    createdTxHash: varchar("created_tx_hash", { length: 66 }).notNull(),
    // Aggregated stats — updated in the same SQL transaction as the trade.
    totalVolumeSrx: numeric("total_volume_srx", { precision: 78, scale: 0 })
      .notNull()
      .default("0"),
    tradeCount: integer("trade_count").notNull().default(0),
    // srx_per_token of the latest Buy/Sell trade. Useful for the launchpad
    // grid — avoids a per-card subquery into cb_trades.
    lastPriceSrx: numeric("last_price_srx", { precision: 78, scale: 0 })
      .notNull()
      .default("0"),
    // 2026-05-06: rich metadata fields written by POST /coinblast/metadata
    // (sig-gated to the curve owner). Image is the IPFS URI from /api/pin
    // on the launchpad frontend; the rest are optional links the launcher
    // can attach. NULL until the owner posts metadata — frontend falls
    // back to MOCK_TOKENS / localStorage when these are missing so legacy
    // launches without metadata don't render as blank cards.
    imageUrl: text("image_url"),
    description: text("description"),
    twitterUrl: text("twitter_url"),
    telegramUrl: text("telegram_url"),
    websiteUrl: text("website_url"),
    metadataUpdatedAt: bigint("metadata_updated_at", { mode: "bigint" }),
  },
  (t) => ({
    ownerIdx: index("cb_tokens_owner_idx").on(t.ownerAddress),
    graduatedIdx: index("cb_tokens_graduated_idx").on(t.isGraduated),
    createdBlockIdx: index("cb_tokens_created_block_idx").on(t.createdBlock),
  })
);

export const cbTrades = pgTable(
  "cb_trades",
  {
    id: bigint("id", { mode: "bigint" })
      .primaryKey()
      .generatedAlwaysAsIdentity(),
    curveAddress: varchar("curve_address", { length: 42 }).notNull(),
    tokenAddress: varchar("token_address", { length: 42 }),
    // "buy" | "sell" | "graduated". Graduated is one-shot per curve so we
    // could split it off — keeping it here means the activity feed is one
    // table instead of two.
    type: varchar("type", { length: 12 }).notNull(),
    traderAddress: varchar("trader_address", { length: 42 }).notNull(),
    // For buy: srxIn (gross paid). For sell: srxOut (net received).
    // For graduated: srxLiquidity (raised total).
    srxAmount: numeric("srx_amount", { precision: 78, scale: 0 })
      .notNull()
      .default("0"),
    // For buy: tokensOut. For sell: tokensIn. For graduated: tokenLiquidity.
    tokenAmount: numeric("token_amount", { precision: 78, scale: 0 })
      .notNull()
      .default("0"),
    // Buy/Sell only; 0 for graduated.
    fee: numeric("fee", { precision: 78, scale: 0 }).notNull().default("0"),
    blockNumber: bigint("block_number", { mode: "bigint" }).notNull(),
    txHash: varchar("tx_hash", { length: 66 }).notNull(),
    logIndex: integer("log_index").notNull(),
  },
  (t) => ({
    // Idempotency key — re-running backfill must not double-write trades.
    uniqLog: uniqueIndex("cb_trades_uniq_log").on(t.txHash, t.logIndex),
    curveIdx: index("cb_trades_curve_idx").on(t.curveAddress),
    traderIdx: index("cb_trades_trader_idx").on(t.traderAddress),
    blockIdx: index("cb_trades_block_idx").on(t.blockNumber),
    // /coinblast/whales — ORDER BY srx_amount DESC, filtered to type !=
    // 'graduated'. B-tree on srx_amount alone is enough; type filter is
    // selective only when most trades aren't graduations, but during
    // active-trading periods that's the common case.
    srxAmountDescIdx: index("cb_trades_srx_amount_desc_idx").on(t.srxAmount),
  })
);
