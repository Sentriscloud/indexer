CREATE TABLE IF NOT EXISTS "addresses" (
	"address" varchar(42) PRIMARY KEY NOT NULL,
	"first_seen_block" bigint NOT NULL,
	"last_seen_block" bigint NOT NULL,
	"balance_cached" numeric(78, 0) DEFAULT '0',
	"nonce" bigint DEFAULT 0,
	"is_contract" boolean DEFAULT false NOT NULL,
	"code_hash" varchar(66)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "blocks" (
	"height" bigint PRIMARY KEY NOT NULL,
	"hash" varchar(66) NOT NULL,
	"parent_hash" varchar(66) NOT NULL,
	"timestamp" bigint NOT NULL,
	"validator" varchar(42) NOT NULL,
	"gas_used" bigint DEFAULT 0 NOT NULL,
	"gas_limit" bigint DEFAULT 0 NOT NULL,
	"base_fee" numeric(78, 0),
	"tx_count" integer DEFAULT 0 NOT NULL,
	"state_root" varchar(66),
	"round" integer DEFAULT 0 NOT NULL,
	"justification_signers" jsonb DEFAULT '[]'::jsonb,
	CONSTRAINT "blocks_hash_unique" UNIQUE("hash")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "epochs" (
	"epoch_number" bigint PRIMARY KEY NOT NULL,
	"start_height" bigint NOT NULL,
	"end_height" bigint NOT NULL,
	"validator_set" jsonb NOT NULL,
	"total_staked" numeric(78, 0) DEFAULT '0',
	"total_blocks_produced" bigint DEFAULT 0
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "logs" (
	"block_height" bigint NOT NULL,
	"tx_hash" varchar(66) NOT NULL,
	"log_index" integer NOT NULL,
	"address" varchar(42) NOT NULL,
	"topic0" varchar(66),
	"topic1" varchar(66),
	"topic2" varchar(66),
	"topic3" varchar(66),
	"data" text,
	CONSTRAINT "logs_block_height_log_index_pk" PRIMARY KEY("block_height","log_index")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "_meta" (
	"key" varchar(64) PRIMARY KEY NOT NULL,
	"value" text NOT NULL,
	"updated_at" bigint NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "token_transfers" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "token_transfers_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"block_height" bigint NOT NULL,
	"tx_hash" varchar(66) NOT NULL,
	"log_index" integer NOT NULL,
	"contract" varchar(42) NOT NULL,
	"standard" varchar(12) NOT NULL,
	"from_addr" varchar(42) NOT NULL,
	"to_addr" varchar(42) NOT NULL,
	"token_id" numeric(78, 0),
	"amount" numeric(78, 0) NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "transactions" (
	"hash" varchar(66) PRIMARY KEY NOT NULL,
	"block_height" bigint NOT NULL,
	"tx_index" integer NOT NULL,
	"from_addr" varchar(42) NOT NULL,
	"to_addr" varchar(42),
	"value" numeric(78, 0) DEFAULT '0' NOT NULL,
	"gas_limit" bigint DEFAULT 0 NOT NULL,
	"gas_used" bigint DEFAULT 0,
	"gas_price" numeric(78, 0),
	"fee" numeric(78, 0) DEFAULT '0' NOT NULL,
	"nonce" bigint DEFAULT 0 NOT NULL,
	"data" text,
	"status" smallint DEFAULT 1 NOT NULL,
	"contract_address" varchar(42),
	"tx_type" varchar(24) DEFAULT 'native' NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "validators" (
	"address" varchar(42) PRIMARY KEY NOT NULL,
	"moniker" varchar(64),
	"commission_bp" integer,
	"self_stake" numeric(78, 0) DEFAULT '0',
	"total_delegated" numeric(78, 0) DEFAULT '0',
	"blocks_proposed" bigint DEFAULT 0,
	"last_active_block" bigint,
	"is_jailed" boolean DEFAULT false NOT NULL,
	"jail_until" bigint
);
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "logs" ADD CONSTRAINT "logs_block_height_blocks_height_fk" FOREIGN KEY ("block_height") REFERENCES "public"."blocks"("height") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "logs" ADD CONSTRAINT "logs_tx_hash_transactions_hash_fk" FOREIGN KEY ("tx_hash") REFERENCES "public"."transactions"("hash") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "transactions" ADD CONSTRAINT "transactions_block_height_blocks_height_fk" FOREIGN KEY ("block_height") REFERENCES "public"."blocks"("height") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "blocks_validator_idx" ON "blocks" USING btree ("validator");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "blocks_timestamp_idx" ON "blocks" USING btree ("timestamp");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_address_idx" ON "logs" USING btree ("address");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_topic0_idx" ON "logs" USING btree ("topic0");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_tx_idx" ON "logs" USING btree ("tx_hash");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "transfers_contract_idx" ON "token_transfers" USING btree ("contract");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "transfers_from_idx" ON "token_transfers" USING btree ("from_addr");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "transfers_to_idx" ON "token_transfers" USING btree ("to_addr");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "transfers_block_idx" ON "token_transfers" USING btree ("block_height");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_block_height_idx" ON "transactions" USING btree ("block_height");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_from_idx" ON "transactions" USING btree ("from_addr");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_to_idx" ON "transactions" USING btree ("to_addr");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_contract_idx" ON "transactions" USING btree ("contract_address");