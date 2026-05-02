CREATE TABLE IF NOT EXISTS "cb_tokens" (
	"curve_address" varchar(42) PRIMARY KEY NOT NULL,
	"token_address" varchar(42) NOT NULL,
	"owner_address" varchar(42) NOT NULL,
	"name" text NOT NULL,
	"symbol" text NOT NULL,
	"curve_supply" numeric(78, 0) NOT NULL,
	"graduation_threshold" numeric(78, 0) NOT NULL,
	"is_graduated" boolean DEFAULT false NOT NULL,
	"created_block" bigint NOT NULL,
	"created_tx_hash" varchar(66) NOT NULL,
	"total_volume_srx" numeric(78, 0) DEFAULT '0' NOT NULL,
	"trade_count" integer DEFAULT 0 NOT NULL,
	"last_price_srx" numeric(78, 0) DEFAULT '0' NOT NULL,
	CONSTRAINT "cb_tokens_token_address_unique" UNIQUE("token_address")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "cb_trades" (
	"id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "cb_trades_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"curve_address" varchar(42) NOT NULL,
	"token_address" varchar(42),
	"type" varchar(12) NOT NULL,
	"trader_address" varchar(42) NOT NULL,
	"srx_amount" numeric(78, 0) DEFAULT '0' NOT NULL,
	"token_amount" numeric(78, 0) DEFAULT '0' NOT NULL,
	"fee" numeric(78, 0) DEFAULT '0' NOT NULL,
	"block_number" bigint NOT NULL,
	"tx_hash" varchar(66) NOT NULL,
	"log_index" integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_tokens_owner_idx" ON "cb_tokens" USING btree ("owner_address");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_tokens_graduated_idx" ON "cb_tokens" USING btree ("is_graduated");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_tokens_created_block_idx" ON "cb_tokens" USING btree ("created_block");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "cb_trades_uniq_log" ON "cb_trades" USING btree ("tx_hash","log_index");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_trades_curve_idx" ON "cb_trades" USING btree ("curve_address");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_trades_trader_idx" ON "cb_trades" USING btree ("trader_address");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_trades_block_idx" ON "cb_trades" USING btree ("block_number");