CREATE INDEX IF NOT EXISTS "addresses_contract_recent_idx" ON "addresses" USING btree ("is_contract","first_seen_block");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "cb_trades_srx_amount_desc_idx" ON "cb_trades" USING btree ("srx_amount");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "txs_value_desc_idx" ON "transactions" USING btree ("value");