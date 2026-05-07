ALTER TABLE "cb_tokens" ADD COLUMN "image_url" text;--> statement-breakpoint
ALTER TABLE "cb_tokens" ADD COLUMN "description" text;--> statement-breakpoint
ALTER TABLE "cb_tokens" ADD COLUMN "twitter_url" text;--> statement-breakpoint
ALTER TABLE "cb_tokens" ADD COLUMN "telegram_url" text;--> statement-breakpoint
ALTER TABLE "cb_tokens" ADD COLUMN "website_url" text;--> statement-breakpoint
ALTER TABLE "cb_tokens" ADD COLUMN "metadata_updated_at" bigint;