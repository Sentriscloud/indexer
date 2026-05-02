# Indexer worker — Node 22 LTS + pnpm 9.12, multistage so the runtime image
# only ships node_modules + source for this filter, not the whole monorepo.
# Pattern mirrors FastPoint apps/worker/Dockerfile (we're the same operator,
# same monorepo style); swapped to node:22-alpine + pnpm 9.12.0 to match
# this repo's `engines` + `packageManager` pinning.

# ── Stage 1: deps ────────────────────────────────────────────────────────
FROM node:22-alpine AS deps
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

# Workspace metadata first — these change rarely, so the install layer
# caches well across edits to actual source.
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json turbo.json tsconfig.base.json ./
COPY packages/db/package.json packages/db/
COPY packages/chain/package.json packages/chain/
COPY apps/indexer/package.json apps/indexer/

RUN pnpm install --frozen-lockfile --filter @sentriscloud/indexer-worker...

# ── Stage 2: source ──────────────────────────────────────────────────────
FROM node:22-alpine AS source
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

COPY --from=deps /app/ ./
COPY tsconfig.base.json ./
COPY packages/db/ packages/db/
COPY packages/chain/ packages/chain/
COPY apps/indexer/ apps/indexer/

# We don't `tsc` build here — packages/* expose ./src/*.ts via their
# package.json `exports`, so consumers (apps/indexer) import .ts source
# directly. Running tsx at the entrypoint keeps that path working without
# needing every package to re-emit a separate dist tree.

# ── Stage 3: runner ──────────────────────────────────────────────────────
FROM node:22-alpine AS runner
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

ENV NODE_ENV=production

RUN addgroup --system --gid 1001 indexergrp && \
    adduser --system --uid 1001 indexerusr

COPY --from=source --chown=indexerusr:indexergrp /app/ ./

USER indexerusr
WORKDIR /app/apps/indexer
EXPOSE 8082

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8082/health || exit 1

CMD ["npx", "tsx", "src/index.ts"]
