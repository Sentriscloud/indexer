# API server — same multistage pattern as apps/indexer/Dockerfile, just
# filtered to @sentriscloud/indexer-api. Health probe uses /health which
# the api server registers via routes/health.ts.

# ── Stage 1: deps ────────────────────────────────────────────────────────
FROM node:26-alpine AS deps
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

COPY pnpm-lock.yaml pnpm-workspace.yaml package.json turbo.json tsconfig.base.json ./
COPY packages/db/package.json packages/db/
COPY packages/chain/package.json packages/chain/
COPY apps/api/package.json apps/api/

RUN pnpm install --frozen-lockfile --filter @sentriscloud/indexer-api...

# ── Stage 2: source ──────────────────────────────────────────────────────
FROM node:26-alpine AS source
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

COPY --from=deps /app/ ./
COPY tsconfig.base.json ./
COPY packages/db/ packages/db/
COPY packages/chain/ packages/chain/
COPY apps/api/ apps/api/

# ── Stage 3: runner ──────────────────────────────────────────────────────
FROM node:26-alpine AS runner
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app

ENV NODE_ENV=production

RUN addgroup --system --gid 1001 apigrp && \
    adduser --system --uid 1001 apiusr

COPY --from=source --chown=apiusr:apigrp /app/ ./

USER apiusr
WORKDIR /app/apps/api
EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8081/health || exit 1

CMD ["npx", "tsx", "src/index.ts"]
