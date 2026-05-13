#!/usr/bin/env bash
# Sync source + rebuild + recreate the mainnet indexer stack on the mainnet host.
#
# The mainnet host's ~/indexer-deploy is NOT a git checkout (operator
# preference; keeps the prod host out of the GitHub auth path). Source
# files are pushed via scp instead of `git pull`. This script captures
# the dance so we don't have to re-derive the file list on every PR.
#
# Usage:
#   ./scripts/deploy-mainnet.sh                      # full sync + rebuild
#   ./scripts/deploy-mainnet.sh --check              # smoke /health, skip changes
#   ./scripts/deploy-mainnet.sh --skip-build         # scp only, no rebuild
#
# Env:
#   MAINNET_HOST  ssh alias for the mainnet host (required)
#
# Pre-flight:
#   - Refuses if local working tree is dirty
#   - Refuses if main branch isn't up-to-date with origin
#   - Local typecheck must pass
#
# What it ships (post tonight's audit pass):
#   apps/api/{package.json,src/index.ts,src/routes/*.ts}
#   apps/indexer/src/{index.ts,sync.ts,coinblast/worker.ts}
#   packages/chain/src/index.ts
#   packages/db/{src/schema.ts,drizzle/0003_*.sql,drizzle/meta/*}
#   pnpm-lock.yaml + README.md + both docker-compose files

set -euo pipefail

HOST="${MAINNET_HOST:?MAINNET_HOST must be set (ssh alias for the mainnet host)}"
SKIP_BUILD=0
CHECK_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=1 ;;
    --check)      CHECK_ONLY=1 ;;
    -h|--help)
      sed -n '2,28p' "$0" | sed 's/^# \?//'
      exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if [ "$CHECK_ONLY" = 1 ]; then
  echo "==> mainnet /health"
  ssh "$HOST" 'curl -s -m 5 -o /dev/null -w "%{http_code} (%{time_total}s)\n" http://127.0.0.1:8081/health'
  ssh "$HOST" 'docker ps --filter name=sentrix-indexer --format "{{.Names}} {{.Status}}"'
  exit 0
fi

echo "==> pre-flight"
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "✗ working tree is dirty — commit or stash first" >&2
  exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo "✗ not on main (on $CURRENT_BRANCH)" >&2
  exit 1
fi

git fetch origin main --quiet
LOCAL="$(git rev-parse main)"
REMOTE="$(git rev-parse origin/main)"
if [ "$LOCAL" != "$REMOTE" ]; then
  echo "✗ local main is not up-to-date with origin/main" >&2
  echo "  local:  $LOCAL"
  echo "  remote: $REMOTE"
  exit 1
fi

echo "==> typecheck"
( cd apps/api      && npx tsc --noEmit ) >/dev/null
( cd apps/indexer  && npx tsc --noEmit ) >/dev/null
( cd packages/chain && npx tsc --noEmit ) >/dev/null
echo "✓ typecheck clean"

# Files to ship. Keep this list explicit + reviewed; avoid blanket `scp -r`
# that would push internal artifacts or local node_modules.
FILES=(
  apps/api/package.json
  apps/api/src/index.ts
  apps/api/src/routes/coinblast.ts
  apps/api/src/routes/etherscan.ts
  apps/api/src/routes/health.ts
  apps/api/src/routes/native.ts
  apps/indexer/src/coinblast/worker.ts
  apps/indexer/src/index.ts
  apps/indexer/src/sync.ts
  packages/chain/src/index.ts
  packages/db/package.json
  packages/db/src/migrate.ts
  packages/db/src/schema.ts
  packages/db/drizzle/0003_youthful_prima.sql
  packages/db/drizzle/meta/_journal.json
  packages/db/drizzle/meta/0003_snapshot.json
  pnpm-lock.yaml
  README.md
  docker-compose.yml
  docker-compose.testnet.yml
)

echo "==> scp ${#FILES[@]} files to $HOST"
for f in "${FILES[@]}"; do
  if [ ! -f "$ROOT/$f" ]; then
    echo "✗ missing local: $f" >&2
    exit 1
  fi
  ssh "$HOST" "mkdir -p \"\$HOME/indexer-deploy/$(dirname "$f")\""
  scp -q "$ROOT/$f" "$HOST:~/indexer-deploy/$f"
done
echo "✓ source synced"

if [ "$SKIP_BUILD" = 1 ]; then
  echo "==> --skip-build set; not rebuilding"
  exit 0
fi

echo "==> one-time cleanup: stop dead redis container if it's still around"
# PR #23 dropped the redis service from compose, but the container may
# already be running on a host that was provisioned earlier.
ssh "$HOST" 'docker stop sentrix-indexer-redis 2>/dev/null || true; docker rm sentrix-indexer-redis 2>/dev/null || true'

echo "==> rebuild api + indexer images"
# Migrations run inside the indexer container at boot now (PR #28). No
# separate `pnpm db:migrate` step needed; the embedded runner is
# idempotent (~10 ms no-op when schema is current).
ssh "$HOST" 'cd ~/indexer-deploy && docker compose -f docker-compose.yml build api indexer' 2>&1 | tail -3

echo "==> recreate containers"
ssh "$HOST" 'cd ~/indexer-deploy && docker compose -f docker-compose.yml up -d --force-recreate api indexer' 2>&1 | tail -3

echo "==> wait for healthy + smoke"
sleep 12
ssh "$HOST" 'docker ps --filter name=sentrix-indexer --format "{{.Names}} {{.Status}}"'
ssh "$HOST" 'curl -s -m 5 -o /dev/null -w "/health → %{http_code} (%{time_total}s)\n" http://127.0.0.1:8081/health'
ssh "$HOST" 'curl -s -m 5 -w "\n/blocks?before=abc → %{http_code}\n" http://127.0.0.1:8081/blocks?before=abc'

echo ""
echo "✓ mainnet deploy complete"
