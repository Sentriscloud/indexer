# @sentriscloud/indexer

[![CI](https://github.com/Sentriscloud/indexer/actions/workflows/ci.yml/badge.svg)](https://github.com/Sentriscloud/indexer/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/Sentriscloud/indexer)](LICENSE)
[![Latest release](https://img.shields.io/github/v/release/Sentriscloud/indexer?include_prereleases&sort=semver)](https://github.com/Sentriscloud/indexer/releases/latest)


Postgres-backed REST indexer for Sentrix Chain. Sources every block, transaction,
and log from the public RPC + WebSocket endpoints, decodes ERC-20/721/1155 transfers,
tracks reorgs, and exposes the result as an Etherscan-API-compatible REST surface
plus Sentrix-native endpoints (validators, epochs, native token ops).

> **Status:** Live in production — backs the public REST + scan UI on both mainnet (chain 7119) and testnet (chain 7120). A Rust rewrite is in active development at [`Sentriscloud/indexer-rs`](https://github.com/Sentriscloud/indexer-rs); this TS implementation continues to serve until that one reaches dual-run parity + cutover.

## Architecture

```
Sentrix node (rpc.sentrixchain.com)
        │ JSON-RPC + WebSocket
        ▼
 ┌────────────────────────────────────┐
 │  apps/indexer (sync worker)        │
 │  - block fetcher (backfill + tail) │
 │  - log decoder (ERC-20/721/1155)   │
 │  - reorg detector                  │
 │  - reads tip, writes Postgres      │
 └────────────────────────────────────┘
                 │
                 ▼ packages/db (Drizzle)
            ┌────────────┐
            │ Postgres   │
            └────────────┘
                 │
                 ▼ packages/db
 ┌────────────────────────────────────┐
 │  apps/api (Fastify)                │
 │  - REST: /blocks, /tx, /address    │
 │  - Etherscan-compat: ?module=...   │
 │  - Native: /validators, /epochs    │
 │  - WS pass-through to RPC          │
 └────────────────────────────────────┘
                 │
                 ▼  consumed by sentriscloud/frontend/apps/scan
```

## Stack

- **Node 22 LTS + TypeScript 5**
- **Fastify 5** — REST API
- **Drizzle ORM + Postgres 16** — schema source-of-truth
- **viem ^2** — EVM RPC client
- **Pino** — structured logs
- **Turborepo + pnpm workspaces**

## Repo layout

```
.
├── apps/
│   ├── indexer/         # Sync worker
│   └── api/             # REST + WS server
├── packages/
│   ├── db/              # Drizzle schema + migrations + typed queries
│   └── chain/           # Sentrix RPC client (HTTP + WS) wrappers
├── docker-compose.yml
└── package.json
```

## Quickstart (local dev)

```bash
pnpm install
docker compose up -d postgres
pnpm db:generate && pnpm db:migrate
pnpm dev
```

API will come up on `:8081`, indexer worker on `:8082` (health check), Postgres on `:5432`.

## Endpoints — Phase 1

REST native:
- `GET /blocks?limit=&before=`
- `GET /blocks/:height`
- `GET /tx/:hash`
- `GET /address/:addr`
- `GET /address/:addr/txs`
- `GET /address/:addr/transfers`
- `GET /tokens`
- `GET /tokens/:address/holders`
- `GET /validators`
- `GET /epochs`
- `GET /health`

Etherscan-compatible (`/api`):
- `?module=account&action=txlist&address=...`
- `?module=account&action=balance&address=...`
- `?module=account&action=tokentx&address=...`
- `?module=stats&action=ethsupply` (alias `srxsupply`)
- `?module=stats&action=ethsupplyExt`
- `?module=stats&action=ethprice`

Native sentrix:
- `?module=native&action=validators`
- `?module=native&action=tokenomics`
- `?module=native&action=fork-status`

## License

MIT — see [LICENSE](./LICENSE).
