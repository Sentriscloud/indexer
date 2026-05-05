// Sentrix RPC client — thin wrapper around viem's public client + WS transport.
// Exposes the typed surface the indexer worker actually uses (block fetch +
// log fetch + newHeads subscription) so consumers don't have to think about
// viem-specific decoding.
//
// Not generic across chains: the chain id + native decimals are baked in.
// If we ever run an indexer for chain 7120 testnet, we instantiate this twice.

import {
  createPublicClient,
  defineChain,
  http,
  webSocket,
  type Block,
  type GetLogsReturnType,
  type Log,
  type PublicClient,
} from "viem";

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

// Resolve the bundled .proto file relative to this source file. The Docker
// image copies packages/chain/proto/sentrix.proto alongside the .ts source
// (mirrored exactly from the chain repo's crates/sentrix-grpc/proto/), so
// runtime path resolution works identically inside the container and during
// `tsx watch` on the host.
const PROTO_PATH = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../proto/sentrix.proto",
);
const protoPkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const sentrixProto = grpc.loadPackageDefinition(protoPkgDef) as any;

/// Wrap any RPC call in retry-with-backoff on HTTP 429 (rate limit).
/// The validator's per-IP rate limit (SENTRIX_WRITE_RATE_LIMIT) is set
/// low enough that a hot backfill loop trips it within seconds. Catching
/// 429 + sleeping + retrying keeps the indexer alive without operator
/// intervention on the validator side.
async function retry429<T>(fn: () => Promise<T>, attempts = 6): Promise<T> {
  let lastErr: unknown;
  let delayMs = 500;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const msg = String((err as Error)?.message ?? "");
      if (!msg.includes("Status: 429") && !msg.includes("rate limit")) throw err;
      // Exponential backoff: 0.5s → 1s → 2s → 4s → 8s → 16s.
      await new Promise((r) => setTimeout(r, delayMs));
      delayMs = Math.min(delayMs * 2, 16_000);
    }
  }
  throw lastErr;
}

const SENTRIX_MAINNET = defineChain({
  id: 7119,
  name: "Sentrix",
  nativeCurrency: { name: "SRX", symbol: "SRX", decimals: 18 },
  rpcUrls: {
    default: {
      http: ["https://rpc.sentrixchain.com"],
      webSocket: ["wss://rpc.sentrixchain.com/ws"],
    },
  },
});

const SENTRIX_TESTNET = defineChain({
  id: 7120,
  name: "Sentrix Testnet",
  nativeCurrency: { name: "tSRX", symbol: "tSRX", decimals: 18 },
  rpcUrls: {
    default: {
      http: ["https://testnet-rpc.sentrixchain.com"],
      webSocket: ["wss://testnet-rpc.sentrixchain.com/ws"],
    },
  },
});

export interface SentrixClientConfig {
  network: "mainnet" | "testnet";
  httpUrl?: string;
  wsUrl?: string;
  /** gRPC endpoint (host:443). Defaults to grpc.sentrixchain.com / grpc-testnet. */
  grpcUrl?: string;
}

interface GrpcBlockHeader {
  index: bigint;
  hash: string;
}

/**
 * Tip watcher backed by the side-car gRPC `GetBlock {latest:true}`. Called
 * once per `intervalMs`; emits the new tip height whenever it advances.
 *
 * Why polling instead of streaming: the chain side-car ships GetBlock +
 * GetBalance in v0.2 — server-streaming `StreamEvents` returns Unimplemented
 * until v0.3. Polling at ~200 ms is a clean intermediate: same throughput as
 * the prior viem WS subscription with much less log noise (no 4 s viem
 * fallback poll, no 429 retry stack), and switching to push later is a
 * one-line method-name swap once StreamEvents is wired.
 */
export interface TipWatcher {
  stop: () => void;
}

/** Push-based block subscription (gRPC server-streaming, v2.1.71+). */
export interface BlockStreamSub {
  stop: () => void;
}

export class SentrixClient {
  readonly http: PublicClient;
  readonly ws: PublicClient;
  readonly grpcUrl: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private grpcStub: any | null = null;

  constructor(cfg: SentrixClientConfig) {
    const chain = cfg.network === "mainnet" ? SENTRIX_MAINNET : SENTRIX_TESTNET;
    // Operator overrides via env, falling back to caller cfg, falling back
    // to the public defaults. The wg1 / loopback path is what saves the
    // backfill from the public RPC's per-IP rate limit — running on the
    // build host we point at a validator's :8545/rpc directly.
    const httpUrl =
      process.env.INDEXER_RPC_HTTP_URL ??
      cfg.httpUrl ??
      chain.rpcUrls.default.http[0];
    const wsUrl =
      process.env.INDEXER_RPC_WS_URL ??
      cfg.wsUrl ??
      chain.rpcUrls.default.webSocket?.[0];

    this.http = createPublicClient({ chain, transport: http(httpUrl) });
    this.ws = wsUrl
      ? createPublicClient({ chain, transport: webSocket(wsUrl) })
      : this.http;

    this.grpcUrl =
      process.env.INDEXER_GRPC_URL ??
      cfg.grpcUrl ??
      (cfg.network === "mainnet"
        ? "grpc.sentrixchain.com:443"
        : "grpc-testnet.sentrixchain.com:443");
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private getGrpcStub(): any {
    if (this.grpcStub) return this.grpcStub;
    // SSL credentials by default (we always go through the public TLS edge).
    // Set INDEXER_GRPC_INSECURE=1 only for local plaintext side-car testing.
    const creds =
      process.env.INDEXER_GRPC_INSECURE === "1"
        ? grpc.credentials.createInsecure()
        : grpc.credentials.createSsl();
    this.grpcStub = new sentrixProto.sentrix.v1.Sentrix(this.grpcUrl, creds);
    return this.grpcStub;
  }

  /**
   * Fetch the latest block header via gRPC. Cheap (no tx body in v0.2).
   * Used by the tip watcher to detect head advance with minimal payload.
   */
  async getLatestHeaderGrpc(): Promise<GrpcBlockHeader> {
    return new Promise((resolve, reject) => {
      this.getGrpcStub().GetBlock(
        { latest: true },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (err: Error | null, resp: any) => {
          if (err) return reject(err);
          // proto-loader returns `index` as string when longs:String. Hash is
          // { value: Buffer }.
          const idx = BigInt(resp.index);
          const hash = Buffer.from(resp.hash?.value ?? new Uint8Array()).toString("hex");
          resolve({ index: idx, hash });
        },
      );
    });
  }

  /**
   * Poll-based tip watcher. Calls `onTip(height)` exactly once per height
   * advance. Internally polls GetBlock at `intervalMs` (default 200 ms) and
   * deduplicates against the last seen height.
   *
   * On gRPC errors, increments a per-instance retry delay (capped) and
   * retries — no zombie processes, no unbounded log spam. The caller's
   * `onError` callback (if provided) sees the error so it can be surfaced
   * in the dashboard log; otherwise we just log.warn it via `onError = null`.
   */
  /**
   * Push-based block subscription via gRPC server-streaming (v2.1.71+).
   * Subscribes to `sentrix.v1.Sentrix/StreamEvents` and invokes `onBlock`
   * once per `BlockFinalized` ChainEvent. The stream sits on the server's
   * EventBus broadcast channel — same source the JSON-RPC `eth_subscribe`
   * WebSocket handlers consume — so ordering / Lagged semantics match.
   *
   * Resilience: on stream end (network blip, server restart, broadcast
   * Closed), re-subscribes with exponential backoff (500 ms → 8 s cap).
   * No zombie processes; `stop()` cancels both the active call and any
   * pending reconnect timer.
   *
   * On a `Lagged` sentinel from the server (consumer fell behind 1024+
   * events), the callback fires with `kind: "lagged"` so the indexer can
   * resync via JSON-RPC backfill instead of silently missing blocks.
   */
  streamBlocks(
    onBlock: (
      ev:
        | { kind: "block"; height: bigint; hash: string; latencyMs: number }
        | { kind: "lagged"; skipped: bigint },
    ) => void,
    opts: { onError?: (err: unknown) => void; onReconnect?: (attempt: number) => void } = {},
  ): BlockStreamSub {
    let stopped = false;
    let backoffMs = 500;
    let attempt = 0;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let activeCall: any = null;
    let pendingTimer: NodeJS.Timeout | null = null;

    const subscribe = () => {
      if (stopped) return;
      attempt++;
      if (attempt > 1 && opts.onReconnect) opts.onReconnect(attempt);
      const tStart = Date.now();
      // Empty StreamEventsRequest = subscribe to all events from "now"
      // (server v0.3 always returns BlockFinalized + Lagged sentinels;
      // filter / from_sequence are server-side ignored until v0.4).
      activeCall = this.getGrpcStub().StreamEvents({});

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      activeCall.on("data", (msg: any) => {
        // Reset backoff on first successful frame after a reconnect.
        backoffMs = 500;
        const latencyMs = Date.now() - tStart;
        if (msg.block_finalized?.block) {
          const b = msg.block_finalized.block;
          onBlock({
            kind: "block",
            height: BigInt(b.index),
            hash: Buffer.from(b.hash?.value ?? new Uint8Array()).toString("hex"),
            latencyMs,
          });
        } else if (msg.lagged) {
          onBlock({
            kind: "lagged",
            skipped: BigInt(msg.lagged.skipped_count ?? 0),
          });
        }
      });

      activeCall.on("error", (err: Error) => {
        if (opts.onError) opts.onError(err);
        scheduleReconnect();
      });

      activeCall.on("end", () => {
        // Server closed (process restart, broadcast Closed) — reconnect.
        scheduleReconnect();
      });
    };

    const scheduleReconnect = () => {
      if (stopped) return;
      if (pendingTimer) return;
      const delay = backoffMs;
      backoffMs = Math.min(backoffMs * 2, 8000);
      pendingTimer = setTimeout(() => {
        pendingTimer = null;
        subscribe();
      }, delay);
    };

    subscribe();

    return {
      stop: () => {
        stopped = true;
        if (pendingTimer) clearTimeout(pendingTimer);
        if (activeCall) {
          try {
            activeCall.cancel();
          } catch {
            /* ignore */
          }
        }
        if (this.grpcStub) {
          try {
            this.grpcStub.close();
          } catch {
            /* ignore */
          }
        }
      },
    };
  }

  watchTipGrpc(
    onTip: (height: bigint, latencyMs: number) => void,
    opts: { intervalMs?: number; onError?: (err: unknown) => void } = {},
  ): TipWatcher {
    const intervalMs = opts.intervalMs ?? 200;
    let stopped = false;
    let lastHeight: bigint | null = null;
    let backoffMs = intervalMs;

    const tick = async () => {
      if (stopped) return;
      const t0 = Date.now();
      try {
        const head = await this.getLatestHeaderGrpc();
        const latency = Date.now() - t0;
        backoffMs = intervalMs;
        if (lastHeight === null || head.index > lastHeight) {
          lastHeight = head.index;
          onTip(head.index, latency);
        }
      } catch (err) {
        // Exponential backoff on stream/connection errors. Cap at 8 s so
        // a flapping endpoint doesn't permanently silence the indexer.
        backoffMs = Math.min(backoffMs * 2, 8000);
        if (opts.onError) opts.onError(err);
      }
      if (!stopped) setTimeout(tick, backoffMs);
    };
    tick();

    return {
      stop: () => {
        stopped = true;
        if (this.grpcStub) {
          try {
            this.grpcStub.close();
          } catch {
            /* ignore */
          }
        }
      },
    };
  }

  async getBlockNumber(): Promise<bigint> {
    return retry429(() => this.http.getBlockNumber());
  }

  /** Full block — but the transactions array may carry hash-only entries.
   *
   * Sentrix's `eth_getBlockByNumber` doesn't honor `includeTransactions=true`
   * (always returns hashes), and `eth_getTransactionByHash` returns the
   * native `{transaction: {amount, from_address, …}}` shape instead of the
   * EVM-spec `{blockHash, from, to, value, gas, …}` shape that viem expects.
   * Viem throws on the unknown response, so we don't try to fill the array
   * with full tx objects on the indexer side — sync.ts already skips
   * string entries (`typeof t === "string" → continue`), so the blocks
   * table populates fine and the transactions table stays empty until
   * the chain RPC is brought into spec OR the indexer reads tx via the
   * REST `/transactions/<hash>` shape with a native-format adapter. See
   * Sentriscloud/indexer issue tracker.
   */
  async getBlock(height: bigint): Promise<Block<bigint, true>> {
    return retry429(() =>
      this.http.getBlock({ blockNumber: height, includeTransactions: true }),
    );
  }

  async getLogsRange(fromBlock: bigint, toBlock: bigint): Promise<GetLogsReturnType> {
    return retry429(() => this.http.getLogs({ fromBlock, toBlock }));
  }

  /**
   * Subscribe to new heads. Each event delivers the next block's header —
   * the indexer should refetch the block by number to get full tx + log data.
   */
  watchBlocks(onBlock: (height: bigint) => void) {
    return this.ws.watchBlockNumber({
      onBlockNumber: (n) => onBlock(n),
      emitOnBegin: false,
    });
  }
}

export type { Block, Log };
