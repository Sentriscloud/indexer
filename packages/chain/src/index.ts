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
}

export class SentrixClient {
  readonly http: PublicClient;
  readonly ws: PublicClient;

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
  }

  async getBlockNumber(): Promise<bigint> {
    return retry429(() => this.http.getBlockNumber());
  }

  /** Full block w/ all transactions. */
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
