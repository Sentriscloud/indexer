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
    const httpUrl =
      cfg.httpUrl ?? chain.rpcUrls.default.http[0];
    const wsUrl =
      cfg.wsUrl ?? chain.rpcUrls.default.webSocket?.[0];

    this.http = createPublicClient({ chain, transport: http(httpUrl) });
    this.ws = wsUrl
      ? createPublicClient({ chain, transport: webSocket(wsUrl) })
      : this.http;
  }

  async getBlockNumber(): Promise<bigint> {
    return this.http.getBlockNumber();
  }

  /** Full block w/ all transactions. */
  async getBlock(height: bigint): Promise<Block<bigint, true>> {
    return this.http.getBlock({ blockNumber: height, includeTransactions: true });
  }

  async getLogsRange(fromBlock: bigint, toBlock: bigint): Promise<GetLogsReturnType> {
    return this.http.getLogs({ fromBlock, toBlock });
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
