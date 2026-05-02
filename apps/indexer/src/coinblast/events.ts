// CoinBlast event ABIs + topic0 hashes + per-network deploy info.
//
// Used by the CoinBlast worker (sibling worker.ts) to filter logs by topic +
// decode them. Kept ABI-only here so the worker can stay focused on flow.
//
// Topic0 values are computed at module-load via keccak256(eventSignature) so
// they always match the ABI — if we rename a parameter the topic doesn't
// silently drift. The ABI shapes mirror canonical-contracts/contracts/
// CoinBlastFactory.sol + CoinBlastCurve.sol.

import { keccak256, toBytes } from "viem";

export const COINBLAST_FACTORY_ADDRESS: Record<"mainnet" | "testnet", `0x${string}`> = {
  mainnet: "0xc9D7a61D7C2F428F6A055916488041fD00532110",
  testnet: "0xc7FBd67fb809b189998cB27F1857b50A3e09619c",
};

// First block at which the factory could possibly emit. Used as the floor of
// the backfill scan so we don't waste 1.1M getLogs round-trips on empty
// pre-deploy history.
export const COINBLAST_DEPLOY_BLOCK: Record<"mainnet" | "testnet", bigint> = {
  mainnet: 1_178_667n,
  testnet: 1_637_883n,
};

// ── Event ABIs ─────────────────────────────────────────────────────────────

export const factoryEvents = [
  {
    type: "event",
    name: "CurveCreated",
    inputs: [
      { indexed: true, name: "curve", type: "address" },
      { indexed: true, name: "token", type: "address" },
      { indexed: true, name: "owner", type: "address" },
      { indexed: false, name: "name", type: "string" },
      { indexed: false, name: "symbol", type: "string" },
      { indexed: false, name: "curveSupply", type: "uint256" },
      { indexed: false, name: "graduationSrxThreshold", type: "uint256" },
    ],
  },
] as const;

export const curveEvents = [
  {
    type: "event",
    name: "Buy",
    inputs: [
      { indexed: true, name: "buyer", type: "address" },
      { indexed: false, name: "srxIn", type: "uint256" },
      { indexed: false, name: "fee", type: "uint256" },
      { indexed: false, name: "tokensOut", type: "uint256" },
    ],
  },
  {
    type: "event",
    name: "Sell",
    inputs: [
      { indexed: true, name: "seller", type: "address" },
      { indexed: false, name: "tokensIn", type: "uint256" },
      { indexed: false, name: "fee", type: "uint256" },
      { indexed: false, name: "srxOut", type: "uint256" },
    ],
  },
  {
    type: "event",
    name: "Graduated",
    inputs: [
      { indexed: true, name: "pair", type: "address" },
      { indexed: false, name: "srxLiquidity", type: "uint256" },
      { indexed: false, name: "tokenLiquidity", type: "uint256" },
      { indexed: false, name: "lpBurned", type: "uint256" },
    ],
  },
] as const;

// ── topic0 hashes ──────────────────────────────────────────────────────────
// keccak256(canonical signature). Canonical signature for CurveCreated is
// "CurveCreated(address,address,address,string,string,uint256,uint256)" —
// indexed flags are NOT part of the hash, only the type list.

function topic0(signature: string): `0x${string}` {
  return keccak256(toBytes(signature));
}

export const TOPIC_CURVE_CREATED = topic0(
  "CurveCreated(address,address,address,string,string,uint256,uint256)",
);
export const TOPIC_BUY = topic0("Buy(address,uint256,uint256,uint256)");
export const TOPIC_SELL = topic0("Sell(address,uint256,uint256,uint256)");
export const TOPIC_GRADUATED = topic0(
  "Graduated(address,uint256,uint256,uint256)",
);
