// Declarative event-handler registry. Pre-Tier-3 sync.ts hard-coded the
// dispatch:
//
//   if (t0 === ERC20_TRANSFER && length === 3) { … erc-20 row … }
//   else if (t0 === ERC20_TRANSFER && length === 4) { … erc-721 row … }
//   else if (t0 === ERC1155_SINGLE) { … }
//
// Adding a new event type — DEX swap, NFT mint, custom protocol log —
// meant editing sync.ts in the middle of its hot loop and growing an
// already-busy if/else. The registry pattern lifts the dispatch into a
// table the loop just iterates: each handler declares the topic0 it
// owns and a pure decoder that returns the row to insert (or null if
// the log shape is recognised but should be skipped, eg ERC-1155 batch
// transfers we defer materialising).
//
// Same wire-format / behaviour as the previous hardcoded path — this is
// purely a refactor. Adding a handler is now `register(myHandler)` from
// a new file under apps/indexer/src/handlers/, no sync.ts edits.

import type { Log as ViemLog } from "viem";

import type { tokenTransfers } from "@sentriscloud/indexer-db";

export type TransferRow = typeof tokenTransfers.$inferInsert;

/** Chain-shape log we hand to a handler. Same field set sync.ts already
 * produces from `chain.getLogsRange()` plus the lowercased + per-block
 * fields the handler needs to build a row. */
export interface DecodedLogContext {
  /** Log itself, viem shape. Topics may be undefined past the indexed
   * count; handlers should validate the length they expect. */
  log: ViemLog;
  /** Lowercased contract address — already normalised so handlers don't
   * each repeat the toLowerCase. */
  contract: string;
  /** Lowercased tx hash. */
  txHash: string;
}

/** Handler contract: declare which topic0 you own + how to decode it. */
export interface EventHandler {
  /** Owning topic0 (lowercased hex string with 0x prefix). The registry
   * keys handlers by this — at most one handler per topic0 today.
   * Multiple-handler-per-topic-0 (eg disambiguating ERC-20 vs ERC-721
   * Transfer by topic count) is encoded inside the decode function via
   * a length check + null return. */
  topic0: string;
  /** Pure decoder. Returns the transfer row to insert, or null if the
   * log matched topic0 but the handler chose to skip it (wrong arity,
   * deferred decode, malformed data). Throwing is reserved for genuine
   * bugs — caller surfaces them via log.error so an operator can grep. */
  decode: (ctx: DecodedLogContext) => TransferRow | null;
}

const REGISTRY = new Map<string, EventHandler[]>();

/** Register a handler. Multiple handlers can share a topic0 — the
 * dispatcher walks them in registration order and keeps the first
 * non-null result. Useful for the ERC-20 / ERC-721 split where both
 * declare the same Transfer topic but disambiguate via topic count. */
export function register(handler: EventHandler) {
  const existing = REGISTRY.get(handler.topic0);
  if (existing) {
    existing.push(handler);
  } else {
    REGISTRY.set(handler.topic0, [handler]);
  }
}

/** Run every handler that matched the log's topic0; return the first
 * non-null decoded row, or null if no handler claimed it. */
export function dispatch(ctx: DecodedLogContext): TransferRow | null {
  const t0 = ctx.log.topics[0]?.toLowerCase();
  if (!t0) return null;
  const handlers = REGISTRY.get(t0);
  if (!handlers) return null;
  for (const h of handlers) {
    const row = h.decode(ctx);
    if (row !== null) return row;
  }
  return null;
}

/** Reset between tests — never called from the worker. */
export function _reset() {
  REGISTRY.clear();
}

/** Helper: 32-byte right-padded address topic → 0x-prefixed lowercased
 * 20-byte address. */
export function topicToAddress(topic: string): string {
  return "0x" + topic.slice(-40).toLowerCase();
}
