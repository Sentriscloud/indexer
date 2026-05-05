// Lazy is_contract detection. The hot tx-insertion path in sync.ts upserts
// addresses with `is_contract=false` + `code_hash=NULL` because doing an
// `eth_getCode` per address mid-batch would dominate runtime. This worker
// runs in the background, picks up addresses with `code_hash IS NULL`, and
// flips the flag based on whether the chain reports any deployed code.
//
// Cadence is intentionally slow (4s between scans, 10 addresses per scan)
// so a fresh testnet boot doesn't fire 1000+ getCode calls in one second
// and trip the public-RPC rate limit. The pace catches up to real-world
// contract-deploy traffic comfortably (~150 contracts/min capacity).

import { eq, isNull } from "drizzle-orm";
import type { Logger } from "pino";
import { keccak256 } from "viem";

import {
  type DbClient,
  addresses as addressesTable,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

interface DetectorArgs {
  db: DbClient;
  chain: SentrixClient;
  log: Logger;
}

const SCAN_INTERVAL_MS = Number(process.env.INDEXER_CONTRACT_DETECT_INTERVAL_MS ?? 4_000);
const SCAN_BATCH = Number(process.env.INDEXER_CONTRACT_DETECT_BATCH ?? 10);
const NO_CODE_SENTINEL = "0x"; // matches what RPC returns for EOAs

export function startContractDetector(args: DetectorArgs): () => void {
  const { db, chain, log } = args;
  let stopped = false;

  const tick = async () => {
    if (stopped) return;
    try {
      const candidates = await db
        .select({ address: addressesTable.address })
        .from(addressesTable)
        .where(isNull(addressesTable.codeHash))
        .limit(SCAN_BATCH);

      if (candidates.length === 0) return;

      for (const { address } of candidates) {
        if (stopped) return;
        try {
          const code = await chain.getCode(address as `0x${string}`);
          const isContract = code !== NO_CODE_SENTINEL;
          // Sentinel for "checked, no code" so we never re-probe an EOA.
          // Real contract code_hash uses keccak256 to match how the chain
          // computes it on EVM-side state; the column is informational
          // here, not a consensus value, so the cost-of-mismatch is zero.
          const codeHash = isContract ? keccak256(code) : NO_CODE_SENTINEL;
          await db
            .update(addressesTable)
            .set({ isContract, codeHash })
            .where(eq(addressesTable.address, address));
          if (isContract) {
            log.debug({ address }, "marked as contract");
          }
        } catch (err) {
          // Single-address failure shouldn't block the rest of the batch.
          // Most likely cause: transient 502 from the public RPC, fixed on
          // next tick when we'll re-pick this address up (code_hash still NULL).
          log.warn({ address, err: String(err) }, "getCode failed; will retry");
        }
      }
    } catch (err) {
      log.error({ err: String(err) }, "contract-detect tick failed");
    }
  };

  const interval = setInterval(() => {
    void tick();
  }, SCAN_INTERVAL_MS);

  log.info(
    { intervalMs: SCAN_INTERVAL_MS, batchSize: SCAN_BATCH },
    "contract-detect worker started",
  );

  return () => {
    stopped = true;
    clearInterval(interval);
  };
}
