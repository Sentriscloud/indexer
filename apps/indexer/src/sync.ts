// Block-level sync. Both backfill and tail funnel through `indexBlock`.
//
// Phase 1 scope:
//   - Persist block + transactions + logs.
//   - Decode ERC-20/721/1155 Transfer logs into token_transfers.
//   - Update _meta.last_synced_height in the same SQL transaction.
//
// Out of Phase 1 scope (will land later):
//   - Reorg detection beyond a single-block parentHash check.
//   - Address balance materialisation.
//   - Validator / epoch denormalisation (those come from the native REST
//     endpoints, not from EVM logs, so they live in a separate worker).

import { eq, sql } from "drizzle-orm";
import type { Logger } from "pino";

import {
  type DbClient,
  addresses as addressesTable,
  blocks as blocksTable,
  logs as logsTable,
  meta,
  tokenTransfers,
  transactions as txsTable,
} from "@sentriscloud/indexer-db";
import type { SentrixClient } from "@sentriscloud/indexer-chain";

const ERC20_TRANSFER =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const ERC1155_SINGLE =
  "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
const ERC1155_BATCH =
  "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

interface SyncOnceArgs {
  db: DbClient;
  chain: SentrixClient;
  target: bigint;
  log: Logger;
}

/** Run one backfill batch from `last_synced_height + 1` up to `target`. */
export async function syncOnce(args: SyncOnceArgs): Promise<bigint> {
  const { db, chain, target, log } = args;
  const lastSynced = await readLastSynced(db);
  const start = lastSynced + 1n;
  if (start > target) return lastSynced;

  // Cap each pass to keep memory + transaction size bounded. Operator
  // can bump via env when running against a wg1 / loopback RPC that
  // doesn't enforce the public rate limit.
  const BATCH = BigInt(process.env.INDEXER_BATCH_SIZE ?? 50);
  const end = start + BATCH > target ? target : start + BATCH - 1n;

  log.info({ from: start.toString(), to: end.toString() }, "backfill batch");

  for (let h = start; h <= end; h++) {
    await indexBlock({ db, chain, height: h, log });
  }
  return end;
}

interface IndexBlockArgs {
  db: DbClient;
  chain: SentrixClient;
  height: bigint;
  log: Logger;
}

export async function indexBlock(args: IndexBlockArgs) {
  const { db, chain, height, log } = args;
  const block = await chain.getBlock(height);

  // We do all writes inside a single transaction so a partial block never
  // ends up in the DB on crash.
  await db.transaction(async (tx) => {
    await tx
      .insert(blocksTable)
      .values({
        height,
        hash: block.hash ?? "0x",
        parentHash: block.parentHash,
        timestamp: block.timestamp,
        validator: block.miner ?? "0x0000000000000000000000000000000000000000",
        gasUsed: block.gasUsed ?? 0n,
        gasLimit: block.gasLimit ?? 0n,
        baseFee: block.baseFeePerGas?.toString() ?? null,
        txCount: block.transactions.length,
        stateRoot: block.stateRoot ?? null,
      })
      .onConflictDoNothing();

    // Native-shape adapter. eth_getBlockByNumber on Sentrix returns hash-
    // only entries (chain doesn't honor includeTransactions=true), and
    // eth_getTransactionByHash returns the native `{transaction: {…}}`
    // wrapper instead of the EVM-spec shape viem expects. So we fetch
    // each tx via the chain native REST, map the native fields into the
    // indexer schema, and convert sentri → wei (1 sentri = 1e10 wei) so
    // the value/fee columns share the same 18-decimal scale as the EVM
    // rail. COINBASE sender sentinel maps to the all-zero address +
    // tx_type='coinbase' so consumers can filter rewards out of any
    // address-history query.
    const ZERO = "0x0000000000000000000000000000000000000000" as const;
    const SENTRI_TO_WEI = 10_000_000_000n;
    for (let i = 0; i < block.transactions.length; i++) {
      const entry = block.transactions[i];
      const hash = typeof entry === "string" ? entry : entry.hash;
      const native = await chain.getNativeTransaction(hash);
      if (!native) continue; // 404 or fetch error — skip, will retry on resync
      const inner = native.transaction;
      const isCoinbase = inner.from_address === "COINBASE";
      const fromAddr = isCoinbase ? ZERO : inner.from_address.toLowerCase();
      const toAddr = inner.to_address ? inner.to_address.toLowerCase() : null;
      const txHash = hash.startsWith("0x") ? hash.toLowerCase() : `0x${hash.toLowerCase()}`;
      await tx
        .insert(txsTable)
        .values({
          hash: txHash,
          blockHeight: height,
          txIndex: i,
          fromAddr,
          toAddr,
          value: (BigInt(inner.amount) * SENTRI_TO_WEI).toString(),
          gasLimit: 0n,
          gasUsed: 0n,
          gasPrice: null,
          fee: (BigInt(inner.fee) * SENTRI_TO_WEI).toString(),
          nonce: BigInt(inner.nonce),
          data: inner.data,
          status: 1,
          contractAddress: null,
          txType: isCoinbase ? "coinbase" : "native",
        })
        .onConflictDoNothing();

      // Upsert into addresses for both sender and receiver. Without this,
      // the table sits empty and any "list of addresses I've ever seen on
      // chain" query (eg `/contracts/stats`, scan's recent-deployments feed)
      // returns nothing — even though we have millions of indexed txs.
      // is_contract stays false here; a separate eth_getCode pass marks
      // it true for addresses with non-empty code (cheap, lazy backfill).
      // Coinbase sentinel skipped on the from side — the all-zero address
      // shouldn't claim a balance row from validator rewards.
      const heightBig = BigInt(height);
      if (!isCoinbase) {
        await tx
          .insert(addressesTable)
          .values({
            address: fromAddr,
            firstSeenBlock: heightBig,
            lastSeenBlock: heightBig,
          })
          .onConflictDoUpdate({
            target: addressesTable.address,
            set: {
              lastSeenBlock: sql`GREATEST(${addressesTable.lastSeenBlock}, EXCLUDED.last_seen_block)`,
            },
          });
      }
      if (toAddr) {
        await tx
          .insert(addressesTable)
          .values({
            address: toAddr,
            firstSeenBlock: heightBig,
            lastSeenBlock: heightBig,
          })
          .onConflictDoUpdate({
            target: addressesTable.address,
            set: {
              lastSeenBlock: sql`GREATEST(${addressesTable.lastSeenBlock}, EXCLUDED.last_seen_block)`,
            },
          });
      }
    }

    // Pull all logs in this block in one shot.
    const evmLogs = await chain.getLogsRange(height, height);
    for (const l of evmLogs) {
      if (
        l.blockNumber == null ||
        l.transactionHash == null ||
        l.logIndex == null
      ) {
        continue;
      }
      // Normalize address + topics to lowercase before insert. txs.fromAddr
      // / txs.toAddr already store lowercase (sync.ts:112-114), and downstream
      // consumers (scan, faucet, indexer endpoints) query with lowercase
      // WHERE clauses. If we leave logs.address mixed-case (some RPCs return
      // EIP-55 checksum), JOINs and address-history filters silently miss
      // events. topics also lowercased for selector-prefix LIKE patterns.
      const logAddr = l.address.toLowerCase();
      await tx
        .insert(logsTable)
        .values({
          blockHeight: l.blockNumber,
          txHash: l.transactionHash.toLowerCase(),
          logIndex: l.logIndex,
          address: logAddr,
          topic0: l.topics[0]?.toLowerCase() ?? null,
          topic1: l.topics[1]?.toLowerCase() ?? null,
          topic2: l.topics[2]?.toLowerCase() ?? null,
          topic3: l.topics[3]?.toLowerCase() ?? null,
          data: l.data,
        })
        .onConflictDoNothing();

      // Decoded transfer materialisation.
      const t0 = l.topics[0];
      if (t0 === ERC20_TRANSFER && l.topics.length === 3) {
        // ERC-20 Transfer(from indexed, to indexed, uint256 value)
        await tx
          .insert(tokenTransfers)
          .values({
            blockHeight: l.blockNumber,
            txHash: l.transactionHash,
            logIndex: l.logIndex,
            contract: logAddr,
            standard: "erc20",
            fromAddr: topicToAddress(l.topics[1]!),
            toAddr: topicToAddress(l.topics[2]!),
            tokenId: null,
            amount: BigInt(l.data || "0x0").toString(),
          })
          .onConflictDoNothing();
      } else if (t0 === ERC20_TRANSFER && l.topics.length === 4) {
        // ERC-721 Transfer(from indexed, to indexed, tokenId indexed)
        await tx
          .insert(tokenTransfers)
          .values({
            blockHeight: l.blockNumber,
            txHash: l.transactionHash,
            logIndex: l.logIndex,
            contract: logAddr,
            standard: "erc721",
            fromAddr: topicToAddress(l.topics[1]!),
            toAddr: topicToAddress(l.topics[2]!),
            tokenId: BigInt(l.topics[3]!).toString(),
            amount: "1",
          })
          .onConflictDoNothing();
      } else if (t0 === ERC1155_SINGLE) {
        // ERC-1155 TransferSingle(operator indexed, from indexed, to indexed, id, value)
        const data = l.data.replace(/^0x/, "");
        const id = BigInt("0x" + data.slice(0, 64));
        const value = BigInt("0x" + data.slice(64, 128));
        await tx
          .insert(tokenTransfers)
          .values({
            blockHeight: l.blockNumber,
            txHash: l.transactionHash,
            logIndex: l.logIndex,
            contract: logAddr,
            standard: "erc1155",
            fromAddr: topicToAddress(l.topics[2]!),
            toAddr: topicToAddress(l.topics[3]!),
            tokenId: id.toString(),
            amount: value.toString(),
          })
          .onConflictDoNothing();
      } else if (t0 === ERC1155_BATCH) {
        // Decoding a batch transfer from raw log data is non-trivial (two
        // dynamic arrays). Defer to Phase 2 — record the raw log here, the
        // worker that materialises balances can re-decode at its own pace.
      }
    }

    await tx
      .insert(meta)
      .values({
        key: "last_synced_height",
        value: height.toString(),
        updatedAt: BigInt(Math.floor(Date.now() / 1000)),
      })
      .onConflictDoUpdate({
        target: meta.key,
        set: {
          value: sql`excluded.value`,
          updatedAt: sql`excluded.updated_at`,
        },
      });
  });

  log.debug({ h: height.toString() }, "indexed");
}

async function readLastSynced(db: DbClient): Promise<bigint> {
  const rows = await db
    .select({ value: meta.value })
    .from(meta)
    .where(eq(meta.key, "last_synced_height"))
    .limit(1);
  if (!rows[0]) return 0n;
  return BigInt(rows[0].value);
}

function topicToAddress(topic: string): string {
  // Topics are 32-byte right-padded; addresses are the right-most 20 bytes.
  return "0x" + topic.slice(-40).toLowerCase();
}
