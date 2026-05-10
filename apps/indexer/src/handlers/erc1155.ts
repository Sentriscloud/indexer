// ERC-1155 TransferSingle handler. Layout differs from ERC-20/721:
//   topic0: TransferSingle signature
//   topic1: indexed operator (ignored — same as msg.sender for transfers)
//   topic2: indexed from
//   topic3: indexed to
//   data:   abi.encode(uint256 id, uint256 value) — two 32-byte words
//
// TransferBatch shares the same shape header but data is two dynamic
// arrays — non-trivial to decode without an ABI decoder. We register
// a stub that records the raw log via sync.ts (the registry returns
// null so no transfer row, but the log row still lands) and defer the
// batch materialisation to a follow-up worker.

import { register, topicToAddress, type EventHandler } from "./registry.js";

export const ERC1155_TRANSFER_SINGLE =
  "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
export const ERC1155_TRANSFER_BATCH =
  "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

const single: EventHandler = {
  topic0: ERC1155_TRANSFER_SINGLE,
  decode: ({ log, contract, txHash }) => {
    if (log.blockNumber == null || log.logIndex == null) return null;
    if (log.topics.length < 4) return null;
    const data = log.data.replace(/^0x/, "");
    if (data.length < 128) return null; // need two 32-byte words
    const id = BigInt("0x" + data.slice(0, 64));
    const value = BigInt("0x" + data.slice(64, 128));
    return {
      blockHeight: log.blockNumber,
      txHash,
      logIndex: log.logIndex,
      contract,
      standard: "erc1155",
      fromAddr: topicToAddress(log.topics[2]!),
      toAddr: topicToAddress(log.topics[3]!),
      tokenId: id.toString(),
      amount: value.toString(),
    };
  },
};

const batch: EventHandler = {
  topic0: ERC1155_TRANSFER_BATCH,
  decode: () => {
    // Two dynamic arrays encoded inline — the per-transfer rows can't
    // be flattened cleanly into the schema's one-row-per-transfer
    // shape without growing tokenTransfers. The raw log still lands
    // via sync.ts so the deferred batch materialiser can re-decode.
    return null;
  },
};

register(single);
register(batch);
