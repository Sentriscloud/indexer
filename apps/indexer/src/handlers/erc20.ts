// ERC-20 Transfer handler. The Transfer event signature is shared with
// ERC-721 — both emit topic0 = keccak("Transfer(address,address,uint256)").
// The two are disambiguated by indexed-arg count: ERC-20 indexes from
// + to (3 topics total including topic0); ERC-721 also indexes the
// tokenId (4 topics total).

import { register, topicToAddress, type EventHandler } from "./registry.js";

export const ERC20_TRANSFER_TOPIC =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const handler: EventHandler = {
  topic0: ERC20_TRANSFER_TOPIC,
  decode: ({ log, contract, txHash }) => {
    // ERC-20 has exactly three topics (Transfer signature + indexed
    // from + indexed to). Anything else is the ERC-721 sibling
    // (4 topics) which the dedicated handler picks up via its own
    // length check.
    if (log.topics.length !== 3) return null;
    if (log.blockNumber == null || log.logIndex == null) return null;
    return {
      blockHeight: log.blockNumber,
      txHash,
      logIndex: log.logIndex,
      contract,
      standard: "erc20",
      fromAddr: topicToAddress(log.topics[1]!),
      toAddr: topicToAddress(log.topics[2]!),
      tokenId: null,
      // value is the unindexed uint256 in `data`. Empty data is a
      // malformed but on-chain-valid event; treat as zero so the row
      // still lands and the operator can grep for it later.
      amount: BigInt(log.data || "0x0").toString(),
    };
  },
};

register(handler);
