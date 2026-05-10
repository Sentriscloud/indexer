// ERC-721 Transfer handler. Shares topic0 with ERC-20 — disambiguated
// here by the indexed-arg count (4 topics: signature + indexed from,
// to, tokenId).

import { register, topicToAddress, type EventHandler } from "./registry.js";
import { ERC20_TRANSFER_TOPIC } from "./erc20.js";

const handler: EventHandler = {
  topic0: ERC20_TRANSFER_TOPIC,
  decode: ({ log, contract, txHash }) => {
    if (log.topics.length !== 4) return null;
    if (log.blockNumber == null || log.logIndex == null) return null;
    return {
      blockHeight: log.blockNumber,
      txHash,
      logIndex: log.logIndex,
      contract,
      standard: "erc721",
      fromAddr: topicToAddress(log.topics[1]!),
      toAddr: topicToAddress(log.topics[2]!),
      tokenId: BigInt(log.topics[3]!).toString(),
      amount: "1",
    };
  },
};

register(handler);
