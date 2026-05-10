// GraphQL surface — Mercurius plugin mirroring the REST shape with a
// proper typed schema. dApp devs that prefer GraphQL (subgraph muscle
// memory, join-heavy queries) can hit /graphql instead of stitching
// multiple REST round-trips client-side.
//
// Design notes:
//
//   - Schema-first via SDL. Resolvers stay tiny — most just delegate to
//     the same Drizzle queries the REST routes use, so behaviour stays
//     in lock-step across the two surfaces.
//
//   - Custom BigInt scalar — block heights / wei amounts overflow
//     JavaScript Number. The serializer emits a string; the parser
//     accepts string or numeric literals. Same convention every modern
//     EVM client follows (Etherscan API, Alchemy, Infura).
//
//   - GraphiQL playground enabled in non-production for self-serve
//     schema exploration. Production deploys can flip it off via env.
//
//   - No subscriptions yet. The push surface lives in the chain node's
//     gRPC StreamEvents; adding a GraphQL subscription tier here would
//     duplicate state and risk drift. Revisit when sdk-ts demand
//     emerges.

import mercurius from "mercurius";
import { GraphQLScalarType, Kind } from "graphql";
import type { FastifyInstance } from "fastify";
import { and, asc, desc, eq, lte, or } from "drizzle-orm";

import {
  addresses,
  blocks,
  logs,
  tokenTransfers,
  transactions,
  type DbClient,
} from "@sentriscloud/indexer-db";

const MAX_PAGE = 100;

const SCHEMA = /* GraphQL */ `
  scalar BigInt

  type Block {
    height: BigInt!
    hash: String!
    parentHash: String!
    timestamp: BigInt!
    validator: String!
    gasUsed: BigInt!
    gasLimit: BigInt!
    baseFee: String
    txCount: Int!
    stateRoot: String
    round: Int!
    transactions: [Tx!]!
  }

  type Tx {
    hash: String!
    blockHeight: BigInt!
    txIndex: Int!
    from: String!
    to: String
    value: String!
    fee: String!
    nonce: BigInt!
    data: String
    status: Int!
    contractAddress: String
    txType: String!
    logs: [Log!]!
  }

  type Log {
    blockHeight: BigInt!
    txHash: String!
    logIndex: Int!
    address: String!
    topics: [String!]!
    data: String
  }

  type Transfer {
    blockHeight: BigInt!
    txHash: String!
    logIndex: Int!
    contract: String!
    standard: String!
    from: String!
    to: String!
    tokenId: String
    amount: String!
  }

  type Address {
    address: String!
    firstSeenBlock: BigInt!
    lastSeenBlock: BigInt!
    isContract: Boolean!
    codeHash: String
    txs(limit: Int = 25): [Tx!]!
    transfers(limit: Int = 25, standard: String): [Transfer!]!
  }

  type Query {
    block(height: BigInt!): Block
    blocks(limit: Int = 25, before: BigInt): [Block!]!
    tx(hash: String!): Tx
    address(address: String!): Address
  }
`;

// Custom BigInt scalar — serialises to string (clients parse via
// BigInt()), accepts integer or string input on the parse side.
const BigIntScalar = new GraphQLScalarType<bigint, string>({
  name: "BigInt",
  description:
    "Arbitrary-precision integer; serialised as a base-10 string so values larger than 2^53 survive the JSON round-trip.",
  serialize(value) {
    if (typeof value === "bigint") return value.toString();
    if (typeof value === "number") return Math.trunc(value).toString();
    if (typeof value === "string") return value;
    throw new TypeError(`BigInt cannot serialize ${typeof value}`);
  },
  parseValue(value) {
    if (typeof value === "string" || typeof value === "number") return BigInt(value);
    throw new TypeError(`BigInt cannot parse ${typeof value}`);
  },
  parseLiteral(ast) {
    if (ast.kind === Kind.INT || ast.kind === Kind.STRING) return BigInt(ast.value);
    throw new TypeError(`BigInt cannot parse literal ${ast.kind}`);
  },
});

function clampLimit(raw: number | undefined): number {
  const n = raw ?? 25;
  if (!Number.isFinite(n) || n <= 0) return 25;
  return Math.min(n, MAX_PAGE);
}

interface Ctx {
  db: DbClient;
}

// Resolver shapes mirror the REST serialisers in routes/native.ts so
// the two surfaces never disagree on field names or formatting (fee /
// value as decimal-string for u256 fits, hashes lowercase, etc).
const resolvers = {
  BigInt: BigIntScalar,
  Query: {
    async block(_root: unknown, args: { height: bigint }, ctx: Ctx) {
      const row = await ctx.db
        .select()
        .from(blocks)
        .where(eq(blocks.height, args.height))
        .limit(1);
      return row[0] ?? null;
    },
    async blocks(
      _root: unknown,
      args: { limit?: number; before?: bigint },
      ctx: Ctx,
    ) {
      const limit = clampLimit(args.limit);
      const where = args.before !== undefined ? lte(blocks.height, args.before) : undefined;
      return ctx.db
        .select()
        .from(blocks)
        .where(where)
        .orderBy(desc(blocks.height))
        .limit(limit);
    },
    async tx(_root: unknown, args: { hash: string }, ctx: Ctx) {
      const row = await ctx.db
        .select()
        .from(transactions)
        .where(eq(transactions.hash, args.hash.toLowerCase()))
        .limit(1);
      return row[0] ?? null;
    },
    async address(_root: unknown, args: { address: string }, ctx: Ctx) {
      const row = await ctx.db
        .select()
        .from(addresses)
        .where(eq(addresses.address, args.address.toLowerCase()))
        .limit(1);
      return row[0] ?? null;
    },
  },
  Block: {
    async transactions(
      parent: typeof blocks.$inferSelect,
      _args: unknown,
      ctx: Ctx,
    ) {
      return ctx.db
        .select()
        .from(transactions)
        .where(eq(transactions.blockHeight, parent.height))
        .orderBy(asc(transactions.txIndex));
    },
  },
  Tx: {
    from: (parent: typeof transactions.$inferSelect) => parent.fromAddr,
    to: (parent: typeof transactions.$inferSelect) => parent.toAddr,
    async logs(parent: typeof transactions.$inferSelect, _args: unknown, ctx: Ctx) {
      const rows = await ctx.db
        .select()
        .from(logs)
        .where(eq(logs.txHash, parent.hash))
        .orderBy(asc(logs.logIndex));
      return rows.map((l) => ({
        ...l,
        topics: [l.topic0, l.topic1, l.topic2, l.topic3].filter(
          (t): t is string => Boolean(t),
        ),
      }));
    },
  },
  Address: {
    async txs(
      parent: typeof addresses.$inferSelect,
      args: { limit?: number },
      ctx: Ctx,
    ) {
      const limit = clampLimit(args.limit);
      const a = parent.address;
      // Composite (from_addr, block_height) + (to_addr, block_height)
      // indexes from migration 0004 serve filter + sort in one scan.
      return ctx.db
        .select()
        .from(transactions)
        .where(or(eq(transactions.fromAddr, a), eq(transactions.toAddr, a)))
        .orderBy(desc(transactions.blockHeight))
        .limit(limit);
    },
    async transfers(
      parent: typeof addresses.$inferSelect,
      args: { limit?: number; standard?: string },
      ctx: Ctx,
    ) {
      const limit = clampLimit(args.limit);
      const a = parent.address;
      const where = args.standard
        ? and(
            or(eq(tokenTransfers.fromAddr, a), eq(tokenTransfers.toAddr, a)),
            eq(tokenTransfers.standard, args.standard),
          )
        : or(eq(tokenTransfers.fromAddr, a), eq(tokenTransfers.toAddr, a));
      const rows = await ctx.db
        .select()
        .from(tokenTransfers)
        .where(where)
        .orderBy(desc(tokenTransfers.blockHeight))
        .limit(limit);
      return rows.map((t) => ({
        blockHeight: t.blockHeight,
        txHash: t.txHash,
        logIndex: t.logIndex,
        contract: t.contract,
        standard: t.standard,
        from: t.fromAddr,
        to: t.toAddr,
        tokenId: t.tokenId,
        amount: t.amount,
      }));
    },
  },
};

export async function registerGraphql(app: FastifyInstance, ctx: { db: DbClient }) {
  await app.register(mercurius, {
    schema: SCHEMA,
    // Mercurius types resolvers loosely on purpose — the SDL above + the
    // typed Drizzle helpers are the real contract.
    resolvers: resolvers as unknown as mercurius.IResolvers,
    context: () => ({ db: ctx.db }),
    graphiql: process.env.NODE_ENV !== "production",
    // Fastify 5 needs the path explicit; default would collide with the
    // root Caddy redirect logic on some deployments.
    path: "/graphql",
  });
}
