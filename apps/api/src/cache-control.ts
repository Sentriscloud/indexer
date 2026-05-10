// Per-route Cache-Control policy. Most read endpoints serve stable data
// (block headers, finalized txs, address history) but every consumer
// (scan, faucet, dapps) re-fetches the same row hundreds of times per
// minute. A short shared cache at the edge — Caddy + browser — absorbs
// the duplication without hiding live data updates.
//
// Conventions per response shape:
//   - live-tip data (chain/info, latest blocks): 2 s. One block-time
//     window so users still see new blocks within a tick.
//   - finalized objects with a cursor (specific block / tx): 5 min +
//     immutable, since block N or tx H never changes once mined.
//   - paginated lists keyed off the latest tip (address/txs, whale/tx,
//     contracts/recent): 10–30 s. Long enough to dedupe burst traffic,
//     short enough the next request lands in roughly real time.
//   - aggregate stats (daily counts, validator scores): 30–60 s. The
//     materialised view / 5-min in-memory cache already absorbs query
//     cost; HTTP cache layer just stops the round-trip.
//
// Routes are free to set their own Cache-Control inside the handler —
// the plugin only fills in a default when none is set, never overrides.

import type { FastifyInstance } from "fastify";

const POLICIES: Array<[RegExp, string]> = [
  [/^\/health$/, "no-store"],
  [/^\/chain\/info$/, "public, max-age=2"],
  [/^\/blocks$/, "public, max-age=2"],
  // Specific block / tx are immutable once finalized; SAFE_LAG (5 by
  // default) keeps the indexer behind tip so we never cache an
  // unfinalized object. immutable hint lets browsers skip revalidation.
  [/^\/blocks\/[0-9]+$/, "public, max-age=300, immutable"],
  [/^\/tx\/0x[0-9a-f]+$/i, "public, max-age=300, immutable"],
  [/^\/address\/0x[0-9a-f]+\/(txs|transfers)/i, "public, max-age=10"],
  [/^\/address\/0x[0-9a-f]+$/i, "public, max-age=10"],
  [/^\/stats\/daily$/, "public, max-age=60"],
  [/^\/accounts\/active/, "public, max-age=30"],
  [/^\/contracts\/recent/, "public, max-age=10"],
  [/^\/contracts\/pioneers/, "public, max-age=300, immutable"],
  [/^\/contracts\/stats/, "public, max-age=30"],
  [/^\/whale\/tx/, "public, max-age=10"],
  [/^\/validators$/, "public, max-age=30"],
  [/^\/epochs$/, "public, max-age=60"],
  [/^\/tokens$/, "public, max-age=30"],
  [/^\/tokens\/0x[0-9a-f]+\/holders/i, "public, max-age=30"],
];

export function registerCacheControl(app: FastifyInstance) {
  app.addHook("onSend", async (req, reply, payload) => {
    // Skip errors — 4xx/5xx should always revalidate.
    if (reply.statusCode >= 400) return payload;
    // Don't override an explicit policy set by the route handler.
    if (reply.getHeader("cache-control")) return payload;
    for (const [pattern, value] of POLICIES) {
      if (pattern.test(req.url.split("?")[0])) {
        reply.header("Cache-Control", value);
        // Explicit Vary so a future Accept-based variant doesn't
        // collide in shared caches.
        reply.header("Vary", "Accept, Accept-Encoding");
        break;
      }
    }
    return payload;
  });
}
