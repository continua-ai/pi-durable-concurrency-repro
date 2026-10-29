// Standalone @earendil-works/pi-durable@1.4.0 concurrency microbenchmark (N = 1, 2, 4, 8, 16, 32)
// Zero CPU work & zero external effects inside any transaction callback.
// Run: node repro-pi-durable-concurrency.mjs
import { performance } from "node:perf_hooks";
import { writeFileSync } from "node:fs";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { createRegistry, defineDoc, defineEntry, Harness, MemoryStorage } from "@earendil-works/pi-durable";

const ctx = BACKGROUND_CONTEXT;
const DB_RTT_MS = 4; // Simulated 4ms network DB round-trip (e.g. same-region Postgres/CockroachDB pool)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Wrap MemoryStorage to simulate a 4ms network DB round-trip per Storage I/O call.
// A 32-connection DB pool can execute 32 disjoint conversation commits in parallel in ~4ms,
// whereas SessionImpl.#tail serializes every commitWith(), readOnLine(), and cold snapshot() in one FIFO queue.
function createNetworkedStorage(rttMs = DB_RTT_MS) {
  const mem = new MemoryStorage();
  return new Proxy(mem, {
    get(target, prop) {
      const val = Reflect.get(target, prop);
      if (typeof val !== "function") return val;
      if (
        prop === "commit" ||
        prop === "findDocument" ||
        prop === "document" ||
        prop === "conversation" ||
        prop === "scanEntries"
      ) {
        return async (...args) => {
          await sleep(rttMs);
          return Reflect.apply(val, target, args);
        };
      }
      return val.bind(target);
    },
  });
}

const PerConvNoteDoc = defineDoc({
  kind: "repro.conv.note",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({ rev: 0, text: "" }),
  checkpointWhen: (_v, _ops, info) => info.deltasSinceBase >= 32,
});

const TurnNoteEntry = defineEntry("repro.turn.note");

function pct(arr, q) {
  const s = [...arr].sort((a, b) => a - b);
  return Number(s[Math.min(s.length - 1, Math.floor(q * (s.length - 1)))].toFixed(1));
}

const storage = createNetworkedStorage(DB_RTT_MS);
const harness = await Harness.open(
  storage,
  { models: createModels(), registry: createRegistry() },
  ctx,
);

// Pre-create 36 disjoint conversations (0..31 for active workers, 32..35 for cold snapshot reads)
const convIds = [];
for (let i = 0; i < 36; i++) {
  const convId = await harness.commit(async (tx) => {
    const c = await tx.createConversation({ ownership: { kind: "ownerless" } });
    const d = await tx.doc(PerConvNoteDoc, c.id);
    d.rev = 1;
    return c.id;
  }, ctx);
  convIds.push(convId);
}

const steps = [1, 2, 4, 8, 16, 32];
const rows = [];

for (const N of steps) {
  // 1. Ideal per-conversation commit queue over a 32-connection DB pool (N disjoint 4ms commits in parallel)
  const idealLats = await Promise.all(
    Array.from({ length: N }, async () => {
      const s = performance.now();
      await sleep(DB_RTT_MS);
      return performance.now() - s;
    }),
  );

  // 2. pi-durable: N concurrent commits to 100% DISJOINT ConversationDocTokens (warm cache, 0ms CPU in callback)
  const t0Doc = performance.now();
  const docLats = await Promise.all(
    convIds.slice(0, N).map(async (id, idx) => {
      const s = performance.now();
      await harness.commit(async (tx) => {
        const d = await tx.doc(PerConvNoteDoc, id);
        d.rev += 1;
        d.text = "step-" + N + "-" + idx;
      }, ctx);
      return performance.now() - s;
    }),
  );
  const docWall = Number((performance.now() - t0Doc).toFixed(1));

  // 3. pi-durable: N concurrent commits to 100% DISJOINT conversations appending 1 entry + updating 1 doc
  //    while 2 cold snapshot() reads happen on unrelated conversations (convIds[32..33]). Zero CPU work in callbacks.
  await harness.unloadDocuments();
  // Re-warm active worker docs so only convIds[32..33] are cold snapshots
  for (const id of convIds.slice(0, N)) {
    await harness.snapshot(PerConvNoteDoc, id, ctx);
  }
  const coldReads = Promise.all([
    harness.snapshot(PerConvNoteDoc, convIds[32], ctx),
    harness.snapshot(PerConvNoteDoc, convIds[33], ctx),
  ]);
  const entryAndDocLats = await Promise.all(
    convIds.slice(0, N).map(async (id, idx) => {
      const s = performance.now();
      await harness.commit(async (tx) => {
        await tx.appendEntry(TurnNoteEntry, id, { data: { step: idx } });
        const d = await tx.doc(PerConvNoteDoc, id);
        d.rev += 1;
      }, ctx);
      return performance.now() - s;
    }),
  );
  await coldReads;

  rows.push({
    N,
    idealPoolP95Ms: pct(idealLats, 0.95),
    disjointDocP50Ms: pct(docLats, 0.5),
    disjointDocP95Ms: pct(docLats, 0.95),
    disjointDocWallMs: docWall,
    disjointEntryPlusColdSnapP95Ms: pct(entryAndDocLats, 0.95),
  });
}

await harness.close(ctx);
console.table(rows);
writeFileSync(
  new URL("./repro-results.json", import.meta.url),
  JSON.stringify(rows, null, 2),
);
