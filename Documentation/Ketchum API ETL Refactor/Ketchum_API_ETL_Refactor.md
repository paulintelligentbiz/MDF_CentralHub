# Ketchum API ETL Refactor — Parallel Per-Topic Ingestion

## Status

Design sketch only — nothing in this document has been implemented. Written in response to a
question about whether the current `nb_Bronze_Generic` / per-source-notebook pattern (observed
live in `DMI Sensing CentralHub DEV`, source-copied from `Sensing Test`) could run faster. See
the companion diagram: [Ketchum_API_ETL_Refactor_Diagram.mmd](Ketchum_API_ETL_Refactor_Diagram.mmd).

## Current architecture (as-is)

`nb_Bronze_Generic` is a source-agnostic driver notebook. Given a `source_name` parameter, it:

1. Reads that source's connection config from `meta.bronze_sources` (`Base_URL`, `Secret_Name`,
   `API_Notebook`) and its list of topics from `meta.bronze_topics` (`Topic_Name`, `Search_Query`,
   `Project_ID`, `Countries`, `Resume_Offset`, `Is_Active`).
2. Loops over each active topic **sequentially**. For each one, it calls
   `notebookutils.notebook.run(source_config.API_Notebook, 1800, {...topic params...})` —
   e.g. `nb_Bronze_Dimensions` for `source_name = "dimensions"`, `nb_Bronze_Altmetric` for
   `"altmetric"`.
3. That child notebook knows nothing about any other topic — it just calls the source's API with
   the parameters it was given and builds one or more `global_temp` views
   (`dimensions_pub` / `dimensions_policy` / `dimensions_grant` for Dimensions; a single view named
   after the source for Altmetric).
4. Back in `nb_Bronze_Generic`, immediately after the child notebook returns, it reads those exact
   `global_temp` views, appends them to their permanent `bronze.*` Delta tables (partitioned by
   `topic_part`/`year_part`/`month_part`/`day_part`), and drops the temp views.
5. Only then does it move on to the next topic and repeat.

**Why it's sequential today:** `notebookutils.notebook.run(...)` is called at the *top level* of
`nb_Bronze_Generic` (not nested inside a wrapper), which is exactly what makes the child
notebook's `global_temp` views visible back in `nb_Bronze_Generic`'s own session once the child
finishes — a Spark/Trident constraint, not a design choice. `notebookutils.notebook.runMultiple()`
would let topics run in parallel, but each parallel run gets its **own isolated session**, so its
`global_temp` views would never be visible back in the caller. The current design trades speed for
the simplicity of one shared session and one shared persistence routine
(`append_bronze_table`, defined once in `nb_Bronze_Generic`).

**Observed cost:** ~30 seconds per topic, run one at a time. For Dimensions' 14 active topics
that's roughly 7 minutes end to end; it scales linearly as more topics or sources are added.

## Proposed architecture (parallel, isolated sessions)

The core change: stop relying on a shared session at all. Instead of a child notebook handing
data *back* to `nb_Bronze_Generic` via a `global_temp` view, each per-topic run persists its own
data and reports back only a small, serializable result (the new offset) — which removes the
session-sharing requirement and unlocks real parallelism.

1. **Move `append_bronze_table` (the read-view → partition → append-Delta → drop-view logic)
   out of `nb_Bronze_Generic` and into `nb_Global_Functions`.** Every source notebook already
   `%run`s `nb_Global_Functions`, so this is a relocation, not new logic, and keeps the
   partitioning/write behavior centralized in one place rather than duplicated per source.
2. **Each source notebook (`nb_Bronze_Dimensions`, `nb_Bronze_Altmetric`, etc.) becomes fully
   self-contained per topic run**: call the API, build the DataFrame(s), call
   `append_bronze_table(...)` itself (now available via `nb_Global_Functions`), then
   `notebookutils.notebook.exit(json.dumps({"topic_name": ..., "new_offset": ...}))`. No
   `global_temp` view needs to survive past the end of that one notebook's own execution.
3. **`nb_Bronze_Generic` becomes a pure orchestrator.** It builds a DAG (or flat list, if there
   are no cross-topic dependencies — there don't appear to be any today) of per-topic run
   requests from `meta.bronze_topics`, then calls
   `notebookutils.notebook.runMultiple(dag, {"concurrency": N})` once, instead of looping
   `notebook.run` one topic at a time.
4. **After `runMultiple` returns**, `nb_Bronze_Generic` reads the `exit_value` from each entry in
   the result dict (mirroring the already-commented-out `for name, details in e.result.items()`
   pattern visible in `Bronze Pipeline` today) and writes each topic's new `Resume_Offset` back to
   `meta.bronze_topics` — batched into one update rather than one per topic.

### Concurrency / rate limiting

`runMultiple`'s `concurrency` setting becomes the one place controlling how many simultaneous API
calls hit Dimensions/Altmetric at once. This needs to be set deliberately (not "as high as
possible") to respect each API's own rate limits — likely a per-source concurrency value read
from `meta.bronze_sources` (a new column) rather than one constant, since different APIs
tolerate different levels of concurrency.

### Error handling

The current sequential loop wraps each topic's `notebook.run` call in its own `try/except`,
logs a per-topic failure, and continues to the next topic regardless. `runMultiple` reports
success/failure per DAG node in its own result structure rather than raising per-iteration — the
replacement orchestration logic needs to explicitly walk that result set and preserve the same
"one topic's failure doesn't block the others" behavior, rather than letting the whole
`runMultiple` call fail on a single bad topic.

## Tradeoffs

| | Current (sequential, shared session) | Proposed (parallel, isolated sessions) |
|---|---|---|
| Throughput | ~30s × topic count, strictly serial | Bounded by slowest topic × (topic count / concurrency) |
| Persistence logic location | Centralized in `nb_Bronze_Generic` only | Centralized in `nb_Global_Functions`, called from every source notebook |
| Session model | One shared session (required for `global_temp` hand-off) | Each topic run fully isolated — no hand-off needed |
| Failure isolation | Per-topic try/except in one loop | Per-DAG-node result from `runMultiple`, needs equivalent handling written explicitly |
| New-source cost | Add config rows + one notebook that builds `global_temp` views | Add config rows + one notebook that calls `append_bronze_table` and exits with an offset |

## Open questions before implementing

- **Where `Resume_Offset` is actually written back today** wasn't confirmed in this session — the
  `[('Animal Welfare', '831'), ...]` exit-value list observed live suggests `nb_Bronze_Generic`
  hands offsets back up to whatever calls *it* (`Bronze Pipeline`?), rather than writing
  `meta.bronze_topics` itself. The proposed design assumes `nb_Bronze_Generic` should own that
  write directly — worth confirming against `Bronze Pipeline`'s actual logic before building this.
- **Per-source concurrency limits** aren't modeled anywhere today (`meta.bronze_sources` has no
  rate-limit column) — needs a real value per API, not a guess, before enabling parallel calls.
- This sketch only covers Dimensions/Altmetric, the two sources actually deployed into
  `DMI Sensing CentralHub DEV` so far. The other six sources configured in `meta.bronze_sources`
  (Glimpse, Polimonitor, Talkwalker ×2, US Gov Info, YouScan) live only in `Sensing Test` today and
  weren't inspected for this design.
