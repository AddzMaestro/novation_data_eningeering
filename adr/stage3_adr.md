# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** Adolph Mojapelo
**Date:** 2026-05-02
**Status:** Final

---

## Context

Stage 3 introduced a real-time requirement: the mobile virtual-account app must surface current balance and recent transactions within seconds of a transaction, rather than waiting for the next overnight batch. The fintech now provides a stream of micro-batch JSONL files at `/data/stream/`, each containing 50–500 events, with the same transaction schema as the Stage 2 batch source. The pipeline must (a) keep the Stage 2 batch path running unchanged, (b) poll the stream directory for new files, (c) maintain two new Gold tables — `current_balances/` (one row per `account_id`, upserted) and `recent_transactions/` (last 50 events per account, retained by `transaction_timestamp DESC`) — and (d) hit a 5-minute SLA measured as `updated_at − transaction_timestamp`. Same Docker constraints apply: 2 GB RAM, 2 vCPU, 30-min wall clock, `--read-only` rootfs with a 512 MB `/tmp` tmpfs, `--network=none`.

Coming into Stage 3 my repository had ~1,100 lines of Python across 11 modules: a thin orchestrator (`run_all.py`), an ingest layer that already preserved `_raw_line` for downstream DQ detection, externalised DQ rules in `config/dq_rules.yaml`, composable transform helpers, a star-schema provisioner, boundary-only metric collectors, and a `dq_report.json` builder. The Spark session factory was already tuned for the scoring container (1 GB driver / 512 MB executor / 4 shuffle partitions / `spark.local.dir → /data/output/.spark_scratch` to escape the 512 MB tmpfs). All of this carried over with no modification — the only orchestration change was three lines in `run_all.py` to invoke the new stream module after the batch DQ report is written.

---

## Decision 1: How did your existing Stage 1/2 architecture facilitate or hinder the streaming extension?

### What made Stage 3 easier

Three Stage 2 design choices repaid themselves immediately:

1. **Raw-line preservation in `pipeline/ingest.py` and the `detect_dq_signals_from_raw` helper in `pipeline/transforms.py`.** Stage 3 §2 warns that "the same data quality variance that applies to Stage 2 batch data may be present in stream events." Because stream events are read with the same `spark.read.text(...) → from_json(_, SOURCE_TRANSACTIONS_SCHEMA)` pattern that Stage 2 batch uses, the regex-based TYPE_MISMATCH / DATE_FORMAT / CURRENCY_VARIANT detectors plug straight in. `pipeline/stream_ingest.py::_enrich` calls the same `flatten_location()`, `flatten_metadata()`, `detect_dq_signals_from_raw("_raw_line")`, `cast_date()`, `cast_decimal()`, `normalise_currency()` and `assign_dq_flag()` functions the Silver layer already used.

2. **Externalised DQ rules in `config/dq_rules.yaml`.** The streaming module reads the same rules file via `load_dq_rules()`. The `dq_flag_priority` list (`TYPE_MISMATCH > DATE_FORMAT > CURRENCY_VARIANT`) governs both batch and stream — there is no Stage 3-specific DQ policy to maintain.

3. **Spark session as a singleton tuned for the scoring container.** `pipeline/spark_session.py::get_or_create_spark` returns the same `SparkSession` the batch path built. Spec §8 puts "concurrent Spark session management" on the participant; the singleton sidesteps it entirely. The `spark.local.dir → /data/output/.spark_scratch` redirect, originally added to fit the Stage 2 dedup window-shuffle into the 512 MB tmpfs, also covers the stream MERGE shuffles.

### What made Stage 3 harder

One real friction point: my Stage 2 `pipeline/transform.py` flattens `location` and `metadata` structs eagerly as part of the batch flow. The stream module needs the same flattening but writes its output to a different schema (`recent_transactions` doesn't carry province or device_id). I extracted the flatteners as standalone helpers in `transforms.py` rather than letting them stay inline in the batch transform — that refactor was the only structural change Stage 3 forced into the existing batch code. Without it I would have had to duplicate ~20 lines of struct-flattening logic in the stream module.

A second, smaller friction: the Gold provisioner in `pipeline/provision.py` is hardcoded to write to `gold/` (fact + two dims). The stream Gold layer lives at `stream_gold/` because its semantics are different (mutable upsert, retention pruning) — but my `pipeline_config.yaml` only had a single `output.gold_path` key, so I added a `streaming.stream_gold_path` key instead. In hindsight, a `gold:` block with sub-paths for `batch:` and `stream:` would have been cleaner, but the cost is one extra config key, not a structural change.

### Code survival rate

About **95 %** of the Stage 1/2 codebase survives intact into Stage 3. Concretely:

- `pipeline/spark_session.py`, `pipeline/schemas.py`, `pipeline/dq_rules.py`, `pipeline/transforms.py`, `pipeline/dq_metrics.py`, `pipeline/dq_report.py`, `pipeline/logger.py`, `pipeline/ingest.py`, `pipeline/transform.py`, `pipeline/provision.py` — **0 lines changed for Stage 3**.
- `pipeline/run_all.py` — **+4 lines** (one import, three lines guarding the `run_stream_ingestion(config)` call on a `streaming` config block).
- `config/pipeline_config.yaml` — **+7 lines** (the `streaming:` block).
- `pipeline/stream_ingest.py` — **+330 lines new**, but every cleaning step inside it is a call into existing transform helpers.

The streaming path is genuinely additive. That is the strongest evidence that the Stage 1 architecture was right for the long arc of the challenge.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

Three concrete things I would do differently if I were starting over, in priority order:

1. **I would have introduced a `pipeline/output_layer.py` (or `gold_writer.py`) abstraction from Day 1, with `BatchGoldWriter` and `StreamGoldWriter` subclasses.** Today, my batch Gold writer lives inline in `pipeline/provision.py` (uses `mode("overwrite")` writes against full snapshots) and my stream Gold writer lives inline in `pipeline/stream_ingest.py::_merge_current_balances` and `_merge_recent_transactions` (uses `DeltaTable.forPath(...).merge(...)`). The two share no code, but they share the concept of "write a Gold table." A common abstraction would have given me one place to enforce the Gold schemas and the merge-key contracts. Stage 3's two new tables (`current_balances`, `recent_transactions`) would have slotted in as two new writer subclasses, instead of triggering a fresh module that re-derives the merge pattern.

2. **I would have made `pipeline/run_all.py` accept a `--mode {batch,stream,both}` argument from the start, defaulting to `both`**, instead of guarding the stream call with `if "streaming" in config:`. The current shape works but couples *whether* the stream runs to the *contents* of the config file, which is implicit. An explicit mode flag would let me run the batch and stream paths independently in development (useful when iterating on stream logic without burning 8 minutes on the batch each time), and would document at the entry point that the pipeline has two modes. The scoring system invokes `python pipeline/run_all.py` with no arguments, so the default needs to remain `both`.

3. **I would have defined the Gold table schemas in a single `pipeline/schemas.py` rather than inline in `provision.py` and `stream_ingest.py`.** Today, `FACT_COLS` lives in `provision.py` and the two stream Gold schemas live in `stream_ingest.py::_ensure_target_tables`. When Stage 3 required the new `current_balances` and `recent_transactions` tables I had to add their `StructType` definitions inside the stream module. Centralising them in `schemas.py` (next to the source-data schemas already there) would have made the Gold contract auditable in one place, and would have surfaced the column-name and type contracts to anyone reading the repo without needing to find the writer code.

I would not change the externalised DQ rules, the raw-line preservation, the `spark.local.dir` redirect, the broadcast-join dim strategy, the deterministic `Window.orderBy(natural_key)` surrogate keys, or the boundary-only `.agg()`-once metric collection. Those decisions all paid off across both stages.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If I had Day 1 visibility into the full three-stage spec, I would change four things — moving from a "batch pipeline that grew a stream tail" into a "shared-core pipeline with two ingestion modes":

**Ingestion as a pluggable layer.** I would define an `IngestionSource` protocol (Python `Protocol` or duck-typed interface) with two implementations: `BatchFileSource` (CSV / JSONL one-shot reader) and `StreamDirectorySource` (poll-with-state). Both yield `(file_id, raw_dataframe)` tuples. The cleaning pipeline downstream — flatten / detect / cast / dq_flag — would consume that abstraction and not care which source produced the rows. Today my stream ingest re-implements the file-discovery loop and the raw-line read; with the Day 1 abstraction those would be ~30 lines of shared code.

**State management as a first-class concern.** `current_balances` is the first stateful table in my pipeline — every other Gold table is derivable from a one-shot batch. I would have introduced a `pipeline/state.py` module from the start, with three primitives: `load_processed(...)`, `mark_processed(...)`, and `merge_state_table(target_path, input_df, key_cols, update_set, insert_values)`. The current code spreads this over `_load_processed_set`, `_mark_processed`, and the two `DeltaTable.merge(...)` blocks in `stream_ingest.py`. A state module would also be the right place to handle Delta Lake's `OPTIMIZE` and `VACUUM` cycles — important for long-running streams where small files pile up but irrelevant for a one-shot batch.

**One Gold output module, two semantics.** Today I have `gold/` (batch, immutable per run) and `stream_gold/` (stream, mutable upsert). A cleaner Day 1 design has a single `gold/` namespace with sub-folders that signal write semantics: `gold/snapshot/` for batch-overwrite tables and `gold/state/` for stream-merged tables. Both are still Delta, both still queryable by DuckDB; the directory structure documents the contract.

**Single entry point with explicit phase selection.** I would build `run_all.py` as a thin dispatcher around an explicit `phases = ["ingest", "transform", "provision", "report", "stream"]` list, with `--phases ingest,transform` to skip ahead during development. Stage 3 then becomes a new phase appended to the list, not a new conditional. This also makes the SLA constraint visible in the entry point: stream is the only phase that polls and quiesces; the other phases are one-shot and complete in ≤ 10 min.

Two architectural debts the current code carries that the Day 1 design would have avoided: (1) `pipeline/run_all.py` knows the order of every phase (it imports `run_ingestion`, `run_transformation`, `run_provisioning`, `run_stream_ingestion` directly), so adding a phase requires editing the orchestrator; (2) the stream module has its own copy of the schema-creation boilerplate (`_ensure_target_tables`) because no shared helper exists for "create empty Delta table if missing." Both would be solved by the abstractions above.

---

## Appendix

**Stage 3 source-of-truth files added:**

```
pipeline/stream_ingest.py     330 lines — poll loop, DQ enrich, two Delta MERGE upserts
adr/stage3_adr.md              this file
```

**Stage 3 source-of-truth files modified:**

```
pipeline/run_all.py            +4 lines (import + guarded call)
config/pipeline_config.yaml    +7 lines (streaming: block)
```

**Stage 3 stream_gold contract (per stream_interface_spec.md §§4–5):**

```
/data/output/stream_gold/
├── current_balances/      Delta — 4 cols, MERGE on account_id
└── recent_transactions/   Delta — 7 cols, MERGE on (account_id, transaction_id), retain top 50 per account
```

**Sign convention for `current_balance` deltas:**

```
DEBIT, FEE       →  −amount   (money out)
CREDIT, REVERSAL →  +amount   (money in)
First event for an account: insert silver.current_balance + delta
Subsequent events:           t.current_balance + delta
```
