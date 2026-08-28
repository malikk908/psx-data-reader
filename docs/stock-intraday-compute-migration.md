# Stock Intraday Compute Migration

## Status

| Phase | Status | Scope |
| --- | --- | --- |
| 0 | Complete | Architecture, ownership, contracts, and release gates documented here. |
| 1 | Complete | Durable Python candle materialization, reconciliation, retention, generation state, and parity tests. |
| 2 | Complete | Python intraday technical-snapshot parity engine, writer contract, and deterministic parity tests. |
| 3 | Pending | Dedicated coalescing compute worker, operational controls, and benchmarks. |
| 4 | Pending | Direct production cutover and removal of backend stock intraday jobs. |

## Purpose

Move stock intraday computation from the FinHisaab Node backend to this
repository, where `python -m psx.market_watch_poller` already owns the raw
market-watch ingest. The target is faster materialization without adding
compute or BullMQ fan-out load to the API server.

The migration is intentionally stock-only. It must preserve the existing
MongoDB collections and API response contracts so backend readers remain
read-only consumers.

## Scope

### In scope

- Materialize synthetic stock candles from `intraday_klines_temp` into
  `intraday_klines` for `1m`, `5m`, `15m`, `30m`, `1h`, and `4h`.
- Maintain candle lifecycle and freshness fields: `isForming`, `barCloseAt`,
  and true-change `updatedAt`.
- Compute and persist raw-price intraday stock technical snapshots into
  `stock_technical_snapshots` for the six intraday timeframes.
- Run a canonical end-of-day reconciliation and retention cleanup in this
  repository.
- Replace backend stock intraday rollup and technical-compute scheduling after
  direct cutover.

### Out of scope

- Index intraday ingestion and index technicals. Those use the independent
  `psxterminal` source and remain backend-owned.
- Market breadth, sector pulse, market history, price alerts, and API serving.
- Daily stock technicals, including bonus/split-adjusted daily candles.
- New indicators, screener fields, warmup metadata, and API contract changes.
- Replacing the existing backend models, chart readers, or technical detail
  endpoint.

## Current Ownership

| Concern | Current owner | Target owner |
| --- | --- | --- |
| Market-watch fetch and raw snapshots | `psx-data-reader` poller | `psx-data-reader` poller |
| Synthetic intraday candle rollup | Backend BullMQ job every 5 min | `psx-data-reader` compute worker |
| Stock intraday technical snapshots | Backend BullMQ fan-out, one job per changed pair | `psx-data-reader` compute worker |
| EOD candle reconciliation and pruning | Backend rollup job | `psx-data-reader` compute worker |
| Raw snapshot retention | Backend cleanup job | `psx-data-reader` compute worker |
| Daily stock technical snapshots | Backend nightly job | Backend nightly job |
| Intraday chart/API reads | Backend | Backend |

## Existing Data Contracts

### Raw market-watch snapshots

Collection: `intraday_klines_temp` in the company-data database.

- Unique key: `{ symbol, scraped_at_minute }`.
- A poll cycle fetches roughly 500 symbols in one request and bulk-upserts the
  result.
- More than one poll in the same UTC minute updates the same document; a raw
  snapshot is mutable until its minute has passed.
- `price` is the current visible price, while `open`, `high`, `low`, and
  `volume` are session-level values.
- `volume_delta` is the trusted change in cumulative volume relative to the
  prior retained snapshot. It can be null.

### Durable candles

Collection: `intraday_klines` in the company-data database.

Unique key: `{ symbol, timeframe, timestamp }`.

- One-minute candles are synthetic, not exchange-native OHLCV.
- Their timestamps are back-dated by the PSX feed delay.
- Higher timeframes use UTC-epoch bucket alignment:
  `floor(timestamp / bucket_ms) * bucket_ms`.
- Higher bars are derived only from persisted one-minute bars, scoped to one
  PKT trading date.
- `updatedAt` changes only when OHLCV changes. Lifecycle-only settling must not
  change it.
- `isForming` and `barCloseAt` are computed against `dataAsOf = wall_clock -
  PSX_FEED_DELAY_MS`, because the source itself is delayed.

### Intraday technical snapshots

Collection: `stock_technical_snapshots` in the primary database.

Unique key: `{ symbol, timeframe }`.

- Valid intraday timeframes: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`.
- Source is raw-price `IntradayKline`, with no corporate-action adjustment.
- Lookbacks: `1m`/`5m` = 500, `15m`/`30m` = 400, `1h`/`4h` = 300.
- Snapshot metadata must include source timeframe, candle count, first/last
  candle date, newest candle `updatedAt`, and newest-bar lifecycle fields.
- The Python writer must preserve the existing BSON field names, nullable
  values, rounding, and nested object shape exactly.

## Target Architecture

### Processes

1. `python -m psx.market_watch_poller`
   - Continues to fetch and durably write raw snapshots.
   - Does not calculate candles or indicators inline.
   - Advances a compact durable generation after each completed write batch.

2. `python -m psx.intraday_compute_worker`
   - Runs as a separate long-lived service in this repository.
   - Holds the only lease for stock intraday materialization.
   - Coalesces generations: if ingest advances while it works, it processes the
     newest durable state rather than queueing every intermediate poll.
   - Owns incremental materialization, technical computation, EOD repair, and
     retention cleanup.

The poller cadence is currently randomized between 90 and 150 seconds. The
worker must never delay a new poll. A slow worker is observable as materializer
lag, not as a blocked fetch loop.

### Queue decision

**Decision: do not add a Redis job queue for phases 1-3.** The dedicated
compute worker is asynchronous relative to the poller, but MongoDB pipeline
state is its durable, coalescing work signal.

| Option | Benefits | Decision |
| --- | --- | --- |
| Per-symbol/per-timeframe Redis jobs | Familiar retry and visibility model | Rejected. Recreates the backend's up-to-3,000-job fan-out and permits backlog growth. |
| One Redis job per poll | Fast worker wake-up and Redis observability | Deferred. Adds a third availability dependency without improving correctness; a missed or duplicate job still requires durable state recovery. |
| Mongo generation plus dedicated worker | Natural coalescing, restart recovery, no new service dependency, bounded work | Chosen. The worker polls a tiny state document and always derives from durable raw data. |

The worker must poll pipeline state frequently enough that this does not add
meaningful latency compared with the 90-150 second ingest cadence. Mongo state
is authoritative because it survives Redis loss, process restarts, duplicate
wake-ups, and partial raw-batch writes.

Redis can be reconsidered only if measured state polling is a material source
of latency or this repository later gains several independent asynchronous
workloads. If added, it is a best-effort **wake-up** transport for one
coalesced generation task, never the task ledger: the worker must still read
Mongo state, claim the lease, and process the newest generation. Do not enqueue
symbol/timeframe pairs.

### Durable coordination

The company-data database will hold one small pipeline-state document with:

- monotonically increasing `generation` written by the poller after raw writes;
- latest raw scrape timestamp and write timestamp;
- compute worker's last completed generation;
- a lease owner and bounded lease expiry;
- last successful incremental and EOD reconciliation timestamps;
- failure metadata for operational diagnosis.

The state document is not a source of candle truth. It is a wake-up and
recovery signal. On restart or uncertain partial writes, the worker rebuilds a
safe rolling raw tail and conditional writes make repeats idempotent.

### Incremental pipeline

For each observed generation, the compute worker:

1. Reads a bounded raw-snapshot tail plus a short seed lookback for every
   touched symbol.
2. Rebuilds eligible one-minute candles, conditionally persists changed OHLCV,
   and settles lifecycle-only rows once.
3. Uses the earliest changed one-minute timestamp per symbol to read every
   affected higher-timeframe bucket from its own bucket floor.
4. Conditionally persists changed higher candles and produces the compact
   `{ timeframe: [symbols] }` change signal.
5. Computes technical snapshots only for changed symbol/timeframe pairs.
6. Marks the generation complete only after all stages finish successfully.

The worker can retry a failed generation by rebuilding the tail. It must not
rely on per-symbol events or in-memory state for correctness.

### EOD reconciliation and retention

After the buffered official-close checkpoint, the worker performs a full
trading-date rebuild before pruning.

- Raw snapshots retain the newest seven PKT trading dates.
- Candle retention remains per timeframe: `1m` 15, `5m` 30, `15m` 90, `30m`
  180, `1h` 365, and `4h` 730 trading days.
- Pruning begins only after each timeframe has enough distinct trading dates.
- EOD reconciliation is the repair path for worker downtime, dropped
  generations, raw same-minute corrections, and incremental implementation
  defects.

## Python Technical Engine Parity

The backend's technical engine is a shared JavaScript implementation backed by
`technicalindicators`. It calculates more than thirty indicator families plus
ratings, pivots, and derived fields. This is not a trigger-only migration.

The Python implementation must:

- port formula and warmup semantics deliberately rather than assume a Python
  indicator library has equivalent initialization behavior;
- preserve four-decimal `roundTo` semantics and null behavior;
- preserve rating inputs, labels, counts, pivots, OHLC/volume conventions, and
  source metadata;
- use raw intraday prices only; daily bonus-adjusted logic stays in the backend;
- batch Mongo reads by timeframe where safe, while bounding CPU and write
  concurrency;
- write only the existing snapshot document contract.

No production cutover is allowed until fixture parity proves this engine is
compatible with the current Node result.

## Testing and Release Gates

### Candle parity fixtures

Frozen raw-snapshot fixtures must cover:

- opening bar and delayed timestamp assignment;
- same-minute raw overwrite;
- missing snapshots and seed chaining;
- volume-delta null/negative cases;
- session-high/session-low changes;
- UTC bucket boundaries, including 4-hour boundaries;
- forming-to-closed lifecycle settling;
- repeated idempotent runs and EOD convergence.

Expected documents are generated from the current backend implementation before
it is retired.

### Technical parity fixtures

Fixtures must compare every persisted snapshot field for representative series:

- insufficient history and full warmup;
- forming-bar revision;
- trending, ranging, zero-volume, and gap-like candle series;
- all six timeframes;
- null propagation, four-decimal values, ratings, and pivots.

Numeric tolerance is only permitted where a documented library-level floating
point difference remains after rounding. Ratings, nulls, labels, counts, dates,
and source metadata must match exactly.

### Performance gates

Before production cutover, benchmark a representative approximately 500-symbol
market update and record:

- poll-cycle duration with the compute worker active;
- materializer end-to-end lag from raw write to candle write;
- technical snapshot completion lag;
- worker generation backlog under sustained updates;
- Mongo reads/writes and process memory.

The compute worker must converge within the minimum poll interval under normal
market load. If it cannot, optimize batching or reduce worker concurrency only
after parity is preserved; do not reintroduce backend fan-out as a shortcut.

## Direct Cutover Runbook

1. Apply and verify the existing primary-database `{ symbol, timeframe }`
   snapshot index migration in every target environment.
2. Provision least-privilege reader credentials for both company-data and
   primary databases. The backend URI names are not copied verbatim; the reader
   owns separate explicit configuration.
3. Deploy the poller and compute worker images, but verify connectivity,
   indexes, fixture parity, and benchmark gates before enabling continuous
   compute ownership.
4. Drain backend `intraday-kline-rollup-cron` and
   `stock-technicals-intraday` work. Disable their repeatable schedules first
   so no new work arrives.
5. Run the reader's canonical reconciliation over current retained data, then
   start continuous reader ownership.
6. Verify raw generation advancement, candle freshness/lifecycle, snapshot
   freshness/source metadata, document counts, and the unchanged backend API
   response.
7. Remove backend rollup, stock intraday technical worker, queues, startup
   registrations, and environment settings in a separate backend commit.
8. Retain operational rollback instructions: stop reader compute, re-enable
   backend repeatables, and run the backend's canonical EOD repair. Never
   delete raw, candle, or snapshot collections as part of rollback.

This is a direct ownership cutover, not a production dual-write period. The
fixture and benchmark gates above are therefore mandatory before step 4.

## Implementation Phases

### Phase 1: Candle materialization substrate

- Add company-data connection/configuration and durable pipeline state.
- Port candle builders, lifecycle, conditional writes, higher-timeframe
  derivation, EOD reconciliation, and retention.
- Add golden candle fixtures and integration tests.
- Mark this phase complete here only after its commit passes the parity suite.

### Phase 2: Technical snapshot parity substrate

- Add primary-database configuration and writer contract.
- Port the technical engine and snapshot metadata.
- Add Node-generated golden technical fixtures and parity tests.
- Implemented in `src/psx/technical_snapshot.py` and
  `src/psx/intraday_technical_writer.py`. The writer reads `IntradayKline` from
  the company-data database and upserts only the existing
  `stock_technical_snapshots` document shape in the primary database.
- The golden fixture is generated from the backend's `technicalindicators`
  v3.1.0 implementation and covers every persisted indicator section. The
  engine reproduces that library's initialization/alignment rules directly so
  warmups remain nullable and do not depend on pandas defaults.
- Marked complete after all fixture fields match. Phase 3 still needs to wire
  this writer into the dedicated coalescing compute worker.

### Phase 3: Dedicated compute worker

- Add the coalescing worker entry point, lease handling, generation recovery,
  bounded compute concurrency, EOD scheduling, metrics, and benchmark tooling.
- Mark this phase complete only after sustained-load benchmark gates pass.

### Phase 4: Backend cutover

- Remove backend stock intraday scheduling and worker ownership while retaining
  read models and APIs.
- Execute the direct-cutover runbook and verify production behavior.
- Record the deployed configuration, verification results, and rollback owner
  in this document.
