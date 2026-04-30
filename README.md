# Caseware Incremental Sync — Take-Home

Incremental data-sharing service that reads customer / case changes from
Postgres, publishes them into lake-style JSONL outputs and consumer-facing
share artifacts, and maintains a per-table composite watermark checkpoint
that is safe under crashes and re-runs.

## Prerequisites

- Docker + Docker Compose (Postgres 16 runs in a container)
- Python 3.11+
- POSIX-y OS (Linux / macOS — the file lock and `fsync` paths assume POSIX)

## Quickstart

```bash
# 1. Start the dockerized Postgres (auto-applies db/init.sql on first boot)
make up
make seed-info        # sanity check: 30 customers, 200 cases

# 2. Install the service in editable mode
make install

# 3. Run the service (uvicorn on :8000)
make run

# 4. In another shell, run an ingest
make ingest           # POST /ingest?dry_run=false
make dry-ingest       # POST /ingest?dry_run=true

# 5. Validate incrementality
make changes          # apply db/changes.sql
make ingest           # see only the 17 new/updated rows in the manifest

# 6. Test
make test             # unit tests
make integration      # integration tests against the running DB
```

## Endpoint

```
POST /ingest?dry_run=true|false
```

Returns a JSON manifest. Example (real run, fresh DB):

```json
{
  "run_id": "1f5f1c43fe1e536000dfcf360cbb78fc",
  "started_at": "2026-04-29T10:00:00Z",
  "finished_at": "2026-04-29T10:00:01Z",
  "dry_run": false,
  "checkpoint_before": {
    "cases":     {"updated_at": "0001-01-01T00:00:00Z", "last_pk": 0},
    "customers": {"updated_at": "0001-01-01T00:00:00Z", "last_pk": 0}
  },
  "checkpoint_after": {
    "cases":     {"updated_at": "2026-03-30T16:48:00Z", "last_pk": 200},
    "customers": {"updated_at": "2026-03-30T00:00:00Z", "last_pk": 30}
  },
  "tables": [
    {
      "table": "customers",
      "delta_row_count": 30,
      "lake_paths": ["lake/customers/date=2026-03-01/data.jsonl", "..."],
      "share_path": "share/customers/changes.jsonl",
      "schema_fingerprint": "7bccf22a775dcf2b"
    },
    {
      "table": "cases",
      "delta_row_count": 200,
      "lake_paths": ["lake/cases/date=2026-03-01/data.jsonl", "..."],
      "share_path": "share/cases/changes.jsonl",
      "schema_fingerprint": "2d76125b0e63dbbb"
    }
  ]
}
```

## Output examples

### Lake (`./lake/<table>/date=YYYY-MM-DD/data.jsonl`)

Append-only CDC change log. One JSONL record per row in the delta, in
canonical (sorted-key) form. Re-runs with no changes append nothing.

```jsonl
{"country":"US","customer_id":31,"email":"customer031@example.com","name":"Customer 031","updated_at":"2026-04-15T12:00:00Z"}
{"country":"CA","customer_id":32,"email":"customer032@example.com","name":"Customer 032","updated_at":"2026-04-15T12:00:00Z"}
```

### Share (`./share/<table>/changes.jsonl`)

Latest successful incremental batch — *replaced* on each non-zero-delta
run. Byte-for-byte deterministic for a given (source state, checkpoint).

```jsonl
{"checkpoint_after":{"cases":{...},"customers":{...}},"customer_id":31,"op":"upsert","record":{"country":"US","customer_id":31,"email":"...","name":"Customer 031","updated_at":"2026-04-15T12:00:00Z"},"run_id":"...","schema_fingerprint":"7bccf22a775dcf2b","table":"customers","updated_at":"2026-04-15T12:00:00Z"}
```

### Events (`./events/<run_id>.jsonl`)

One line per table per run (zero-delta tables included).

```jsonl
{"checkpoint_after":{...},"delta_row_count":15,"lake_paths":["lake/cases/date=2026-04-15/data.jsonl"],"run_id":"...","schema_fingerprint":"2d76125b0e63dbbb","share_path":"share/cases/changes.jsonl","table":"cases"}
{"checkpoint_after":{...},"delta_row_count":2,"lake_paths":["lake/customers/date=2026-04-15/data.jsonl"],"run_id":"...","schema_fingerprint":"7bccf22a775dcf2b","share_path":"share/customers/changes.jsonl","table":"customers"}
```

### Checkpoint (`./state/checkpoint.json`)

Human-readable, sorted, atomically replaced.

```json
{
  "cases":     {"last_pk": 210, "updated_at": "2026-04-15T12:00:00Z"},
  "customers": {"last_pk": 32,  "updated_at": "2026-04-15T12:00:00Z"}
}
```

## Architecture

```
api/        thin FastAPI route -> orchestrator
core/       config, clock, errors, logging
domain/     value types: TableSpec, Checkpoint, Manifest, Event, ShareRecord
source/     PgConnectionFactory + IncrementalSourceRepository (composite predicate)
state/      CheckpointStore (atomic load/stage/commit)
outputs/    LakeWriter, ShareWriter, EventWriter (write into staging dir only)
pipeline/   run_id, fingerprint, lock, staging, commit, orchestrator
utils/      jsonio (canonical serialization), fs (atomic_replace, fsync)
```

The orchestrator is the only module that knows the full shape of an
ingest run; everything else has a single, narrow responsibility behind a
testable interface.

## Composite watermark contract

Per-table watermark stored as `(updated_at, last_pk)`. Each table's delta
query is exactly:

```sql
SELECT <columns> FROM <table>
WHERE updated_at > :ckpt_updated_at
   OR (updated_at = :ckpt_updated_at AND <pk> > :ckpt_last_pk)
ORDER BY updated_at ASC, <pk> ASC
```

A composite watermark — rather than `updated_at` alone — is what
prevents skipping rows that share a timestamp with the prior run's
last row. The seed deliberately places case_id `i` and `i+100` at the
same `updated_at` for `i in 1..100` to exercise this property in tests.

If `./state/checkpoint.json` is missing, the watermark defaults to
`(0001-01-01T00:00:00Z, 0)` so the predicate is uniform on first run.

## Crash-consistency strategy

Every run is a tiny journaled transaction with the staging dir
`./state/runs/<run_id>/` as the journal:

1. **Stage.** Compute deltas, derive deterministic `run_id`, write every
   final-form file (lake partitions, share files, events file, new
   checkpoint) into the run dir. Lake "appends" are implemented as
   *read-existing-bytes + concat-new + stage as full file* — atomic
   rename then gives idempotent replace semantics with no in-place
   torn-write risk.
2. **Plan + READY.** Write `commit_plan.json` (the ordered list of
   `staged → live` renames), then atomically write the `READY` marker.
   The marker's existence is the gate that resume uses.
3. **Commit.** Execute the plan top-to-bottom: lake renames → share
   renames → events rename → **checkpoint rename (the actual commit
   point)**. Each step is `os.replace` + parent-dir fsync. Cleanup
   removes the run dir last.

On startup and at the start of every `/ingest` call, `resume_pending`
sweeps `./state/runs/`:
- run dir with `READY` → replay the commit plan; missing staged files
  signal "this rename already happened, skip".
- run dir without `READY` → discard (it was an abandoned partial stage).

The single invariant this preserves: **the checkpoint never advances
ahead of durable outputs**. A crash before any rename leaves the
checkpoint untouched and the lake/share/events untouched. A crash
between renames is recoverable because the staged content is
deterministic from `(checkpoint_before, source_state)` and renames are
idempotent.

## Replay & idempotency expectations for consumers

- **Re-running `/ingest` with no source changes** writes nothing. Lake
  files are unmodified, share files are unmodified, the checkpoint is
  unchanged. Only the events file is re-emitted (with byte-identical
  content, because the run_id is deterministic from inputs and the
  inputs are unchanged).
- **`run_id` is a function of inputs.** Same `(checkpoint_before,
  ordered row identities)` → same `run_id`. Consumers can dedupe by
  `run_id` if they replay events.
- **The share artifact is the latest batch, not a log.** A consumer
  that polls `./share/<table>/changes.jsonl` and tracks the contained
  `checkpoint_after` knows exactly which slice of history they have
  pulled. Two reads of the same artifact are byte-identical.
- **The lake is append-only CDC.** A consumer can range-scan partitions
  by date and rely on rows being ordered by `(updated_at, pk)` within
  each file. New rows are only ever appended after existing bytes.

## Schema evolution in production

A real version would extend this with:

- **Column adds** — non-breaking. Bump `schema_fingerprint`; consumers
  reading by fingerprint reload their parser.
- **Column drops / renames** — breaking. Roll out a *new* table version
  in parallel (e.g., `customers_v2`) and run dual-publish for a
  deprecation window; consumers cut over by reading the new fingerprint.
- **Type changes** — handle as drop+add semantically; never edit a
  column's type in place from the consumer's perspective.
- **Open table format** — for the production AWS variant we'd switch
  the lake to Apache Iceberg with explicit schema versioning, time
  travel, and snapshot isolation, and emit schema change events on
  every fingerprint transition.

## Important assumptions / simplifications

- **Source consistency** — per spec, no concurrent source writes during
  an ingest run. We additionally use a `REPEATABLE READ READ ONLY`
  transaction for both per-run table reads as defense-in-depth.
- **Single-instance** — `fcntl.flock` on `./state/.ingest.lock` makes
  `/ingest` single-flight per process. A second concurrent call gets
  HTTP 409. Multi-instance deployment is out of scope.
- **POSIX** — `os.replace` for atomic same-filesystem renames; `fsync`
  on parent directories for durability. Windows works but the
  durability guarantee is weaker (no dir fsync API).
- **Op = upsert** — the source contract has only inserts and updates,
  so the share record's `op` field is always `"upsert"`. `delete` is
  not modeled.
- **Lake partition writes are O(partition size)** — every "append" is
  rewriting the whole partition file via stage+rename. At scale this
  trades CPU for crash safety; the production design uses a different
  storage primitive (Iceberg, S3 multipart) where this isn't necessary.

## Layout

```
db/
  init.sql               # schema + deterministic seed (30 customers, 200 cases)
  changes.sql            # 5 case updates + 2 customers + 10 cases for incremental test
src/caseware_sync/...    # the service (see Architecture above)
tests/
  unit/                  # determinism, atomicity, crash-resume scenarios
  integration/           # end-to-end against the dockerized DB
docker-compose.yml       # postgres:16 only
Makefile                 # up / down / install / run / ingest / changes / test / ...
```
