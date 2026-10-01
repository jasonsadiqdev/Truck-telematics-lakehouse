# Design notes

## Structured Streaming vs Lakeflow

Hand-rolled Structured Streaming in notebooks run by one bundle job. The brief
asks for table structure to be managed as versioned, idempotent scripts.
Lakeflow pipelines own their tables' DDL, so an explicit migration step would
fight the framework; with plain streams, migrations, MERGE logic and checkpoints
are all explicit and easy to reason about. The cost is more code than a Lakeflow
pipeline with expectations, and no built-in lineage or data-quality UI.

## Triggers and latency

Serverless only supports `availableNow` triggers, so each layer processes
everything new and stops, and the job schedule sets the latency: prod every
15 min, test hourly, dev manual. This is incremental batch, which uses no compute
between runs. For sub-minute latency I'd run the same code continuously with a
`processingTime` trigger on classic compute, or move it to a continuous Lakeflow
pipeline.

## Layers, dedup and idempotency

- **Bronze**: Auto Loader with a schema location. All payload columns stay
  STRING. `rescue` mode sends unexpected fields to `_rescued_data` instead of
  changing the table, so table structure only changes through migrations.
- **Silver**: `foreachBatch` casts with `try_cast` (bad values become NULL
  instead of failing). Rows with a missing truck_id, timestamp or coordinate, or
  out-of-range coordinates, go to `silver_pings_quarantine` with a reason. The
  dedup key is **(truck_id, event_ts)**: a truck can't be in two places at the
  same instant, and the generator's duplicates are exact copies. Rows are
  deduped within the batch and then insert-only MERGEd, which also drops
  duplicates that arrive in later batches. With a real device feed I'd prefer a
  device-assigned sequence or ping id.
- **Every write is idempotent**: Silver and quarantine are insert-only MERGEs, and
  Gold updates a truck only when `s.event_ts > t.event_ts`. Replaying a batch
  (after a retry, a restart, or even a lost checkpoint) is a no-op, so
  at-least-once delivery gives exactly-once results.

## Stream–static join (Gold)

`silver_pings` (stream) is left-joined to `truck_details` (static Delta table).
Spark re-resolves a static Delta table to its latest version on every
micro-batch, so dimension changes are picked up without restarting the stream.
The dimension is small, so it's broadcast and the ping stream is never shuffled.
A left join keeps pings from unknown trucks visible with NULL details instead of
dropping them. Trade-off: Gold attributes refresh when a truck next pings, so a
driver reassignment shows on that truck's next ping. If attributes had to be
current immediately, I'd join at read time with a view.

## Checkpoints and restart

Each stream checkpoints to `/Volumes/<catalog>/<schema>/landing/_checkpoints/<table>`,
and Auto Loader's schema lives in `…/_schemas/`. Because these sit in the
target's own volume, environments never share state. On restart, Auto Loader
skips files it has already ingested, and Silver and Gold resume from the last
committed offset. This was verified by re-running the job: only new files were
read, and Silver still had 0 duplicate keys.

## dev / test / prod

One workspace and one bundle; the three targets differ only in configuration:

| | dev | test | prod |
| --- | --- | --- | --- |
| Schema / volume | `workspace.dev` | `workspace.test` | `workspace.prod` |
| Job | `telematics-pipeline-dev` | `…-test` | `…-prod` |
| Schedule | paused (manual) | hourly | every 15 min |
| Bundle root | `.bundle/…/dev` | `.bundle/…/test` | `.bundle/…/prod` (`mode: production`) |

Free Edition can't create catalogs through the API (the metastore has no storage
root), so, as the brief allows, all targets use the built-in `workspace` catalog.
On a full account a separate platform bundle would own a `telematics` catalog.
`dev` doesn't use `mode: development` because that mode would rename the schema
to `dev_<user>_dev`. Its useful parts (paused schedules, tags) are set through
`presets` instead.

## Table changes as code; prod is deploy-only

- DDL lives in `migrations/V###__name.sql`. The job's first task applies pending
  versions in order and records each one in `_schema_migrations` with a SHA-256
  checksum. Because schema and code ship in the same deploy and the migration
  task runs before any stream, a target never runs new code against an old
  schema.
- **Safe to re-run**: applied versions are skipped, and statements use
  `IF NOT EXISTS`. Databricks SQL has no `ADD COLUMN IF NOT EXISTS`, so the runner
  treats "column already exists" on an `ADD COLUMNS` as already done (this covers
  a run that dies between the ALTER and the ledger insert). Backfills are guarded
  with `WHERE … IS NULL`.
- **Immutability**: if an applied file is edited, its checksum no longer matches
  and the run fails. Changes go forward as new versions.
- **Promotion**: `V002` (adds `in_geofence` to Gold and backfills it) went
  dev → test → prod through `bundle deploy` + `bundle run` only. Delta table
  history shows the ALTER and the UPDATE were made by the job, not by a person.
- **Rollback** is forward-only: deploy the previous git commit of the code and add
  a new migration that reverses the change. Dropping a column needs column
  mapping enabled first, so for an additive column the safest rollback is to
  leave it unused. For bad data, use `RESTORE TABLE … TO VERSION AS OF n`.
  Prefer expand/contract changes (add new, migrate readers, drop old later) so
  the previous code version keeps working.

## Scaling from 20 trucks to hundreds of thousands

- **Ingest**: switch Auto Loader to file-notification mode (instead of listing
  directories), or read from Kafka / Event Hubs. Run continuously on classic
  compute with `processingTime` triggers, and size micro-batches with
  `maxFilesPerTrigger` / `maxBytesPerTrigger`.
- **Silver dedup**: an insert-only MERGE against an ever-growing table gets
  expensive. Options: liquid-cluster Silver by `(truck_id, event_ts)` and add a
  time-bound predicate to the MERGE so it only scans recent files, or replace
  the MERGE with `dropDuplicatesWithinWatermark` plus append, which keeps dedup
  state bounded by the watermark.
- **Gold**: one row per truck (hundreds of thousands of rows) is still a cheap
  MERGE. Liquid-cluster it by `truck_id`. A dimension of a few hundred thousand
  narrow rows is still broadcastable (tens of MB); beyond that, switch to a
  regular join.
- **Operations**: one job per layer (or a Lakeflow pipeline) so layers scale and
  fail independently, streaming query listeners and alerts on lag, and
  service-principal `run_as` for prod.
