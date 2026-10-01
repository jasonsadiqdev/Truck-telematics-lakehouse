# Truck Telematics Lakehouse

A streaming pipeline on Databricks Free Edition. It ingests truck GPS pings, cleans
them, and joins them with static truck details to produce a business-ready Gold
table. Everything — schema, landing volume, job, tables and column changes — is
defined in one Databricks Asset Bundle and deployed from the CLI to `dev`, `test`
and `prod`.

```text
landing volume (JSON files)
   │  Auto Loader, availableNow
   ▼
bronze_pings                 raw strings + source file + ingest time (append-only)
   │  foreachBatch: try_cast, validate, dedupe (truck_id, event_ts), MERGE
   ├──────────────▶ silver_pings_quarantine     rejected rows + reason
   ▼
silver_pings                 typed, valid, one row per (truck_id, event_ts)
   │  stream–static join with truck_details (broadcast), MERGE latest per truck
   ▼
gold_truck_current_position  latest position + make/model/driver/depot/region + in_geofence
```

Design choices and trade-offs are in [DESIGN.md](DESIGN.md).

## Repository layout

| Path | What it is |
| --- | --- |
| `databricks.yml` | Bundle definition: variables and the `dev` / `test` / `prod` targets |
| `resources/storage.yml` | Per-target Unity Catalog schema and `landing` volume |
| `resources/telematics_job.yml` | The serverless job and its task graph |
| `migrations/V###__*.sql` | Versioned, idempotent table DDL (create tables, add columns) |
| `src/00_run_migrations.py` | Applies pending migrations, records them in `_schema_migrations` |
| `src/01_seed_truck_details.py` | Seeds the 20-truck dimension with an idempotent MERGE |
| `src/02_generate_pings.py` | Writes synthetic ping files (incl. a duplicate and a bad row per file) |
| `src/03_bronze.py` → `04_silver.py` → `05_gold.py` | The medallion streams |

Job task graph (one job per target):

```text
run_migrations ─┬─ seed_truck_details ─────────────────┐
                └─ generate_pings → bronze → silver ───┴─ gold
```

## Prerequisites

- A [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace.
- Databricks CLI (built and tested with v1.19.0):
  `brew tap databricks/tap && brew install databricks`
- Authenticate (OAuth opens a browser; a personal access token via `databricks configure` also works):

  ```bash
  databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile DEFAULT
  databricks current-user me      # sanity check
  ```

- The workspace host is set per target in `databricks.yml`. To use a different
  workspace, change the three `host:` lines.

## Deploy and run — in order

From the repo root. Each `deploy` creates or updates the target's schema, volume
and job; each `run` applies pending migrations, seeds, generates pings and runs
Bronze → Silver → Gold. Nothing needs to exist beforehand except the built-in
`workspace` catalog.

```bash
databricks bundle validate

# 1. dev   — schema workspace.dev, schedule paused (manual runs)
databricks bundle deploy -t dev
databricks bundle run telematics_pipeline -t dev

# 2. test  — schema workspace.test, scheduled hourly
databricks bundle deploy -t test
databricks bundle run telematics_pipeline -t test

# 3. prod  — schema workspace.prod, scheduled every 15 minutes
databricks bundle deploy -t prod
databricks bundle run telematics_pipeline -t prod
```

A run takes about 2–3 minutes on serverless.

### Pausing schedules (save Free Edition quota)

`test` and `prod` deploy with their schedules **unpaused**. To keep a target
deployed but not running on a schedule, override the variable at deploy time —
the bundle stays the source of truth, no UI edits:

```bash
databricks bundle deploy -t prod --var="schedule_pause_status=PAUSED"
```

A plain `databricks bundle deploy -t prod` turns the schedule back on.

## Verify

In the SQL Editor (swap `dev` for `test` / `prod`):

```sql
-- Applied migrations
SELECT * FROM workspace.dev._schema_migrations ORDER BY version;

-- Row counts per layer
SELECT
  (SELECT count(*) FROM workspace.dev.bronze_pings)                AS bronze,
  (SELECT count(*) FROM workspace.dev.silver_pings)                AS silver,
  (SELECT count(*) FROM workspace.dev.silver_pings_quarantine)     AS quarantined,
  (SELECT count(*) FROM workspace.dev.gold_truck_current_position) AS gold;

-- Silver has no duplicate keys (expect 0 rows)
SELECT truck_id, event_ts, count(*) FROM workspace.dev.silver_pings
GROUP BY ALL HAVING count(*) > 1;

-- Gold: current position per truck with truck/driver/depot details
SELECT truck_id, latitude, longitude, event_ts, in_geofence,
       make, model, driver, home_depot, region
FROM workspace.dev.gold_truck_current_position
ORDER BY truck_id;
```

## Changing a table (schema migrations)

Tables are never created or altered by hand, in any environment. To change one:

1. Add the next file, e.g. `migrations/V003__add_speed_kmh.sql`. Use unqualified
   table names (the runner sets the target's catalog and schema) and keep it
   re-runnable (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE … ADD COLUMNS`,
   backfills with a `WHERE … IS NULL` guard).
2. Update the code that writes the table in the same commit.
3. Promote it: `bundle deploy` + `bundle run` on `dev`, then `test`, then `prod`.
   The job's first task applies it before any stream runs.

Never edit a migration that has been applied — the runner stores a checksum and
fails the run if an applied file changes. Fix forward with a new version.

`V002__add_geofence_flag.sql` is the worked example: it added `in_geofence` to
Gold and was promoted dev → test → prod purely through deploy + run.
