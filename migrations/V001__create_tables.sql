-- V001: initial medallion tables.
-- Names are unqualified: the migration runner sets USE CATALOG / USE SCHEMA for
-- the target, so the same script applies to dev, test and prod.
-- Every statement is IF NOT EXISTS, so re-running is a no-op.

-- Static dimension: one row per truck (seeded by 01_seed_truck_details).
CREATE TABLE IF NOT EXISTS truck_details (
  truck_id      STRING NOT NULL,
  make          STRING,
  model         STRING,
  capacity_lbs  INT,
  home_depot    STRING,
  region        STRING,
  driver        STRING,
  _updated_at   TIMESTAMP
)
COMMENT 'Static truck reference data (dimension for the Gold join).';

-- Bronze: raw pings exactly as landed. Payload columns stay STRING (Auto Loader
-- infers JSON as strings); casting happens in Silver.
CREATE TABLE IF NOT EXISTS bronze_pings (
  truck_id       STRING,
  latitude       STRING,
  longitude      STRING,
  event_ts       STRING,
  _rescued_data  STRING,
  _source_file   STRING,
  _ingested_at   TIMESTAMP
)
COMMENT 'Bronze: append-only raw GPS pings from the landing volume.';

-- Silver: typed, validated, deduplicated on (truck_id, event_ts).
CREATE TABLE IF NOT EXISTS silver_pings (
  truck_id       STRING NOT NULL,
  latitude       DOUBLE NOT NULL,
  longitude      DOUBLE NOT NULL,
  event_ts       TIMESTAMP NOT NULL,
  _source_file   STRING,
  _ingested_at   TIMESTAMP,
  _processed_at  TIMESTAMP
)
COMMENT 'Silver: clean, typed, deduplicated pings (key: truck_id + event_ts).';

-- Quarantine: rows that failed Silver validation, kept with the reason.
CREATE TABLE IF NOT EXISTS silver_pings_quarantine (
  truck_id         STRING,
  latitude         STRING,
  longitude        STRING,
  event_ts         STRING,
  _reason          STRING,
  _source_file     STRING,
  _ingested_at     TIMESTAMP,
  _quarantined_at  TIMESTAMP
)
COMMENT 'Silver rejects: invalid coordinates / timestamps / truck_id, with reason.';

-- Gold: latest known position per truck, enriched with truck details.
CREATE TABLE IF NOT EXISTS gold_truck_current_position (
  truck_id       STRING NOT NULL,
  latitude       DOUBLE,
  longitude      DOUBLE,
  event_ts       TIMESTAMP,
  make           STRING,
  model          STRING,
  capacity_lbs   INT,
  home_depot     STRING,
  region         STRING,
  driver         STRING,
  _updated_at    TIMESTAMP
)
COMMENT 'Gold: current position per truck joined with truck/driver/depot details.';
