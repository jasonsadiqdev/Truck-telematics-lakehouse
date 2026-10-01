-- V002: add a geofence flag to Gold — is the truck inside the Chicago depot
-- yard box (lat 41.80..41.90, lon -87.70..-87.60)?
--
-- Safe to re-run: the ledger skips it once applied, and the runner treats
-- "column already exists" on ADD COLUMNS as done (Databricks SQL has no
-- ADD COLUMN IF NOT EXISTS). The backfill only touches rows still NULL.
--
-- The box is duplicated in src/05_gold.py, which sets the flag for new pings;
-- this backfill covers rows already in Gold so the column is never stale.

ALTER TABLE gold_truck_current_position
  ADD COLUMNS (in_geofence BOOLEAN COMMENT 'Inside the Chicago depot geofence (lat 41.80..41.90, lon -87.70..-87.60)');

UPDATE gold_truck_current_position
SET in_geofence = (latitude BETWEEN 41.80 AND 41.90 AND longitude BETWEEN -87.70 AND -87.60)
WHERE in_geofence IS NULL;
