# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: landing JSON → bronze_pings (Auto Loader)
# MAGIC Append-only and close to raw: payload columns stay STRING, plus the source
# MAGIC file and ingest time. Auto Loader keeps its inferred schema in a schema
# MAGIC location; `rescue` mode sends unexpected fields to `_rescued_data` rather
# MAGIC than changing the table, so table structure only changes via migrations.
# MAGIC `availableNow` processes everything new, then stops; the checkpoint makes
# MAGIC each file ingested exactly once across stops/restarts.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dev")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

VOLUME_ROOT = f"/Volumes/{catalog}/{schema}/{volume}"
SOURCE = f"{VOLUME_ROOT}/pings"
SCHEMA_LOCATION = f"{VOLUME_ROOT}/_schemas/bronze_pings"
CHECKPOINT = f"{VOLUME_ROOT}/_checkpoints/bronze_pings"
TARGET = f"`{catalog}`.`{schema}`.bronze_pings"

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaHints",
            "truck_id STRING, latitude STRING, longitude STRING, event_ts STRING")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(SOURCE)
)

bronze = raw.select(
    "truck_id",
    "latitude",
    "longitude",
    "event_ts",
    "_rescued_data",
    F.col("_metadata.file_path").alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

query = (
    bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()

print(f"bronze_pings rows: {spark.table(TARGET).count()}")
