# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: bronze_pings → silver_pings (+ quarantine)
# MAGIC Per micro-batch (`foreachBatch`):
# MAGIC 1. Cast types with `try_cast` (bad values become NULL instead of failing).
# MAGIC 2. Route rows with a missing truck_id, bad timestamp, or null / out-of-range
# MAGIC    coordinates to `silver_pings_quarantine`, with the reason.
# MAGIC 3. Dedupe valid rows on the natural key **(truck_id, event_ts)** inside the
# MAGIC    batch, then insert-only MERGE into `silver_pings` — which also drops
# MAGIC    duplicates that arrive in later batches.
# MAGIC
# MAGIC Both writes are insert-only MERGEs, so replaying a batch (retry, restart,
# MAGIC even a reset checkpoint) never creates duplicates.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dev")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

SOURCE = f"`{catalog}`.`{schema}`.bronze_pings"
SILVER = f"`{catalog}`.`{schema}`.silver_pings"
QUARANTINE = f"`{catalog}`.`{schema}`.silver_pings_quarantine"
CHECKPOINT = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/silver_pings"

# COMMAND ----------

def upsert_silver(batch_df, batch_id):
    session = batch_df.sparkSession

    typed = batch_df.select(
        F.trim("truck_id").alias("truck_id_clean"),
        F.expr("try_cast(latitude AS DOUBLE)").alias("lat"),
        F.expr("try_cast(longitude AS DOUBLE)").alias("lon"),
        F.expr("try_cast(event_ts AS TIMESTAMP)").alias("ts"),
        "*",
    )

    reason = (
        F.when(F.col("truck_id_clean").isNull() | (F.col("truck_id_clean") == ""), "missing truck_id")
        .when(F.col("ts").isNull(), "missing or invalid event_ts")
        .when(F.col("lat").isNull(), "missing or invalid latitude")
        .when(F.col("lon").isNull(), "missing or invalid longitude")
        .when(~F.col("lat").between(-90, 90), "latitude out of range")
        .when(~F.col("lon").between(-180, 180), "longitude out of range")
    )
    checked = typed.withColumn("_reason", reason)

    # Valid rows: dedupe within the batch, then insert only keys not yet in Silver.
    (checked.filter(F.col("_reason").isNull())
        .select(
            F.col("truck_id_clean").alias("truck_id"),
            F.col("lat").alias("latitude"),
            F.col("lon").alias("longitude"),
            F.col("ts").alias("event_ts"),
            "_source_file",
            "_ingested_at",
            F.current_timestamp().alias("_processed_at"),
        )
        .dropDuplicates(["truck_id", "event_ts"])
        .createOrReplaceTempView("silver_batch"))

    session.sql(f"""
        MERGE INTO {SILVER} AS t
        USING silver_batch AS s
        ON t.truck_id = s.truck_id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Invalid rows: keep the raw strings plus the reason. Null-safe match on the
    # raw row + source file makes replays a no-op here too.
    (checked.filter(F.col("_reason").isNotNull())
        .select(
            "truck_id", "latitude", "longitude", "event_ts", "_reason",
            "_source_file", "_ingested_at",
            F.current_timestamp().alias("_quarantined_at"),
        )
        .dropDuplicates(["truck_id", "latitude", "longitude", "event_ts", "_source_file"])
        .createOrReplaceTempView("quarantine_batch"))

    session.sql(f"""
        MERGE INTO {QUARANTINE} AS t
        USING quarantine_batch AS s
        ON  t._source_file <=> s._source_file
        AND t.truck_id     <=> s.truck_id
        AND t.latitude     <=> s.latitude
        AND t.longitude    <=> s.longitude
        AND t.event_ts     <=> s.event_ts
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

query = (
    spark.readStream.table(SOURCE)
    .writeStream
    .foreachBatch(upsert_silver)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print(f"silver_pings rows: {spark.table(SILVER).count()}, "
      f"quarantined: {spark.table(QUARANTINE).count()}")
