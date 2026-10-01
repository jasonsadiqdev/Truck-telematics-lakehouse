# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: silver_pings ⋈ truck_details → gold_truck_current_position
# MAGIC **Stream–static join.** The streaming side is `silver_pings`; the static
# MAGIC side is the `truck_details` Delta table. Spark re-resolves a Delta static
# MAGIC side to its latest snapshot on every micro-batch, so dimension changes
# MAGIC (e.g. a driver reassignment) are picked up without restarting the stream.
# MAGIC The dimension is tiny, so it's broadcast — no shuffle of the ping stream.
# MAGIC
# MAGIC Per micro-batch: keep each truck's latest ping, then MERGE into Gold only
# MAGIC when it's newer than what Gold already has. Late or replayed pings can
# MAGIC never move a truck backwards, so the write is idempotent.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dev")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

SILVER = f"`{catalog}`.`{schema}`.silver_pings"
TRUCKS = f"`{catalog}`.`{schema}`.truck_details"
GOLD = f"`{catalog}`.`{schema}`.gold_truck_current_position"
CHECKPOINT = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/gold_truck_current_position"

# COMMAND ----------

pings = spark.readStream.table(SILVER).select("truck_id", "latitude", "longitude", "event_ts")
trucks = spark.read.table(TRUCKS).drop("_updated_at")

# Left join: a ping from a truck missing in truck_details still shows up in
# Gold (with NULL details) instead of silently disappearing.
enriched = pings.join(F.broadcast(trucks), "truck_id", "left")


def upsert_gold(batch_df, batch_id):
    latest = (
        batch_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("truck_id").orderBy(F.col("event_ts").desc())))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("_updated_at", F.current_timestamp())
    )
    latest.createOrReplaceTempView("gold_batch")

    batch_df.sparkSession.sql(f"""
        MERGE INTO {GOLD} AS t
        USING gold_batch AS s
        ON t.truck_id = s.truck_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

query = (
    enriched.writeStream
    .foreachBatch(upsert_gold)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

display(spark.table(GOLD).orderBy("truck_id"))
