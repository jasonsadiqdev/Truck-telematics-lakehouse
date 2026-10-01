# Databricks notebook source
# MAGIC %md
# MAGIC # Seed truck_details
# MAGIC The appendix B seed, made idempotent: the table is created by migrations,
# MAGIC and rows are upserted with MERGE (not overwrite) so re-runs change nothing.

# COMMAND ----------

import random

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

random.seed(42)  # deterministic: same 20 trucks every run, in every target

MAKES = [("Freightliner", "Cascadia"), ("Volvo", "VNL"), ("Kenworth", "T680"), ("Peterbilt", "579")]
DEPOTS = [("Chicago", "Midwest"), ("Dallas", "South"), ("Denver", "West"), ("Atlanta", "Southeast")]
DRIVERS = ["A. Rivera", "B. Chen", "C. Okafor", "D. Patel", "E. Nguyen", "F. Santos",
           "G. Kim", "H. Brooks", "I. Novak", "J. Alvarez"]

rows = []
for i in range(1, 21):
    make, model = random.choice(MAKES)
    depot, region = random.choice(DEPOTS)
    rows.append((f"TRK-{i:03d}", make, model, random.choice([20000, 26000, 34000, 40000]),
                 depot, region, random.choice(DRIVERS)))

df = spark.createDataFrame(
    rows,
    "truck_id STRING, make STRING, model STRING, capacity_lbs INT, "
    "home_depot STRING, region STRING, driver STRING",
)
df.createOrReplaceTempView("truck_details_seed")

# COMMAND ----------

# Only touch rows that are new or actually changed, so _updated_at stays stable.
spark.sql(f"""
  MERGE INTO `{catalog}`.`{schema}`.truck_details AS t
  USING truck_details_seed AS s
  ON t.truck_id = s.truck_id
  WHEN MATCHED AND NOT (
       t.make <=> s.make AND t.model <=> s.model AND t.capacity_lbs <=> s.capacity_lbs
   AND t.home_depot <=> s.home_depot AND t.region <=> s.region AND t.driver <=> s.driver)
    THEN UPDATE SET make = s.make, model = s.model, capacity_lbs = s.capacity_lbs,
                    home_depot = s.home_depot, region = s.region, driver = s.driver,
                    _updated_at = current_timestamp()
  WHEN NOT MATCHED
    THEN INSERT (truck_id, make, model, capacity_lbs, home_depot, region, driver, _updated_at)
         VALUES (s.truck_id, s.make, s.model, s.capacity_lbs, s.home_depot, s.region, s.driver,
                 current_timestamp())
""").show()

display(spark.table(f"`{catalog}`.`{schema}`.truck_details").orderBy("truck_id"))
