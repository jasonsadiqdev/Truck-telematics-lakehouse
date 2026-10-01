# Databricks notebook source
# MAGIC %md
# MAGIC # Generate synthetic GPS pings
# MAGIC The appendix A generator, parameterised per target. It writes newline-
# MAGIC delimited JSON files into the target's landing volume (which the bundle
# MAGIC creates — this notebook no longer creates catalogs/schemas/volumes).
# MAGIC Each file deliberately includes one duplicate ping and one null-latitude
# MAGIC row so Silver's dedup and quarantine paths are exercised.

# COMMAND ----------

import json
import random
import time
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dev")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("num_batches", "10")
dbutils.widgets.text("sleep_seconds", "1")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
num_batches = int(dbutils.widgets.get("num_batches"))
sleep_seconds = float(dbutils.widgets.get("sleep_seconds"))

LANDING = f"/Volumes/{catalog}/{schema}/{volume}/pings"
dbutils.fs.mkdirs(LANDING)

TRUCKS = [f"TRK-{i:03d}" for i in range(1, 21)]  # 20 trucks
LAT0, LON0 = 41.85, -87.65                         # near Chicago


def make_ping(tid):
    return {
        "truck_id": tid,
        "latitude": round(LAT0 + random.uniform(-0.2, 0.2), 6),
        "longitude": round(LON0 + random.uniform(-0.2, 0.2), 6),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }

# COMMAND ----------

for batch in range(num_batches):
    rows = [make_ping(random.choice(TRUCKS)) for _ in range(random.randint(5, 15))]
    rows.append(dict(rows[0]))                                           # duplicate
    rows.append({**make_ping(random.choice(TRUCKS)), "latitude": None})  # bad row
    fname = f"{LANDING}/pings_{int(time.time() * 1000)}_{batch}.json"
    with open(fname, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {fname} ({len(rows)} rows)")
    time.sleep(sleep_seconds)

print(f"done: {num_batches} files in {LANDING}")
