# Databricks notebook source
# MAGIC %md
# MAGIC # Run schema migrations
# MAGIC Applies `migrations/V###__*.sql` in version order to the target schema and
# MAGIC records each one in `_schema_migrations`. Already-applied versions are
# MAGIC skipped, so every deploy + run is safe to repeat. An applied migration
# MAGIC whose file has since changed fails the run: migrations are immutable —
# MAGIC fix forward with a new version instead.

# COMMAND ----------

import hashlib
import os
import re

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dev")
dbutils.widgets.text("migrations_dir", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
migrations_dir = dbutils.widgets.get("migrations_dir")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

spark.sql("""
  CREATE TABLE IF NOT EXISTS _schema_migrations (
    version     INT NOT NULL,
    name        STRING NOT NULL,
    checksum    STRING NOT NULL,
    applied_at  TIMESTAMP NOT NULL
  )
  COMMENT 'Ledger of applied schema migrations.'
""")

# COMMAND ----------

MIGRATION_FILE = re.compile(r"^V(\d+)__(.+)\.sql$")
ADD_COLUMN = re.compile(r"^\s*ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMNS?\b", re.IGNORECASE)
COLUMN_EXISTS_ERRORS = ("FIELDS_ALREADY_EXISTS", "COLUMN_ALREADY_EXISTS")


def run_statement(stmt):
    """Run one statement. Databricks SQL has no `ADD COLUMN IF NOT EXISTS`, so an
    ADD COLUMN whose column is already there (e.g. a run that died between the
    ALTER and the ledger insert) is treated as already applied."""
    try:
        spark.sql(stmt)
    except Exception as e:
        if ADD_COLUMN.match(stmt) and any(code in str(e) for code in COLUMN_EXISTS_ERRORS):
            print(f"       column already exists, skipping: {stmt.splitlines()[0]}")
        else:
            raise


def split_statements(sql_text):
    """Split a script on ';', ignoring full-line `--` comments."""
    lines = [l for l in sql_text.splitlines() if not l.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


migrations = []
for fname in os.listdir(migrations_dir):
    m = MIGRATION_FILE.match(fname)
    if m:
        migrations.append((int(m.group(1)), m.group(2), os.path.join(migrations_dir, fname)))
migrations.sort()

applied = {r.version: r.checksum for r in spark.table("_schema_migrations").collect()}

for version, name, path in migrations:
    with open(path) as f:
        sql_text = f.read()
    checksum = hashlib.sha256(sql_text.encode()).hexdigest()

    if version in applied:
        if applied[version] != checksum:
            raise RuntimeError(
                f"V{version:03d} ({name}) was already applied to {catalog}.{schema} "
                "but its file has changed. Migrations are immutable; add a new version."
            )
        print(f"skip   V{version:03d} {name} (already applied)")
        continue

    for stmt in split_statements(sql_text):
        run_statement(stmt)
    spark.sql(
        "INSERT INTO _schema_migrations VALUES (:v, :n, :c, current_timestamp())",
        args={"v": version, "n": name, "c": checksum},
    )
    print(f"apply  V{version:03d} {name}")

print(f"{catalog}.{schema} is at V{max([v for v, _, _ in migrations], default=0):03d}")
