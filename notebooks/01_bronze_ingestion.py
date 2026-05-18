# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Raw Ingestion
# MAGIC Ingests all 4 Alberta wildfire CSVs exactly as received — no transformation.
# MAGIC Each era gets its own table to preserve original schema fidelity.
# MAGIC
# MAGIC | Table | Period | Rows |
# MAGIC |-------|--------|------|
# MAGIC | bronze.wildfires_1961_1982 | 1961–1982 | 15,983 |
# MAGIC | bronze.wildfires_1983_1995 | 1983–1995 | 12,345 |
# MAGIC | bronze.wildfires_1996_2005 | 1996–2005 | 11,352 |
# MAGIC | bronze.wildfires_2006_2025 | 2006–2025 | 27,828 |

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("env", "poc")
ENV = dbutils.widgets.get("env")

CATALOG   = f"wildfire_{ENV}"
DATA_PATH = f"/Workspace/Shared/wildfire_{ENV}/data"

def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"

BATCH_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

# COMMAND ----------
# MAGIC %md ## Source CSV files
# MAGIC
# MAGIC Files are read from workspace files at `/Workspace/Shared/wildfire_poc/data/`
# MAGIC (uploaded automatically by the IaC deploy script).

# COMMAND ----------

LANDING = DATA_PATH

# Audit columns added to every bronze table
def add_audit(df, source_file, period):
    return df.withColumn("_source_file",   F.lit(source_file)) \
             .withColumn("_source_period", F.lit(period)) \
             .withColumn("_ingested_at",   F.lit(BATCH_ID)) \
             .withColumn("_batch_id",      F.lit(BATCH_ID))

# COMMAND ----------
# MAGIC %md ## 1961–1982  (oldest era — abbreviated column names, numeric codes)

# COMMAND ----------

df_1961 = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{LANDING}/af-historic-wildfires-1961-1982-data.csv")

df_1961 = add_audit(df_1961, "af-historic-wildfires-1961-1982-data.csv", "1961-1982")

df_1961.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("YEAR") \
    .saveAsTable(tbl("bronze", "wildfires_1961_1982"))

print(f"1961-1982 ingested: {df_1961.count():,} rows, {len(df_1961.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 1983–1995

# COMMAND ----------

df_1983 = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{LANDING}/af-historic-wildfires-1983-1995-data.csv")

df_1983 = add_audit(df_1983, "af-historic-wildfires-1983-1995-data.csv", "1983-1995")

df_1983.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("fire_year") \
    .saveAsTable(tbl("bronze", "wildfires_1983_1995"))

print(f"1983-1995 ingested: {df_1983.count():,} rows, {len(df_1983.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 1996–2005

# COMMAND ----------

df_1996 = spark.read.option("header", True).option("inferSchema", True) \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .csv(f"{LANDING}/af-historic-wildfires-1996-2005-data.csv")

df_1996 = add_audit(df_1996, "af-historic-wildfires-1996-2005-data.csv", "1996-2005")

df_1996.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("fire_year") \
    .saveAsTable(tbl("bronze", "wildfires_1996_2005"))

print(f"1996-2005 ingested: {df_1996.count():,} rows, {len(df_1996.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 2006–2025  (richest schema — 50 columns including weather)

# COMMAND ----------

df_2006 = spark.read.option("header", True).option("inferSchema", True) \
    .option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .csv(f"{LANDING}/fp-historical-wildfire-data-2006-2025.csv")

df_2006 = add_audit(df_2006, "fp-historical-wildfire-data-2006-2025.csv", "2006-2025")

df_2006.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("YEAR") \
    .saveAsTable(tbl("bronze", "wildfires_2006_2025"))

print(f"2006-2025 ingested: {df_2006.count():,} rows, {len(df_2006.columns)} columns")

# COMMAND ----------
# MAGIC %md ## Verify all 4 tables

# COMMAND ----------

tables = [
    ("bronze.wildfires_1961_1982", "1961-1982"),
    ("bronze.wildfires_1983_1995", "1983-1995"),
    ("bronze.wildfires_1996_2005", "1996-2005"),
    ("bronze.wildfires_2006_2025", "2006-2025"),
]
total = 0
for t, period in tables:
    n = spark.table(tbl(*t.split("."))).count()
    total += n
    print(f"  {period:12s}  {n:>7,} rows")
print(f"  {'TOTAL':12s}  {total:>7,} rows")
