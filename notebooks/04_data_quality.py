# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Data Quality Framework
# MAGIC
# MAGIC Runs DQ checks across all three layers and writes results to `meta.dq_metrics`.
# MAGIC Designed to be re-run after each pipeline execution.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

dbutils.widgets.text("env", "poc")
ENV = dbutils.widgets.get("env")

CATALOG = f"wildfire_{ENV}"
RUN_TS  = datetime.utcnow().isoformat()

def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"

# COMMAND ----------
# MAGIC %md ## DQ Check Helper

# COMMAND ----------

dq_results = []

def check(layer, table, metric, value, threshold_pct=None, status_override=None):
    if status_override:
        status = status_override
    elif threshold_pct is not None:
        status = "PASS" if value <= threshold_pct else "FAIL"
    else:
        status = "INFO"
    dq_results.append({
        "run_ts": RUN_TS, "layer": layer, "table": table,
        "metric": metric, "value": float(value), "threshold_pct": threshold_pct,
        "status": status
    })
    flag = "✓" if status == "PASS" else ("✗" if status == "FAIL" else "·")
    print(f"  {flag} [{layer:6s}] {table:35s} {metric:40s} {value:.2f}" +
          (f"  (threshold {threshold_pct}%)" if threshold_pct else ""))

# COMMAND ----------
# MAGIC %md ## Bronze Layer Checks

# COMMAND ----------

print("=== BRONZE ===")
for period, btable in [
    ("1961-1982", "wildfires_1961_1982"),
    ("1983-1995", "wildfires_1983_1995"),
    ("1996-2005", "wildfires_1996_2005"),
    ("2006-2025", "wildfires_2006_2025"),
]:
    df = spark.table(tbl("bronze", btable))
    n = df.count()
    check("bronze", btable, "row_count", n, status_override="INFO")

# COMMAND ----------
# MAGIC %md ## Silver Layer Checks

# COMMAND ----------

print("\n=== SILVER ===")
silver = spark.table(tbl("silver", "wildfires_unified"))
n = silver.count()
check("silver", "wildfires_unified", "row_count", n, status_override="INFO")

# Null rates on key columns
for col_name in ["latitude", "longitude", "fire_start_date", "total_hectares", "general_cause", "size_class"]:
    null_pct = silver.filter(F.col(col_name).isNull()).count() / n * 100
    threshold = 30.0 if col_name in ["latitude", "longitude"] else 20.0
    check("silver", "wildfires_unified", f"null_rate_{col_name}_pct", null_pct, threshold)

# Coordinate range check (Alberta bounds: lat 49–60, lon -120 to -110)
coord_df = silver.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
out_of_bounds = coord_df.filter(
    (F.col("latitude") < 49) | (F.col("latitude") > 60) |
    (F.col("longitude") < -120) | (F.col("longitude") > -110)
).count()
check("silver", "wildfires_unified", "coord_out_of_alberta_bounds_pct",
      out_of_bounds / n * 100, 5.0)

# Negative hectares
neg_ha = silver.filter(F.col("total_hectares") < 0).count()
check("silver", "wildfires_unified", "negative_hectares_count", neg_ha, 0.0)

# Impossible durations (fire extinguished before it started)
bad_duration = silver.filter(
    F.col("fire_duration_days").isNotNull() & (F.col("fire_duration_days") < 0)
).count()
check("silver", "wildfires_unified", "negative_duration_count", bad_duration, 0.5)

# Duplicate fire IDs within same year
from pyspark.sql.window import Window
dup_window = Window.partitionBy("fire_id", "fire_year")
dups = silver.withColumn("cnt", F.count("*").over(dup_window)).filter(F.col("cnt") > 1).count()
check("silver", "wildfires_unified", "duplicate_fire_id_year_count", dups, 0.0)

# Year range
min_year, max_year = silver.agg(F.min("fire_year"), F.max("fire_year")).first()
check("silver", "wildfires_unified", "min_fire_year", min_year, status_override="INFO")
check("silver", "wildfires_unified", "max_fire_year", max_year, status_override="INFO")

# Unknown cause rate
unknown_cause = silver.filter(
    F.col("general_cause").isNull() | (F.lower(F.col("general_cause")) == "unknown")
).count()
check("silver", "wildfires_unified", "unknown_cause_pct", unknown_cause / n * 100, 25.0)

# COMMAND ----------
# MAGIC %md ## Gold Layer Checks

# COMMAND ----------

print("\n=== GOLD ===")

# Annual statistics: 65 years expected
annual = spark.table(tbl("gold", "annual_fire_statistics"))
year_count = annual.count()
check("gold", "annual_fire_statistics", "year_coverage_count", year_count, status_override="INFO")
missing_years = 65 - year_count
check("gold", "annual_fire_statistics", "missing_years_count", missing_years, 5.0)

# No year should have 0 fires
zero_fire_years = annual.filter(F.col("total_fires") == 0).count()
check("gold", "annual_fire_statistics", "zero_fire_years_count", zero_fire_years, 0.0)

# Top fires: should be exactly 25
top = spark.table(tbl("gold", "top_fires"))
check("gold", "top_fires", "row_count", top.count(), status_override="INFO")

# COMMAND ----------
# MAGIC %md ## Write DQ Metrics to Meta Table

# COMMAND ----------

schema = StructType([
    StructField("run_ts", StringType()),
    StructField("layer", StringType()),
    StructField("table", StringType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("threshold_pct", DoubleType()),
    StructField("status", StringType()),
])

dq_df = spark.createDataFrame(dq_results, schema)

dq_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(tbl("meta", "dq_metrics"))

print(f"\n{len(dq_results)} checks written to meta.dq_metrics")

# COMMAND ----------
# MAGIC %md ## DQ Summary Dashboard

# COMMAND ----------

summary = dq_df.groupBy("status").count().orderBy("status")
display(summary)

fails = dq_df.filter(F.col("status") == "FAIL")
if fails.count() > 0:
    print("\n⚠ FAILING CHECKS:")
    display(fails)
else:
    print("\n✓ All threshold checks passed.")

