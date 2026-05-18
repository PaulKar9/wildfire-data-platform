# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Curated Analytics Tables
# MAGIC
# MAGIC Builds 6 BI-ready tables from `silver.wildfires_unified`.
# MAGIC These answer the core operational questions for the Fire Operations team.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("env", "poc")
ENV = dbutils.widgets.get("env")

CATALOG = f"wildfire_{ENV}"

def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"

src = spark.table(tbl("silver", "wildfires_unified"))

# Only use records with no critical DQ flags for gold layer
DQ_CRITICAL = 1 | 2   # missing coords OR missing date
clean = src.filter(F.expr(f"(_dq_flags & {DQ_CRITICAL}) = 0"))

print(f"Total records: {src.count():,}  |  Clean for gold: {clean.count():,}")

# COMMAND ----------
# MAGIC %md ## 1. Annual Fire Statistics

# COMMAND ----------

annual_stats = clean.groupBy("fire_year").agg(
    F.count("fire_id").alias("total_fires"),
    F.round(F.sum("total_hectares"), 1).alias("total_hectares_burned"),
    F.round(F.avg("total_hectares"), 2).alias("avg_fire_size_ha"),
    F.round(F.max("total_hectares"), 1).alias("largest_fire_ha"),
    F.round(F.avg("fire_duration_days"), 1).alias("avg_duration_days"),
    F.sum(F.when(F.col("general_cause") == "Lightning", 1).otherwise(0)).alias("lightning_fires"),
    F.sum(F.when(F.col("general_cause") == "Human", 1).otherwise(0)).alias("human_fires"),
    F.sum(F.when(F.col("size_class") == "E", 1).otherwise(0)).alias("class_e_fires"),
    F.countDistinct("fire_centre").alias("fire_centres_active"),
).withColumn("human_cause_pct",
    F.round(F.col("human_fires") / F.col("total_fires") * 100, 1)
).withColumn("escape_rate_pct",
    F.round(F.col("class_e_fires") / F.col("total_fires") * 100, 2)
).orderBy("fire_year")

annual_stats.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("gold", "annual_fire_statistics"))

print(f"annual_fire_statistics: {annual_stats.count()} rows (one per year)")
display(annual_stats.orderBy(F.col("total_hectares_burned").desc()).limit(10))

# COMMAND ----------
# MAGIC %md ## 2. Regional Fire Patterns (by fire centre)

# COMMAND ----------

regional = clean.groupBy("fire_centre", "fire_year").agg(
    F.count("fire_id").alias("total_fires"),
    F.round(F.sum("total_hectares"), 1).alias("total_hectares_burned"),
    F.round(F.avg("total_hectares"), 2).alias("avg_fire_size_ha"),
    F.round(F.max("total_hectares"), 1).alias("largest_fire_ha"),
    F.sum(F.when(F.col("general_cause") == "Lightning", 1).otherwise(0)).alias("lightning_fires"),
    F.sum(F.when(F.col("general_cause") == "Human", 1).otherwise(0)).alias("human_fires"),
).withColumn("human_cause_pct",
    F.round(F.col("human_fires") / F.col("total_fires") * 100, 1)
).orderBy("fire_centre", "fire_year")

regional.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("gold", "regional_fire_patterns"))

print(f"regional_fire_patterns: {regional.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 3. Fire Cause Trends (decade-level)

# COMMAND ----------

cause_trends = clean.groupBy("decade", "general_cause").agg(
    F.count("fire_id").alias("fire_count"),
    F.round(F.sum("total_hectares"), 1).alias("total_hectares"),
    F.round(F.avg("total_hectares"), 2).alias("avg_hectares"),
).orderBy("decade", "general_cause")

# Add % within decade
decade_totals = cause_trends.groupBy("decade").agg(
    F.sum("fire_count").alias("decade_total_fires")
)
cause_trends = cause_trends.join(decade_totals, "decade") \
    .withColumn("pct_of_decade_fires",
        F.round(F.col("fire_count") / F.col("decade_total_fires") * 100, 1)) \
    .drop("decade_total_fires")

cause_trends.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("gold", "fire_cause_trends"))

print(f"fire_cause_trends: {cause_trends.count()} rows")
display(cause_trends)

# COMMAND ----------
# MAGIC %md ## 4. Top 25 Largest Fires on Record

# COMMAND ----------

top_fires = src.filter(F.col("total_hectares").isNotNull()) \
    .orderBy(F.col("total_hectares").desc()) \
    .limit(25) \
    .select(
        "fire_id", "fire_year", "fire_centre", "fire_start_date",
        "total_hectares", "size_class", "general_cause", "true_cause",
        "latitude", "longitude", "fire_duration_days", "fuel_type",
        "source_period"
    )

top_fires.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("gold", "top_fires"))

print("Top 5 fires on record:")
display(top_fires.limit(5))

# COMMAND ----------
# MAGIC %md ## 5. Seasonal Distribution

# COMMAND ----------

seasonal = clean.groupBy("season", "fire_year").agg(
    F.count("fire_id").alias("fire_count"),
    F.round(F.sum("total_hectares"), 1).alias("total_hectares"),
    F.round(F.avg("total_hectares"), 2).alias("avg_ha_per_fire"),
    F.round(F.avg(F.month(F.col("fire_start_date"))), 1).alias("avg_month"),
).orderBy("fire_year", "season")

seasonal.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("gold", "seasonal_distribution"))

print(f"seasonal_distribution: {seasonal.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 6. Fire Season Length Trend (Is the season getting longer?)

# COMMAND ----------

season_length = clean.filter(F.col("fire_start_date").isNotNull()) \
    .groupBy("fire_year").agg(
        F.min("fire_start_date").alias("first_ignition_date"),
        F.max("fire_start_date").alias("last_ignition_date"),
        F.count("fire_id").alias("total_fires"),
    ).withColumn("season_length_days",
        F.datediff(F.col("last_ignition_date"), F.col("first_ignition_date"))
    ).withColumn("first_ignition_doy",
        F.dayofyear(F.col("first_ignition_date"))
    ).withColumn("last_ignition_doy",
        F.dayofyear(F.col("last_ignition_date"))
    ).orderBy("fire_year")

season_length.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("gold", "fire_season_length"))

print(f"fire_season_length: {season_length.count()} rows (one per year)")
display(season_length.orderBy("fire_year"))

# COMMAND ----------
# MAGIC %md ## Summary: Gold Layer Complete

# COMMAND ----------

gold_tables = [
    "annual_fire_statistics",
    "regional_fire_patterns",
    "fire_cause_trends",
    "top_fires",
    "seasonal_distribution",
    "fire_season_length",
]
print("Gold tables written:")
for t in gold_tables:
    n = spark.table(tbl("gold", t)).count()
    print(f"  gold.{t:35s}  {n:>6,} rows")

