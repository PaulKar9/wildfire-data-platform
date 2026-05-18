# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Schema Unification & Cleansing
# MAGIC
# MAGIC The 4 source CSVs span 65 years and use 3 different schema conventions:
# MAGIC - **1961–1982**: abbreviated codes, numeric cause/date columns
# MAGIC - **1983–1995**: snake_case, numeric cause codes, more weather fields
# MAGIC - **1996–2005**: descriptive text fields, ISO dates, modern naming
# MAGIC - **2006–2025**: richest schema with weather, resource, and suppression detail
# MAGIC
# MAGIC This notebook produces `silver.wildfires_unified` — one table, one schema, 65 years.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

dbutils.widgets.text("env", "poc")
ENV = dbutils.widgets.get("env")

CATALOG = f"wildfire_{ENV}"

def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"

# Safe cast: returns NULL instead of error for 'NA', empty strings, etc.
def sd(col_name):
    return F.expr(f"try_cast(`{col_name}` as double)")

def si(col_name):
    return F.expr(f"try_cast(`{col_name}` as int)")

# COMMAND ----------
# MAGIC %md ## Cause code lookup tables (1961–1995 used numeric codes)

# COMMAND ----------

# Alberta general cause codes (numeric → text)
GENCAUSE_MAP_61_82 = {
    1: "Lightning", 2: "Human", 3: "Human", 4: "Human",
    5: "Human", 6: "Human", 7: "Human", 8: "Unknown", 9: "Unknown"
}
GENCAUSE_MAP_83_95 = {
    1: "Lightning", 2: "Human", 3: "Human", 4: "Human",
    5: "Human", 6: "Human", 7: "Human", 8: "Prescribed Fire", 9: "Unknown"
}

gencause_map_61 = F.create_map([F.lit(x) for pair in GENCAUSE_MAP_61_82.items() for x in pair])
gencause_map_83 = F.create_map([F.lit(x) for pair in GENCAUSE_MAP_83_95.items() for x in pair])

# Size class lookup (numeric → letter)
SIZE_CLASS_MAP = {1: "A", 2: "B", 3: "C", 4: "D", 5: "E"}
size_class_map = F.create_map([F.lit(x) for pair in SIZE_CLASS_MAP.items() for x in pair])

def derive_size_class(ha_col):
    return (F.when(ha_col < 0.1, "A")
             .when(ha_col < 4.0, "B")
             .when(ha_col < 40.0, "C")
             .when(ha_col < 200.0, "D")
             .otherwise("E"))

def derive_season(month_col):
    return (F.when(month_col.isin(3, 4, 5), "Spring")
             .when(month_col.isin(6, 7, 8), "Summer")
             .when(month_col.isin(9, 10, 11), "Fall")
             .otherwise("Winter"))

# DQ flag bitmask
DQ_MISSING_COORDS    = 1
DQ_MISSING_DATE      = 2
DQ_ZERO_HECTARES     = 4
DQ_MISSING_CAUSE     = 8
DQ_COORD_OUT_OF_AB   = 16   # AB bounds: lat 49-60, lon -120 to -110

# COMMAND ----------
# MAGIC %md ## Transform 1961–1982

# COMMAND ----------

raw_61 = spark.table(tbl("bronze", "wildfires_1961_1982"))

silver_61 = raw_61.select(
    F.concat(F.col("FIRENUMBER")).alias("fire_id"),
    (F.col("YEAR").cast(IntegerType()) + 1900).alias("fire_year"),
    sd("LAT").alias("latitude"),
    sd("LONG").alias("longitude"),
    F.col("FOREST").alias("fire_centre"),
    F.col("DISTRICT").alias("fire_district"),
    gencause_map_61[F.col("GENCAUSE").cast(IntegerType())].alias("general_cause"),
    F.lit(None).cast(StringType()).alias("true_cause"),
    # YEAR is stored as 2-digit (61 = 1961) in this era — add 1900
    F.expr("try_to_date(concat_ws('-', YEAR + 1900, MON, DAY), 'yyyy-M-d')").alias("fire_start_date"),
    F.expr("try_to_date(concat_ws('-', YEAR + 1900, DMON, DDAY), 'yyyy-M-d')").alias("discovered_date"),
    F.lit(None).cast(DateType()).alias("extinguished_date"),
    sd("TOTAL").alias("total_hectares"),
    size_class_map[F.col("SIZECLAS").cast(IntegerType())].alias("size_class"),
    F.col("PRIFUEL").cast(StringType()).alias("fuel_type"),
    F.lit("1961-1982").alias("source_period"),
    F.col("_ingested_at"),
).withColumn("fire_duration_days",
    F.datediff(F.col("extinguished_date"), F.col("fire_start_date"))
).withColumn("decade",
    (F.floor(F.col("fire_year") / 10) * 10).cast(IntegerType())
).withColumn("season",
    derive_season(F.month(F.col("fire_start_date")))
).withColumn("size_class",
    F.when(F.col("size_class").isNull(), derive_size_class(F.col("total_hectares")))
     .otherwise(F.col("size_class"))
).withColumn("_dq_flags",
    (F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), DQ_MISSING_COORDS).otherwise(0)) +
    (F.when(F.col("fire_start_date").isNull(), DQ_MISSING_DATE).otherwise(0)) +
    (F.when(F.col("total_hectares").isNull() | (F.col("total_hectares") == 0), DQ_ZERO_HECTARES).otherwise(0)) +
    (F.when(F.col("general_cause").isNull(), DQ_MISSING_CAUSE).otherwise(0)) +
    (F.when(
        F.col("latitude").isNotNull() & (
            (F.col("latitude") < 49) | (F.col("latitude") > 60) |
            (F.col("longitude") < -120) | (F.col("longitude") > -110)
        ), DQ_COORD_OUT_OF_AB).otherwise(0))
)

print(f"1961-1982 silver: {silver_61.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Transform 1983–1995

# COMMAND ----------

raw_83 = spark.table(tbl("bronze", "wildfires_1983_1995"))

silver_83 = raw_83.select(
    F.col("firenumber").alias("fire_id"),
    si("fire_year").alias("fire_year"),
    sd("lat").alias("latitude"),
    sd("long").alias("longitude"),
    F.col("district").alias("fire_centre"),
    F.lit(None).cast(StringType()).alias("fire_district"),
    gencause_map_83[si("gencause")].alias("general_cause"),
    F.col("truecause").cast(StringType()).alias("true_cause"),
    F.expr("try_to_date(startdate, 'yyyy-MM-dd')").alias("fire_start_date"),
    F.expr("try_to_date(discovdate, 'yyyy-MM-dd')").alias("discovered_date"),
    F.expr("try_to_date(extingdate, 'yyyy-MM-dd')").alias("extinguished_date"),
    sd("grandarea").alias("total_hectares"),
    F.upper(F.col("sizeclass")).alias("size_class"),
    F.col("fueltype").cast(StringType()).alias("fuel_type"),
    F.lit("1983-1995").alias("source_period"),
    F.col("_ingested_at"),
).withColumn("fire_duration_days",
    F.datediff(F.col("extinguished_date"), F.col("fire_start_date"))
).withColumn("decade",
    (F.floor(F.col("fire_year") / 10) * 10).cast(IntegerType())
).withColumn("season",
    derive_season(F.month(F.col("fire_start_date")))
).withColumn("size_class",
    F.when(F.col("size_class").isNull(), derive_size_class(F.col("total_hectares")))
     .otherwise(F.col("size_class"))
).withColumn("_dq_flags",
    (F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), DQ_MISSING_COORDS).otherwise(0)) +
    (F.when(F.col("fire_start_date").isNull(), DQ_MISSING_DATE).otherwise(0)) +
    (F.when(F.col("total_hectares").isNull() | (F.col("total_hectares") == 0), DQ_ZERO_HECTARES).otherwise(0)) +
    (F.when(F.col("general_cause").isNull(), DQ_MISSING_CAUSE).otherwise(0)) +
    (F.when(
        F.col("latitude").isNotNull() & (
            (F.col("latitude") < 49) | (F.col("latitude") > 60) |
            (F.col("longitude") < -120) | (F.col("longitude") > -110)
        ), DQ_COORD_OUT_OF_AB).otherwise(0))
)

print(f"1983-1995 silver: {silver_83.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Transform 1996–2005

# COMMAND ----------

raw_96 = spark.table(tbl("bronze", "wildfires_1996_2005"))

silver_96 = raw_96.select(
    F.col("fire_number").alias("fire_id"),
    F.col("fire_year").cast(IntegerType()).alias("fire_year"),
    sd("fire_location_latitude").alias("latitude"),
    sd("fire_location_longitude").alias("longitude"),
    F.col("fire_origin").alias("fire_centre"),
    F.lit(None).cast(StringType()).alias("fire_district"),
    F.col("general_cause_desc").alias("general_cause"),
    F.col("true_cause").alias("true_cause"),
    F.expr("cast(try_to_timestamp(fire_start_date, 'yyyy-MM-dd HH:mm:ss') as date)").alias("fire_start_date"),
    F.expr("cast(try_to_timestamp(discovered_date, 'yyyy-MM-dd HH:mm:ss') as date)").alias("discovered_date"),
    F.expr("cast(try_to_timestamp(ex_fs_date, 'yyyy-MM-dd HH:mm:ss') as date)").alias("extinguished_date"),
    sd("current_size").alias("total_hectares"),
    F.upper(F.col("size_class")).alias("size_class"),
    F.col("fuel_type").alias("fuel_type"),
    F.lit("1996-2005").alias("source_period"),
    F.col("_ingested_at"),
).withColumn("fire_duration_days",
    F.datediff(F.col("extinguished_date"), F.col("fire_start_date"))
).withColumn("decade",
    (F.floor(F.col("fire_year") / 10) * 10).cast(IntegerType())
).withColumn("season",
    derive_season(F.month(F.col("fire_start_date")))
).withColumn("size_class",
    F.when(F.col("size_class").isNull(), derive_size_class(F.col("total_hectares")))
     .otherwise(F.col("size_class"))
).withColumn("_dq_flags",
    (F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), DQ_MISSING_COORDS).otherwise(0)) +
    (F.when(F.col("fire_start_date").isNull(), DQ_MISSING_DATE).otherwise(0)) +
    (F.when(F.col("total_hectares").isNull() | (F.col("total_hectares") == 0), DQ_ZERO_HECTARES).otherwise(0)) +
    (F.when(F.col("general_cause").isNull(), DQ_MISSING_CAUSE).otherwise(0)) +
    (F.when(
        F.col("latitude").isNotNull() & (
            (F.col("latitude") < 49) | (F.col("latitude") > 60) |
            (F.col("longitude") < -120) | (F.col("longitude") > -110)
        ), DQ_COORD_OUT_OF_AB).otherwise(0))
)

print(f"1996-2005 silver: {silver_96.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Transform 2006–2025  (most complete — preserve extra weather columns)

# COMMAND ----------

raw_06 = spark.table(tbl("bronze", "wildfires_2006_2025"))

# Normalize cause text across this era
def normalize_cause(col):
    return F.when(F.lower(col).contains("lightning"), "Lightning") \
             .when(F.lower(col).isin("human", "recreation", "resident", "industry",
                                     "incendiary", "railway", "utilities",
                                     "prescribed fire escaped"), "Human") \
             .when(F.lower(col).contains("unknown"), "Unknown") \
             .otherwise(col)

silver_06 = raw_06.select(
    F.col("FIRE_NUMBER").alias("fire_id"),
    F.col("YEAR").cast(IntegerType()).alias("fire_year"),
    sd("LATITUDE").alias("latitude"),
    sd("LONGITUDE").alias("longitude"),
    F.col("FIRE_ORIGIN").alias("fire_centre"),
    F.lit(None).cast(StringType()).alias("fire_district"),
    normalize_cause(F.col("GENERAL_CAUSE")).alias("general_cause"),
    F.col("TRUE_CAUSE").alias("true_cause"),
    F.expr("cast(try_to_timestamp(FIRE_START_DATE, 'yyyy-MM-dd HH:mm') as date)").alias("fire_start_date"),
    F.expr("cast(try_to_timestamp(DISCOVERED_DATE, 'yyyy-MM-dd HH:mm') as date)").alias("discovered_date"),
    F.expr("cast(try_to_timestamp(FIRST_EX_DATE, 'yyyy-MM-dd HH:mm') as date)").alias("extinguished_date"),
    sd("CURRENT_SIZE").alias("total_hectares"),
    F.upper(F.col("SIZE_CLASS")).alias("size_class"),
    F.col("FUEL_TYPE").alias("fuel_type"),
    F.lit("2006-2025").alias("source_period"),
    F.col("_ingested_at"),
    # Extra columns available only in this era — keep for enriched analysis
    sd("TEMPERATURE").alias("temperature_c"),
    sd("RELATIVE_HUMIDITY").alias("relative_humidity_pct"),
    sd("WIND_SPEED").alias("wind_speed_kmh"),
    F.col("WIND_DIRECTION").alias("wind_direction"),
    sd("FIRE_SPREAD_RATE").alias("fire_spread_rate"),
).withColumn("fire_duration_days",
    F.datediff(F.col("extinguished_date"), F.col("fire_start_date"))
).withColumn("decade",
    (F.floor(F.col("fire_year") / 10) * 10).cast(IntegerType())
).withColumn("season",
    derive_season(F.month(F.col("fire_start_date")))
).withColumn("size_class",
    F.when(F.col("size_class").isNull(), derive_size_class(F.col("total_hectares")))
     .otherwise(F.col("size_class"))
).withColumn("_dq_flags",
    (F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), DQ_MISSING_COORDS).otherwise(0)) +
    (F.when(F.col("fire_start_date").isNull(), DQ_MISSING_DATE).otherwise(0)) +
    (F.when(F.col("total_hectares").isNull() | (F.col("total_hectares") == 0), DQ_ZERO_HECTARES).otherwise(0)) +
    (F.when(F.col("general_cause").isNull(), DQ_MISSING_CAUSE).otherwise(0)) +
    (F.when(
        F.col("latitude").isNotNull() & (
            (F.col("latitude") < 49) | (F.col("latitude") > 60) |
            (F.col("longitude") < -120) | (F.col("longitude") > -110)
        ), DQ_COORD_OUT_OF_AB).otherwise(0))
)

print(f"2006-2025 silver: {silver_06.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Union all eras → wildfires_unified

# COMMAND ----------

# Common columns shared across all eras
COMMON_COLS = [
    "fire_id", "fire_year", "latitude", "longitude",
    "fire_centre", "fire_district", "general_cause", "true_cause",
    "fire_start_date", "discovered_date", "extinguished_date",
    "total_hectares", "size_class", "fuel_type",
    "fire_duration_days", "decade", "season",
    "source_period", "_ingested_at", "_dq_flags",
]

# Fill weather columns with null for older eras that didn't collect them
def pad_weather(df):
    for col in ["temperature_c", "relative_humidity_pct", "wind_speed_kmh",
                "wind_direction", "fire_spread_rate"]:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(DoubleType()) if "pct" in col or "c" == col[-1] or "h" == col[-1] else F.lit(None).cast(StringType()))
    return df

unified = (pad_weather(silver_61).select(COMMON_COLS + ["temperature_c","relative_humidity_pct","wind_speed_kmh","wind_direction","fire_spread_rate"])
    .unionByName(pad_weather(silver_83).select(COMMON_COLS + ["temperature_c","relative_humidity_pct","wind_speed_kmh","wind_direction","fire_spread_rate"]))
    .unionByName(pad_weather(silver_96).select(COMMON_COLS + ["temperature_c","relative_humidity_pct","wind_speed_kmh","wind_direction","fire_spread_rate"]))
    .unionByName(silver_06.select(COMMON_COLS + ["temperature_c","relative_humidity_pct","wind_speed_kmh","wind_direction","fire_spread_rate"]))
)

# Deduplicate on fire_id + fire_year (keep most-recent ingestion)
from pyspark.sql.window import Window
w = Window.partitionBy("fire_id", "fire_year").orderBy(F.col("_ingested_at").desc())
unified = unified.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

unified.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("fire_year") \
    .saveAsTable(tbl("silver", "wildfires_unified"))

total = unified.count()
print(f"silver.wildfires_unified: {total:,} rows")
print(f"Clean records (_dq_flags=0): {unified.filter(F.col('_dq_flags') == 0).count():,}")

# COMMAND ----------

display(spark.table(tbl("silver", "wildfires_unified")).limit(5))

