# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Regional Risk Profile
# MAGIC
# MAGIC Builds a geographic resource planning table by aggregating fire risk predictions
# MAGIC from notebook 05 across fire centres and decades.
# MAGIC
# MAGIC Addresses the key operational question:
# MAGIC *Which regions should pre-position the most resources, and is that risk growing?*
# MAGIC
# MAGIC Output: `ml.regional_risk_profile`
# MAGIC - One row per fire_centre x decade
# MAGIC - Predicted risk vs actual outcome (model calibration check)
# MAGIC - Risk tier: High / Medium / Low
# MAGIC - Decade-over-decade trend: Increasing / Stable / Decreasing

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("env", "poc")
ENV     = dbutils.widgets.get("env")
CATALOG = f"wildfire_{ENV}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ml")
print(f"Environment : {ENV}  |  Catalog : {CATALOG}")

# COMMAND ----------
# MAGIC %md ## Aggregate predictions by fire centre x decade

# COMMAND ----------

preds = spark.table(f"{CATALOG}.ml.risk_predictions")

regional = (
    preds
    .filter(F.col("fire_centre").isNotNull())
    .groupBy("fire_centre", F.floor(F.col("fire_year") / 10).cast("int").alias("decade_start"))
    .agg(
        F.round(F.avg("prob_large_fire") * 100, 2).alias("avg_predicted_risk_pct"),
        F.round(F.avg("is_large_fire") * 100, 2).alias("actual_large_fire_pct"),
        F.count("fire_id").alias("total_fires"),
        F.sum("is_large_fire").alias("actual_large_fires"),
        F.round(F.sum(F.col("prediction")).cast("double") / F.count("fire_id") * 100, 2).alias("model_predicted_pct"),
    )
    .withColumn("decade", (F.col("decade_start") * 10).cast("int"))
    .withColumn("calibration_error_pct",
        F.round(F.abs(F.col("avg_predicted_risk_pct") - F.col("actual_large_fire_pct")), 2)
    )
    .withColumn("risk_tier",
        F.when(F.col("actual_large_fire_pct") >= 15, "High")
         .when(F.col("actual_large_fire_pct") >= 5,  "Medium")
         .otherwise("Low")
    )
    .drop("decade_start")
)

# COMMAND ----------
# MAGIC %md ## Add decade-over-decade trend

# COMMAND ----------

# Compare each decade's actual rate to the previous decade for that fire centre
w_prev = Window.partitionBy("fire_centre").orderBy("decade")

regional = (
    regional
    .withColumn("prev_decade_pct", F.lag("actual_large_fire_pct").over(w_prev))
    .withColumn("decade_trend",
        F.when(F.col("prev_decade_pct").isNull(), "Baseline")
         .when(F.col("actual_large_fire_pct") > F.col("prev_decade_pct") + 2, "Increasing")
         .when(F.col("actual_large_fire_pct") < F.col("prev_decade_pct") - 2, "Decreasing")
         .otherwise("Stable")
    )
    .drop("prev_decade_pct")
    .orderBy("fire_centre", "decade")
)

# COMMAND ----------
# MAGIC %md ## Write to Delta

# COMMAND ----------

regional.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.ml.regional_risk_profile")

n = regional.count()
print(f"regional_risk_profile: {n:,} rows ({regional.select('fire_centre').distinct().count()} fire centres x decades)")

# COMMAND ----------
# MAGIC %md ## Top high-risk regions

# COMMAND ----------

top_regions = (
    spark.table(f"{CATALOG}.ml.regional_risk_profile")
    .filter(F.col("decade") >= 2000)
    .orderBy(F.col("actual_large_fire_pct").desc())
    .limit(15)
)
display(top_regions)

# COMMAND ----------
# MAGIC %md ## Risk trend summary — regions with increasing risk

# COMMAND ----------

increasing = (
    spark.table(f"{CATALOG}.ml.regional_risk_profile")
    .filter(F.col("decade_trend") == "Increasing")
    .orderBy(F.col("actual_large_fire_pct").desc())
)
print(f"Regions with increasing decade-over-decade risk: {increasing.count()}")
display(increasing)
