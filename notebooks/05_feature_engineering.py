# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — ML: Large Fire Risk Predictor
# MAGIC
# MAGIC Trains a Random Forest classifier to predict whether a new ignition will grow
# MAGIC to Class E (>200 ha) based on features available at the time of ignition:
# MAGIC location, cause, season, month, and decade (climate trend proxy).
# MAGIC
# MAGIC Outputs:
# MAGIC - `ml.feature_store`      — engineered features for every historical fire
# MAGIC - `ml.risk_predictions`   — every fire scored with prob_large_fire (0–1)
# MAGIC - `ml.feature_importance` — which factors drive large-fire risk most
# MAGIC - `ml.model_metrics`      — AUC-ROC + accuracy logged per run

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from datetime import datetime

@F.udf(DoubleType())
def prob_class1(v):
    return float(v[1]) if v is not None else None

dbutils.widgets.text("env", "poc")
ENV     = dbutils.widgets.get("env")
CATALOG = f"wildfire_{ENV}"
RUN_TS  = datetime.utcnow().isoformat()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ml")
print(f"Environment : {ENV}  |  Catalog : {CATALOG}")

# COMMAND ----------
# MAGIC %md ## Feature Engineering

# COMMAND ----------

silver = spark.table(f"{CATALOG}.silver.wildfires_unified")

features_df = (
    silver
    .filter(
        F.col("total_hectares").isNotNull() &
        F.col("fire_centre").isNotNull() &
        F.col("general_cause").isNotNull() &
        F.col("season").isNotNull() &
        F.col("fire_start_date").isNotNull()
    )
    .withColumn("is_large_fire",
        (F.col("total_hectares") >= 200).cast("int")
    )
    .withColumn("fire_month", F.month(F.col("fire_start_date")))
    .select(
        "fire_id", "fire_year", "fire_centre", "general_cause",
        "season", "fire_month", "decade", "total_hectares",
        "size_class", "source_period", "is_large_fire"
    )
)

n_total = features_df.count()
n_large = features_df.filter(F.col("is_large_fire") == 1).count()
print(f"Total fires in feature set : {n_total:,}")
print(f"Large fires (Class E, >=200ha) : {n_large:,}  ({n_large/n_total*100:.1f}%)")

features_df.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.ml.feature_store")

# COMMAND ----------
# MAGIC %md ## Train / Test Split

# COMMAND ----------

# Stratified-ish split: hold out the most recent 10 years as test set
# This mirrors real deployment — train on the past, evaluate on the recent
TRAIN_CUTOFF = features_df.agg(F.max("fire_year")).first()[0] - 10

train = features_df.filter(F.col("fire_year") <= TRAIN_CUTOFF)
test  = features_df.filter(F.col("fire_year") >  TRAIN_CUTOFF)

print(f"Train: {train.count():,} fires (up to {TRAIN_CUTOFF})")
print(f"Test : {test.count():,} fires (after {TRAIN_CUTOFF})")

# COMMAND ----------
# MAGIC %md ## Build & Train Pipeline

# COMMAND ----------

# Fit each stage manually — more compatible with Spark Connect (serverless)
centre_m = StringIndexer(inputCol="fire_centre",   outputCol="centre_idx",  handleInvalid="keep").fit(train)
cause_m  = StringIndexer(inputCol="general_cause", outputCol="cause_idx",   handleInvalid="keep").fit(train)
season_m = StringIndexer(inputCol="season",        outputCol="season_idx",  handleInvalid="keep").fit(train)

def apply_stages(df):
    df = centre_m.transform(df)
    df = cause_m.transform(df)
    df = season_m.transform(df)
    return VectorAssembler(
        inputCols=["centre_idx", "cause_idx", "season_idx", "fire_month", "decade"],
        outputCol="features"
    ).transform(df)

train_t = apply_stages(train)
test_t  = apply_stages(test)

rf = RandomForestClassifier(
    labelCol="is_large_fire",
    featuresCol="features",
    numTrees=50,
    maxDepth=6,
    maxBins=128,
    seed=42
)
model = rf.fit(train_t)
print("Model trained.")

# COMMAND ----------
# MAGIC %md ## Evaluate

# COMMAND ----------

preds = model.transform(test_t)

auc = BinaryClassificationEvaluator(
    labelCol="is_large_fire", rawPredictionCol="rawPrediction"
).evaluate(preds)

accuracy = MulticlassClassificationEvaluator(
    labelCol="is_large_fire", predictionCol="prediction", metricName="accuracy"
).evaluate(preds)

precision = MulticlassClassificationEvaluator(
    labelCol="is_large_fire", predictionCol="prediction", metricName="weightedPrecision"
).evaluate(preds)

print(f"AUC-ROC  : {auc:.4f}")
print(f"Accuracy : {accuracy:.4f}")
print(f"Precision: {precision:.4f}")

metrics_row = [{
    "run_ts": RUN_TS, "env": ENV,
    "model": "RandomForestClassifier",
    "num_trees": 100, "max_depth": 6,
    "train_cutoff_year": int(TRAIN_CUTOFF),
    "train_rows": int(train.count()), "test_rows": int(test.count()),
    "auc_roc": float(auc), "accuracy": float(accuracy), "precision": float(precision)
}]
spark.createDataFrame(metrics_row) \
    .write.format("delta").mode("append").option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.ml.model_metrics")

# COMMAND ----------
# MAGIC %md ## Feature Importance

# COMMAND ----------

rf_model       = model.stages[-1]
feature_names  = ["fire_centre", "general_cause", "season", "fire_month", "decade"]
importance_df  = spark.createDataFrame(
    [(name, float(imp)) for name, imp in zip(feature_names, rf_model.featureImportances.toArray())],
    ["feature", "importance"]
).orderBy(F.col("importance").desc())

importance_df.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.ml.feature_importance")

print("Feature importance:")
display(importance_df)

# COMMAND ----------
# MAGIC %md ## Score All Historical Fires

# COMMAND ----------

all_features = apply_stages(spark.table(f"{CATALOG}.ml.feature_store"))
scored = (
    model.transform(all_features)
    .withColumn("prob_large_fire", prob_class1(F.col("probability")))
    .select(
        "fire_id", "fire_year", "fire_centre", "general_cause",
        "season", "fire_month", "total_hectares", "size_class",
        "is_large_fire", "prediction", "prob_large_fire"
    )
)

scored.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("fire_year") \
    .saveAsTable(f"{CATALOG}.ml.risk_predictions")

n_scored = scored.count()
n_high_risk = scored.filter(F.col("prob_large_fire") >= 0.5).count()
print(f"Scored {n_scored:,} fires")
print(f"High risk (prob >= 0.5): {n_high_risk:,}  ({n_high_risk/n_scored*100:.1f}%)")

# COMMAND ----------
# MAGIC %md ## Top High-Risk Fires

# COMMAND ----------

display(
    scored.filter(F.col("prob_large_fire") >= 0.7)
          .orderBy(F.col("prob_large_fire").desc())
          .limit(20)
)
