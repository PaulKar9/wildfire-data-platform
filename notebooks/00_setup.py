# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup & Configuration
# MAGIC Alberta Wildfire Data Platform — Databricks Free Edition (serverless + Unity Catalog)

# COMMAND ----------

dbutils.widgets.text("env", "poc")
ENV = dbutils.widgets.get("env")

CATALOG   = f"wildfire_{ENV}"
SCHEMAS   = ["bronze", "silver", "gold", "meta"]
DATA_PATH = f"/Workspace/Shared/wildfire_{ENV}/data"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  Schema ready: {CATALOG}.{schema}")

# COMMAND ----------

spark.sql("SELECT 1").show()
print(f"Spark version : {spark.version}")
print(f"Environment   : {ENV}")
print(f"Catalog       : {CATALOG} (Unity Catalog)")
print(f"Data path     : {DATA_PATH}")
print(f"\nSetup complete. Proceed to 01_bronze_ingestion.")
