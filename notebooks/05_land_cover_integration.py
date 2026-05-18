# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Alberta Satellite Land Cover Integration
# MAGIC
# MAGIC Integrates the `AlbertaSatelliteLandCover.gdb` ESRI File Geodatabase
# MAGIC with wildfire fire locations to answer: **what land cover type burns most?**
# MAGIC
# MAGIC **Format:** ESRI File Geodatabase (`.gdb`) — requires `fiona` + `geopandas`.
# MAGIC **Install:** Run the %pip cell below (one-time per cluster lifecycle).

# COMMAND ----------

# MAGIC %pip install fiona geopandas --quiet

# COMMAND ----------

import geopandas as gpd
import fiona
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

USE_UNITY_CATALOG = False
CATALOG = "wildfire_poc"
BASE_PATH = "/FileStore/wildfire_poc"

def tbl(schema, name):
    if USE_UNITY_CATALOG:
        return f"{CATALOG}.{schema}.{name}"
    return f"{schema}.{name}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Upload GDB
# MAGIC Upload the extracted `AlbertaSatelliteLandCover/Data/AlbertaSatelliteLandCover.gdb/`
# MAGIC folder to DBFS. Use the Databricks CLI:
# MAGIC ```bash
# MAGIC databricks fs cp -r ./AlbertaSatelliteLandCover dbfs:/FileStore/wildfire_poc/landing/AlbertaSatelliteLandCover --overwrite
# MAGIC ```

# COMMAND ----------

GDB_PATH = "/dbfs/FileStore/wildfire_poc/landing/AlbertaSatelliteLandCover/Data/AlbertaSatelliteLandCover.gdb"

# List available layers in the GDB
layers = fiona.listlayers(GDB_PATH)
print(f"Layers in GDB: {layers}")

# COMMAND ----------
# MAGIC %md ## Step 2: Read land cover layer and convert to Spark

# COMMAND ----------

# Read the primary raster attribute table (land cover classes)
# The GDB contains raster + polygon layers; we read the attribute/legend table
land_cover_layer = layers[0]   # adjust index based on fiona.listlayers() output above
gdf = gpd.read_file(GDB_PATH, layer=land_cover_layer)

print(f"Columns: {list(gdf.columns)}")
print(f"CRS: {gdf.crs}")
print(gdf.head())

# COMMAND ----------

# Convert to WGS84 if needed
if gdf.crs and gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs(epsg=4326)

# Drop geometry for the attribute table (land cover class legend)
# For spatial join we keep geometry
land_cover_pdf = gdf.drop(columns=["geometry"], errors="ignore")
land_cover_spark = spark.createDataFrame(land_cover_pdf)

land_cover_spark.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("bronze", "land_cover_attributes"))

print(f"Land cover attributes ingested: {land_cover_spark.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Step 3: Spatial join — fire point to land cover polygon
# MAGIC
# MAGIC This requires spatial indexing. For a POC, we use a bounding-box
# MAGIC approximation and then a precise point-in-polygon check.

# COMMAND ----------

from shapely.geometry import Point
import pandas as pd

# Pull fire locations from silver (only those with valid coordinates)
fires_pdf = spark.table(tbl("silver", "wildfires_unified")) \
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull()) \
    .select("fire_id", "fire_year", "latitude", "longitude", "total_hectares", "general_cause") \
    .toPandas()

fires_gdf = gpd.GeoDataFrame(
    fires_pdf,
    geometry=[Point(lon, lat) for lon, lat in zip(fires_pdf["longitude"], fires_pdf["latitude"])],
    crs="EPSG:4326"
)

# Read polygon land cover layer (adjust layer name to match your GDB)
poly_layer = [l for l in layers if "polygon" in l.lower() or "landcover" in l.lower()]
if poly_layer:
    lc_polys = gpd.read_file(GDB_PATH, layer=poly_layer[0]).to_crs(epsg=4326)
    joined = gpd.sjoin(fires_gdf, lc_polys, how="left", predicate="within")
    joined_spark = spark.createDataFrame(
        joined.drop(columns=["geometry", "index_right"], errors="ignore")
    )
    joined_spark.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tbl("silver", "fires_with_land_cover"))
    print(f"Spatial join complete: {joined_spark.count():,} fire-land cover records")
else:
    print("No polygon layer found — inspect fiona.listlayers() output and adjust layer name.")

# COMMAND ----------
# MAGIC %md ## Step 4: Gold — Hectares burned by land cover class

# COMMAND ----------

if spark.catalog.tableExists(tbl("silver", "fires_with_land_cover")):
    lc_gold = spark.table(tbl("silver", "fires_with_land_cover")) \
        .groupBy("DESCRIPTION", "fire_year") \
        .agg(
            F.count("fire_id").alias("fire_count"),
            F.round(F.sum("total_hectares"), 1).alias("total_hectares_burned"),
            F.round(F.avg("total_hectares"), 2).alias("avg_fire_size_ha"),
        ).filter(F.col("DESCRIPTION").isNotNull()) \
         .orderBy(F.col("total_hectares_burned").desc())

    lc_gold.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tbl("gold", "fire_by_land_cover"))

    print("gold.fire_by_land_cover:")
    display(lc_gold.limit(15))
