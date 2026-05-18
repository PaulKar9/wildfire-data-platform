-- Unity Catalog Setup (paid Databricks tier only)
-- Run as account admin in a SQL Warehouse

-- 1. Create catalog
CREATE CATALOG IF NOT EXISTS wildfire_poc
  COMMENT 'Alberta Historical Wildfire Data Platform — POC';

-- 2. Create schemas
CREATE SCHEMA IF NOT EXISTS wildfire_poc.bronze
  COMMENT 'Raw ingestion layer — immutable, append-only';
CREATE SCHEMA IF NOT EXISTS wildfire_poc.silver
  COMMENT 'Cleansed and unified layer — standardized schema';
CREATE SCHEMA IF NOT EXISTS wildfire_poc.gold
  COMMENT 'Curated analytics layer — BI-ready aggregations';
CREATE SCHEMA IF NOT EXISTS wildfire_poc.meta
  COMMENT 'Pipeline metadata and data quality metrics';

-- 3. Create groups (map to your IdP groups)
CREATE GROUP IF NOT EXISTS data_engineer;
CREATE GROUP IF NOT EXISTS data_analyst;
CREATE GROUP IF NOT EXISTS fire_ops_viewer;
CREATE GROUP IF NOT EXISTS bi_service_account;

-- 4. Grant catalog access
GRANT USE CATALOG ON CATALOG wildfire_poc TO data_engineer;
GRANT USE CATALOG ON CATALOG wildfire_poc TO data_analyst;
GRANT USE CATALOG ON CATALOG wildfire_poc TO fire_ops_viewer;
GRANT USE CATALOG ON CATALOG wildfire_poc TO bi_service_account;

-- 5. Schema-level grants
GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA wildfire_poc.bronze TO data_engineer;
GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA wildfire_poc.silver TO data_engineer;
GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA wildfire_poc.gold  TO data_engineer;
GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA wildfire_poc.meta  TO data_engineer;

GRANT USE SCHEMA ON SCHEMA wildfire_poc.silver TO data_analyst;
GRANT USE SCHEMA ON SCHEMA wildfire_poc.gold   TO data_analyst;
GRANT USE SCHEMA ON SCHEMA wildfire_poc.meta   TO data_analyst;

GRANT USE SCHEMA ON SCHEMA wildfire_poc.gold   TO fire_ops_viewer;
GRANT USE SCHEMA ON SCHEMA wildfire_poc.gold   TO bi_service_account;

-- 6. Table-level grants (gold layer is widely accessible)
GRANT SELECT ON ALL TABLES IN SCHEMA wildfire_poc.gold TO data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA wildfire_poc.gold TO bi_service_account;

-- fire_ops_viewer: only regional patterns and season data (not raw or top fires)
GRANT SELECT ON TABLE wildfire_poc.gold.regional_fire_patterns  TO fire_ops_viewer;
GRANT SELECT ON TABLE wildfire_poc.gold.seasonal_distribution   TO fire_ops_viewer;
GRANT SELECT ON TABLE wildfire_poc.gold.fire_season_length      TO fire_ops_viewer;
GRANT SELECT ON TABLE wildfire_poc.gold.annual_fire_statistics  TO fire_ops_viewer;

-- 7. Row filter for silver (analysts see all; fire_ops see own region only)
-- Requires Unity Catalog row filters (Databricks Runtime 12.2+)
CREATE OR REPLACE FUNCTION wildfire_poc.silver.region_filter(fire_centre STRING)
  RETURN is_account_group_member('data_engineer')
      OR is_account_group_member('data_analyst')
      OR fire_centre = session_context('user_region');

ALTER TABLE wildfire_poc.silver.wildfires_unified
  SET ROW FILTER wildfire_poc.silver.region_filter ON (fire_centre);

-- 8. Audit: verify grants
SHOW GRANTS ON CATALOG wildfire_poc;
