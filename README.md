# Alberta Wildfire Modern Data Platform — Databricks POC

**Data Architect:** POC for Fire Operations Team  
**Dataset:** Alberta Open Data — Historical Wildfire Data 1961–2025  
**Platform:** Databricks Lakehouse (Medallion Architecture)  
**Target Cost:** < $10 CAD for full POC run  

---

## Dataset Inventory (67,508 fire records across 65 years)

| File | Period | Rows | Schema |
|------|--------|------|--------|
| `af-historic-wildfires-1961-1982-data.csv` | 1961–1982 | 15,983 | 74 cols, numeric codes |
| `af-historic-wildfires-1983-1995-data.csv` | 1983–1995 | 12,345 | 94 cols, numeric codes |
| `af-historic-wildfires-1996-2005-data.csv` | 1996–2005 | 11,352 | 39 cols, text descriptions |
| `fp-historical-wildfire-data-2006-2025.csv` | 2006–2025 | 27,828 | 50 cols, full weather data |
| `AlbertaSatelliteLandCover.zip` | — | Raster/Vector | ESRI GDB — land cover enrichment |
| *(4 PDF data dictionaries)* | — | — | Column definitions per era |

---

## Quick Start

### 1. Prerequisites
- Databricks account (Community Edition = free, or Trial with $400 USD credits)
- No cloud storage account needed for Community Edition (uses DBFS)

### 2. Cluster Setup (Cost-Optimized)
Import `config/cluster_config.json` or create manually:
- **Runtime:** 14.3 LTS (Spark 3.5, Scala 2.12)
- **Node type:** Single Node (no workers)
- **Driver:** `Standard_DS3_v2` (Azure) or `m5.xlarge` (AWS) — smallest available
- **Auto-terminate:** 30 minutes
- **Spot/preemptible:** enabled

Estimated cost: ~$0.07–0.15 CAD/hour on smallest paid tier. Community Edition = $0.

### 3. Run Order
```
notebooks/00_setup.py            → Create catalog, schemas, configure paths
notebooks/01_bronze_ingestion.py → Ingest raw Alberta wildfire CSV
notebooks/02_silver_transform.py → Cleanse, standardize, enrich
notebooks/03_gold_analytics.py   → Build curated analytical tables
notebooks/04_data_quality.py     → Validate pipeline health
```

### 4. Dataset Source
Alberta Open Data Portal — Historical Wildfire Data (1961–2025):  
https://open.alberta.ca/opendata/alberta-wildfire-data

---

## Architecture Summary

```
Alberta Open Data (CSV/API)
         │
    ┌────▼─────┐
    │  BRONZE  │  Raw ingestion, immutable, append-only
    └────┬─────┘
         │  Schema enforcement, null handling, deduplication
    ┌────▼─────┐
    │  SILVER  │  Cleansed, typed, standardized, geospatially enriched
    └────┬─────┘
         │  Business logic, aggregations, KPIs
    ┌────▼─────┐
    │   GOLD   │  BI-ready, pre-aggregated, domain-oriented
    └────┬─────┘
         │
    ┌────▼──────────────────┐
    │  SQL Warehouse / BI   │  Power BI, Tableau, SQL Endpoint
    └───────────────────────┘
```

---

## Cost Control Checklist
- [ ] Cluster auto-terminates after 30 min idle
- [ ] Use single-node cluster (no workers)
- [ ] Use spot/preemptible instances
- [ ] Use DBFS for storage (no ADLS/S3 cost)
- [ ] Serverless SQL Warehouse off by default
- [ ] Set cluster policy to cap DBU spend
