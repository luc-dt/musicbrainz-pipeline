# MusicBrainz Pipeline - 10-Day Plan

## Final Architecture (Target - Day 10)

```
MUSICBRAINZ API
      │
      ▼
AWS LAMBDA (Ingestion - musicbrainz-api-extract)
      │
      ▼
AMAZON S3 → BRONZE (Raw JSON)
      │
      ▼
AWS GLUE
(Bronze → Silver → Gold)
      │
      ▼
GLUE DATA CATALOG
      │
      ▼
AMAZON ATHENA (Querying)
      │
      ▼
POWER BI (Visualization)
      │
      │
APACHE AIRFLOW ← Orchestrates everything
```

## Current Architecture (Day 5 ✅)

```
MUSICBRAINZ API
      │
      ▼
AWS LAMBDA (Extract: musicbrainz-api-extract)
      │
      ▼
AMAZON S3 → BRONZE LAYER (raw_data/to_processed/*.json)
      │
      ▼
APACHE AIRFLOW (musicbrainz_etl_dag.py)
  ├── TaskGroup (extract_data): LambdaInvoke
  └── TaskGroup (transform_medallion): S3KeySensor ──► GlueJob (Bronze->Silver) ──► GlueJob (Silver->Gold)
      │
      ▼
AWS GLUE (PySpark Distributed Compute)
  ├── Job 1: musicbrainz-bronze-to-silver ──► AMAZON S3 → SILVER LAYER (silver/{artists, albums, songs}/)
  └── Job 2: musicbrainz-silver-to-gold   ──► AMAZON S3 → GOLD LAYER (gold/{artist_summary, yearly_metrics}/)
```

## This Project Demonstrates

```
✅ Orchestration (Airflow)
✅ Ingestion (Lambda)
✅ Storage (S3 + Parquet)
✅ Transformation (Glue PySpark)
✅ Cataloging (Glue Crawler)
✅ Querying (Athena)
✅ Validation (Data Quality)
✅ Visualization (Power BI)
✅ CI/CD (GitHub Actions)
✅ Production patterns (retries, notifications, incremental)
```

---

## Day-by-Day Plan

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MUSIC PIPELINE - 10 DAY PLAN                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DAY 1  │ Airflow Setup                                                   │
│          │ ✅ Docker, Docker Compose, Airflow UI                           │
│          │ ✅ First DAG (musicbrainz_hello.py)                            │
│          │ ✅ Git commit: "feat: initialize Airflow"                       │
│                                                                              │
│  DAY 2  │ Connect Airflow to AWS                                          │
│          │ ✅ Add AWS Connection in Airflow (aws_default)                  │
│          │ ✅ Create musicbrainz_etl_dag.py                                │
│          │ ✅ LambdaInvokeFunctionOperator (trigger extract Lambda)          │
│          │ ✅ S3KeySensor (wait for raw file)                             │
│          │ ✅ Trigger existing Lambda from Airflow                         │
│          │ ✅ Git commit: "feat: connect Airflow to AWS Lambda"            │
│          │ ✅ Bonus: Retry logic (3 retries extract, 20 retries sensor)    │
│                                                                              │
│  DAY 3  │ Complete ETL DAG                                                │
│          │ ✅ Lambda → S3 Sensor → Transform Lambda (end-to-end)           │
│          │ ✅ Test full pipeline                                           │
│          │ ✅ Verify data in S3                                           │
│          │ ✅ Git commit: "feat: complete ETL DAG with S3 sensor"         │
│                                                                              │
│  DAY 4  │ Production Features                                             │
│          │ ✅ TaskGroups (organize related tasks)                          │
│          │ ✅ Retries (handle Lambda failures with exponential backoff)    │
│          │ ✅ Error handling & logging (notify_error callback)             │
│          │ ✅ Email notifications (Airflow SMTP + Gmail alerts)            │
│          │ ✅ Git commit: "feat: add production features"                  │
│                                                                              │
│  DAY 5  │ Medallion Architecture (Bronze → Silver → Gold)                 │
│          │ ✅ COMPLETED                                                    │
│          │ ✅ Understand medallion layers:                                 │
│          │   • Bronze = raw JSON from API (Lambda stores this)             │
│          │   • Silver = cleaned, deduplicated Parquet (Glue / PySpark)     │
│          │   • Gold = aggregated business metrics (Glue / PySpark)         │
│          │ ✅ Step 1: bronze_to_silver.py (PySpark ETL)                     │
│          │   • Double explode_outer, date normalization, Parquet output    │
│          │ ✅ Step 2: silver_to_gold.py (Gold business analytics)          │
│          │   • artist_summary (Artist 360 KPIs)                            │
│          │   • yearly_release_metrics (Yearly & Country trends)            │
│          │ ✅ Step 3: Upgraded Airflow DAG with GlueJobOperator            │
│          │ ✅ Step 4: Deployed resilient Extract Lambda to AWS             │
│          │ ✅ Git commit: "feat: implement medallion architecture"         │
│                                                                              │
│  DAY 6  │ Incremental Loading (COMPLETED ✅)                              │
│          │ ✅ Step 1: Watermark Manager (glue/watermark_manager.py)        │
│          │   • Hybrid S3 & local JSON state store (state/watermarks.json)  │
│          │   • Verified get/update watermark functionality                 │
│          │ ✅ Step 2: Delta Filtering & Early-Exit Guard (bronze_to_silver)│
│          │ ✅ Step 3: Idempotent Merge/Upsert (merge_and_save union logic) │
│          │ ✅ Step 4: Propagate to Gold & verify end-to-end idempotency    │
│          │ □ Git commit: "feat: add incremental loading"                  │
│                                                                              │
│  DAY 7  │ Data Quality (COMPLETED ✅)                                     │
│          │ ✅ Schema validation (expected columns & data types)            │
│          │ ✅ Null checks (required fields non-empty)                     │
│          │ ✅ Duplicate detection & primary key uniqueness                │
│          │ ✅ Row count & value range validation (bounds checks)           │
│          │ ✅ Unit tests with pytest (5/5 passed)                          │
│          │ ✅ Airflow task for data quality gate (glue_data_quality)       │
│          │ □ Git commit: "feat: add data quality validation"             │
│                                                                              │
│  DAY 8  │ Analytics Layer (Star Schema - COMPLETED ✅)                   │
│          │ ✅ Dimensional modeling in Athena (Kimball Star Schema)         │
│          │   • dim_artist (name, sort name, search alias, URL)           │
│          │   • dim_album (name, country, status, release date, tracks)   │
│          │   • dim_song (title, length, video flag, score)               │
│          │   • fact_artist_summary (Career aggregates)                   │
│          │   • fact_yearly_release_metrics (Year x Country trends)       │
│          │ ✅ AWS Glue Data Catalog setup (DDL external Parquet tables)   │
│          │ ✅ Executed 5 core business analytical SQL queries in Athena   │
│          │ □ Git commit: "feat: add star schema analytics"                │
│                                                                              │
│  DAY 9  │ Power BI Dashboard                                             │
│          │ □ Connect Power BI to Athena via ODBC                         │
│          │ □ Build visualizations:                                        │
│          │   • Top genres by artist                                      │
│          │   • Releases by year                                          │
│          │   • Artists by country                                        │
│          │   • Recordings by label                                       │
│          │ □ Screenshot dashboard for README                              │
│          │ □ Git commit: "feat: add Power BI dashboard"                  │
│                                                                              │
│  DAY 10 │ IaC + CI/CD + Documentation                                    │
│          │ □ AWS SAM template.yaml (Infrastructure as Code):              │
│          │   • S3 buckets                                               │
│          │   • Lambda functions                                          │
│          │   • Glue jobs & crawlers                                      │
│          │   • IAM roles                                                 │
│          │ □ GitHub Actions:                                             │
│          │   • Lint DAG files                                            │
│          │   • Test Airflow syntax                                       │
│          │   • Deploy on push                                           │
│          │ □ README.md:                                                  │
│          │   • Architecture diagram                                      │
│          │   • DAG screenshot                                            │
│          │   • Power BI screenshot                                       │
│          │   • Setup instructions                                        │
│          │   • "sam deploy --guided" command                             │
│          │ □ Git commit: "feat: add IaC, CI/CD, and documentation"     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Architecture Details

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE      │ Raw JSON from MusicBrainz API               │
│              │ • Lambda stores as-is                       │
│              │ • Not modified                             │
├──────────────┼────────────────────────────────────────────┤
│  SILVER      │ Cleaned, deduplicated data                 │
│              │ • Convert JSON → Parquet                   │
│              │ • Remove duplicates                        │
│              │ • Schema enforcement                       │
├──────────────┼────────────────────────────────────────────┤
│  GOLD        │ Aggregated business metrics                │
│              │ • artist_summary                           │
│              │ • release_summary                          │
│              │ • recording_stats                          │
└──────────────┴────────────────────────────────────────────┘
```

### Lambda vs Glue

```
┌─────────────────────────────────────────────────────────────┐
│  LAMBDA (Extract)          │ Keep existing                │
│                            │ • Already works              │
│                            │ • Airflow triggers it        │
├────────────────────────────┼───────────────────────────────┤
│  GLUE (Transform)          │ Replace with 3 jobs          │
│                            │ • bronze_job                 │
│                            │ • silver_job                 │
│                            │ • gold_job                   │
└────────────────────────────┴───────────────────────────────┘
```

---

## Project Skills Demonstrated

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SKILLS THIS PROJECT DEMONSTRATES                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  CLOUD & INFRASTRUCTURE                                                     │
│  ├── AWS Lambda (serverless compute) ← ✅ Existing                         │
│  ├── Amazon S3 (data lake) ← ✅ Existing                                 │
│  ├── AWS Glue (ETL/PySpark) ← NEW                                         │
│  ├── Amazon Athena (serverless SQL) ← NEW                                  │
│  ├── AWS IAM (security) ← ✅ Existing                                     │
│  └── AWS SAM (Infrastructure as Code) ← NEW                               │
│                                                                              │
│  ORCHESTRATION                                                              │
│  ├── Apache Airflow (workflow management) ← ✅ Day 1                       │
│  ├── Task dependencies & scheduling                                         │
│  └── Retry logic & error handling                                          │
│                                                                              │
│  DATA ENGINEERING                                                           │
│  ├── Medallion architecture (Bronze/Silver/Gold) ← NEW                     │
│  ├── Incremental data loading ← NEW                                        │
│  ├── Data quality validation ← NEW                                        │
│  ├── Dimensional modeling (star schema) ← NEW                              │
│  └── ETL pipeline development ← ✅ Existing                                │
│                                                                              │
│  DEVOPS & DEPLOYMENT                                                       │
│  ├── Docker & Docker Compose ← ✅ Day 1                                     │
│  ├── GitHub Actions (CI/CD) ← NEW                                         │
│  └── Infrastructure as Code (AWS SAM) ← NEW                                 │
│                                                                              │
│  VISUALIZATION                                                              │
│  └── Power BI dashboards ← NEW                                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Questions (Answered by Each Day)

| Question | Answered On |
|----------|-------------|
| Why Airflow instead of EventBridge? | Day 1-3 |
| Why Lambda for ingestion? | Day 2 |
| Why store raw JSON in S3? | Day 3 |
| Why convert to Parquet? | Day 5 |
| Why Bronze → Silver → Gold? | Day 5 |
| Why use Glue Crawler? | Day 5 |
| Why Athena instead of a database? | Day 8 |
| How does incremental loading work? | Day 6 |
| How to recover from failed Glue job? | Day 4 |
| How is the pipeline idempotent? | Day 6 |

---

## Progress Summary

| Day | Status | Completed |
|-----|--------|-----------|
| Day 1 | ✅ DONE | Airflow setup, first DAG |
| Day 2 | ✅ DONE | Connect to AWS, ETL DAG, Lambda orchestration + retry logic |
| Day 3 | ✅ DONE | Complete ETL DAG, end-to-end testing, verify S3 data |
| Day 4 | ✅ DONE | Production features (TaskGroups, retries/backoff, logging, email alerts) |
| Day 5 | ✅ DONE | Medallion architecture (Bronze -> Silver -> Gold PySpark ETL) |
| Day 6 | ✅ DONE | Incremental loading & watermarking (Idempotent merge) |
| Day 7 | ✅ DONE | Data quality validation (PySpark assertions + pytest unit tests) |
| Day 8 | ✅ DONE | Analytics Layer & Star Schema in Athena (DDL + 5 BI SQL queries) |
| Day 9 | ⬜ TODO | Power BI dashboard |
| Day 10 | ⬜ TODO | IaC + CI/CD + README |

---

## Next Step

Ready to continue with **Day 9: Power BI Dashboard**? 🚀

**Day 9 Checklist:**
1. Connect Power BI to Amazon Athena (ODBC / Athena Connector)
2. Build Executive Visualizations:
   - Artist 360 catalog breakdown (Recordings vs Albums vs Duration)
   - Global release map & country distribution
   - Release trends by year / decade
3. Export / document dashboard screenshots and report metrics
4. Git commit: "feat: add power bi dashboard"
