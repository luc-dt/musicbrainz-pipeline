# Daily Learning Diary

## 2026-08-24 / 2026-08-25 - Day 8: Analytics Layer, Kimball Star Schema & Athena (COMPLETED ✅)

### Deep Dive: What, Why, How, and Purpose of Dimensional Modeling & Athena in Data Engineering

#### 1. WHAT is Dimensional Modeling (Kimball Star Schema)?
In enterprise data architecture, **Dimensional Modeling (Ralph Kimball Methodology)** structures data into two fundamental types of tables to optimize for human scannability and high-speed analytical queries:

1. **Conformed Master Dimensions (`dim_*`)**:
   - **Grain**: Exactly 1 entity instance per row.
   - **`dim_artist`** (Grain: 1 unique artist entity): Contains primary key `artist_id` (UUID), `artist_name`, `artist_disambiguation`, `artist_url`.
   - **`dim_album`** (Grain: 1 unique release package): Contains primary key `album_id`, foreign key `artist_id`, `album_name`, `country`, `status`, `track_count`, `release_date`.
   - **`dim_song`** (Grain: 1 unique recorded track): Contains primary key `recording_id`, foreign key `artist_id`, `title`, `length_ms`, `video`, `score`, `first_release_date`.

2. **Fact & Summary Mart Tables (`fact_*`)**:
   - **`fact_yearly_release_metrics`** (Kimball Classification: **Periodic Snapshot Fact**):
     - **Grain**: Exactly 1 calendar year $\times$ 1 country code.
     - **Measures**: `total_releases`, `avg_tracks_per_album`, `distinct_artists`.
   - **`fact_artist_summary`** (Kimball Classification: **Aggregate Data Mart / Consolidated Summary Table**):
     - **Grain**: Exactly 1 unique artist career totals.
     - **Design Rationale**: Pre-computes heavy aggregations (`total_recordings`, `avg_song_length_min`, `total_albums`, `distinct_release_countries`) at ETL write time. This gives BI dashboards sub-second response times without forcing the BI engine to perform full-table scans and runtime joins across millions of song and album records.

```
                  ┌───────────────────────────────┐
                  │       dim_artist (Silver)     │
                  │ PK: artist_id (UUID)          │
                  │ artist_name, disambiguation   │
                  └───────────────┬───────────────┘
                                  │ 1
                                  │
                                  ├──────────────────────────────┐
                                  │ N                            │ N
                  ┌───────────────▼───────────────┐   ┌──────────▼────────────────────┐
                  │       dim_album (Silver)      │   │       dim_song (Silver)       │
                  │ PK: album_id                  │   │ PK: recording_id              │
                  │ FK: artist_id (UUID)          │   │ FK: artist_id (UUID)          │
                  │ album_name, country, status   │   │ title, length_ms, video       │
                  └───────────────┬───────────────┘   └──────────┬────────────────────┘
                                  │                              │
                                  │  PySpark Silver-to-Gold      │
                                  │  (glue/silver_to_gold.py)    │
                                  ▼                              ▼
                  ┌───────────────────────────────────────────────────────────────────┐
                  │              fact_artist_summary (Gold Aggregate Mart)            │
                  │ PK/Grain: 1 unique artist_id                                      │
                  │ total_recordings, total_albums, avg_song_length_min, country_reach │
                  └───────────────────────────────────────────────────────────────────┘
```

---

#### 2. WHY Join on `artist_id` UUID instead of String `artist_search`?
⚠️ **The "Entity Collision" Disaster in Real Data Engineering:**
- When querying an open metadata database like MusicBrainz, multiple distinct musical entities share the exact same string name (e.g., **"BTS"**).
- If tables are joined on the string `artist_search = 'BTS'`:
  - **BTS (South Korean boy band)** with 196 songs and 198 albums.
  - **BTS (US rapper, Born To Spit)** with 6 songs and 1 album.
  - **BTS (Irish New Wave band)** with 1 song and 1 album.
- **The Bug**: Joining on `artist_search` collated all 3 distinct bands into a single entity with 205 songs and 202 albums, fabricating fake business metrics!
- **The Kimball Fix**: We refactored `glue/bronze_to_silver.py` to propagate the immutable **`artist_id` UUID** foreign key into `dim_album` and `dim_song`. Grouping and joining on `artist_id` guarantees mathematical accuracy.

---

#### 3. Real-World Production RCA Case Studies (Senior DE Interview Gold)

##### 🛠️ RCA Case Study 1: AWS Glue `SystemExit: 0` Failure
- **Incident**: AWS Glue Job `musicbrainz-bronze-to-silver` failed with error `SystemExit: 0` when 0 delta records were found.
- **Root Cause**: In standard Python, `sys.exit(0)` signals clean termination. However, **AWS Glue runs Spark scripts inside a Java ProcessLauncher wrapper**. The Java wrapper catches any Python exception (including `SystemExit`) and marks the entire Glue Job Run as `FAILED`.
- **Resolution**: Replaced `sys.exit(0)` with a conditional block (`if delta_count > 0: ... else: print(...)`), allowing the script to terminate naturally at EOF with exit code 0.

##### 🛠️ RCA Case Study 2: Schema Evolution & The Power of Data Quality Gates
- **Incident**: Quality Gatekeeper (`musicbrainz-data-quality`) failed with:
  `[❌ FAIL] albums -> Schema Validation: Missing columns: ['artist_id']`
- **Root Cause**: `merge_and_save()` in `bronze_to_silver.py` was reading existing historical Parquet files in S3 that were written under the old schema (which lacked `artist_id`). The union operation preserved the old schema without the newly propagated foreign key.
- **Why this was a WIN**: Our **automated Data Quality gate caught the schema discrepancy before corrupted data reached the Gold layer**!
- **Resolution**: Cleared legacy S3 Parquet tables, reset the incremental watermark to epoch `1970-01-01`, and re-executed a clean backfill. All 24 quality checks passed immediately (`SUCCEEDED`).

##### 🛠️ RCA Case Study 3: Incremental State Machine S3 Key Isolation
- **Incident**: Rebuild job processed 0 records despite reset command.
- **Root Cause**: `watermark_manager.py` persists state at S3 key `state/watermarks.json` with payload structure `{"bronze_to_silver": {"last_processed_timestamp": "..."}}`. The reset script initially targeted `metadata/watermarks.json`.
- **Resolution**: Aligned the CLI reset script to target `state/watermarks.json`, successfully triggering the full historical backfill.

---

#### 4. Live Verification & Analytical Evidence (Amazon Athena):

All 5 analytical queries executed flawlessly against the live Athena catalog (`musicbrainz_dw`):

1. **Query 1: Artist 360 Leaderboard (Entity Disambiguation Verified)**:
   - **Taylor Swift**: 276 recordings, 1,499 albums, 50 countries, avg song: 4.59 min.
   - **James Blunt**: 216 recordings, 368 albums, 35 countries, avg song: 4.07 min.
   - **Dua Lipa**: 212 recordings, 375 albums, 35 countries, avg song: 3.81 min.
   - **Coldplay**: 200 recordings, 169 albums, 21 countries, avg song: 4.79 min.
   - **BTS (South Korean boy group)**: 196 recordings, 198 albums, 7 countries (correctly isolated!).
   - **BTS (US rapper, Born To Spit)**: 6 recordings, 1 album, 1 country (correctly isolated!).
   - **BTS (Irish New Wave band)**: 1 recording, 1 album, 0 countries (correctly isolated!).
2. **Query 2: Global Music Release Trends**:
   - Top markets: **US (333 releases)**, **XW/Worldwide (274)**, **GB (216)**, **JP (180)**, **CA (175)**.
3. **Query 3: Outlier & Extended Duration Analysis**:
   - *The Eras Tour* concert film: **169.0 minutes** (longest recorded item).
   - In the Studio With James Blunt: **30.05 minutes**.
4. **Query 4: Career Lifespan & Release Velocity**:
   - **Coldplay**: 24-year career span (1998–2022), 7.04 albums/year.
   - **Taylor Swift**: 23-year career span (2003–2026), 65.17 releases/year (driven by re-recordings & international editions).
5. **Query 5: Star Schema Dimensional Join**:
   - Joined `dim_artist` $\times$ `dim_album` on `artist_id` FK: Taylor Swift has 1,150 Official releases, 152 Withdrawn, 118 Bootleg, 75 Promotion.

---

#### 5. Senior DE Interview Cheat Sheet (STAR Method)

**Q: "Tell me about a time you designed an analytics lakehouse layer."**
> *"In my MusicBrainz project, I implemented Kimball dimensional modeling over AWS S3 using PySpark and AWS Glue. In the Silver layer, I created 3 conformed dimensions (`dim_artist`, `dim_album`, `dim_song`) with strict grain definitions. To eliminate runtime join bottlenecks for BI dashboards, I engineered an Aggregate Business Mart (`fact_artist_summary`) in the Gold layer that pre-aggregates catalog totals. I registered the data in AWS Glue Data Catalog and exposed it through Amazon Athena, cutting dashboard query latency to sub-second times."*

**Q: "How do you handle entity resolution and foreign key integrity in a distributed data lake?"**
> *"I noticed that joining on string artist names caused entity collisions when multiple artists shared the same name (like South Korean BTS vs US BTS). I refactored the PySpark extraction logic to unnest the nested JSON `artist-credit` struct, propagate the immutable `artist_id` UUID as a foreign key across albums and songs, and group aggregations strictly by `artist_id`. Furthermore, our automated Data Quality gate asserts that `artist_id` is 100% non-null and matches schema types before allowing downstream Gold aggregations."*

---

## 2026-08-23 / 2026-08-24 - Day 7: Data Quality Validation (COMPLETED ✅)

### Deep Dive: What, Why, How, and Purpose of Data Quality in Data Engineering

#### 1. WHAT is Data Quality Validation?
In simple terms: **Data Quality is the automated "Security & Customs Checkpoint" of your data pipeline.**

```
BRONZE (Raw JSON)
   │
   ▼
SILVER (Cleaned Parquet)
   │
   ├──► 🛑 [ DATA QUALITY GATE ] ◄── (Are columns valid? Any null IDs? Negative durations?)
   │         │
   │         ├── If PASS ✅ ──► GOLD LAYER (Power BI / Executives)
   │         └── If FAIL ❌ ──► STOP PIPELINE & ALERT ON-CALL ENGINEER 🚨
```
Instead of blindly assuming the data transformed correctly, Data Quality runs automated assertions and checks against the Silver data before allowing it to reach business dashboards or ML models.

#### 2. WHY is Data Quality Essential?
⚠️ **The "Silent Pipeline Failure" Nightmare (Real-World Industry Scenario):**
1. The MusicBrainz API suddenly changes a field name from `id` to `artist_uuid`.
2. PySpark doesn't crash — it just extracts `NULL` for every artist ID.
3. The Airflow DAG turns **GREEN** (success).
4. Two weeks later, the VP of Analytics opens Power BI and sees `Total Artists: 0`.
5. **Result**: The Data Engineering team loses the trust of the entire company.

> 💡 **The Golden Rule of Data Engineering:**
> *"It is 100x better for a pipeline to FAIL visibly with a clear error than to SUCCEED silently with corrupted data."* — **Garbage In, Garbage Out (GIGO)**

#### 3. HOW Do We Validate Data? (The 5 Core Dimensions)
In professional data platforms, we test data across 5 fundamental dimensions:

| Dimension | Rule | Why it matters |
| :--- | :--- | :--- |
| **Completeness** | `artist_id IS NOT NULL` | An artist dimension without an ID breaks all table joins. |
| **Uniqueness** | `COUNT(recording_id) == COUNT(DISTINCT recording_id)` | Duplicate song records double-count song metrics in Power BI. |
| **Validity / Range** | `length_ms > 0` and `length_ms < 3600000` | A song cannot have negative length or be 50 hours long. |
| **Schema Integrity** | `track_count` is `IntegerType` | If strings slip into numeric columns, Power BI aggregations fail. |
| **Volume Check** | `row_count > 0` | Ensures upstream extraction didn't produce an empty dataset. |

#### 4. PURPOSE in a Real DE Project
- **For the Business**: Guaranteed trust in reports. Decisions are made on validated, accurate metrics.
- **For the Engineers**: Instant root-cause alerts when APIs change; zero manual database auditing.
- **For Interviews**: Shows you build **production-grade enterprise pipelines**, not just toy scripts. Most junior engineers only know ETL; senior engineers build **ETL + Observability + Quality Gates**.

#### 5. What We Built & Tested:
1. **`DataQualityChecker` Engine (`glue/data_quality.py`)**:
   - `check_row_count()`: Verifies non-empty outputs.
   - `check_schema()`: Validates column names and types against `EXPECTED_SCHEMAS`.
   - `check_non_null()`: Asserts 0 nulls on critical business keys.
   - `check_uniqueness()`: Enforces Primary Key uniqueness.
   - `check_range()`: Enforces physical bounds (durations, scores, track counts).
   - **Quality Gate Summary**: Evaluates pass/fail rates and halts with `sys.exit(1)` on violations (Fail-Fast pattern).
2. **Unit Test Suite (`tests/test_data_quality.py`)**:
   - Tested both happy paths and failure edge cases with `pytest` on synthetic mock data (5/5 passed).
3. **Airflow Orchestration (`airflow/dags/musicbrainz_etl_dag.py`)**:
   - Inserted `glue_data_quality` task between Silver and Gold layers.

### Verification & Test Results:
- **Pytest Unit Test Suite**: **5 out of 5 tests PASSED in 2.39s ✅**.
- **Live AWS Glue Pipeline Execution**:
  - `musicbrainz-bronze-to-silver`: **SUCCEEDED ✅**
  - `musicbrainz-data-quality`: **SUCCEEDED (24/24 Rules Passed) ✅**
  - `musicbrainz-silver-to-gold`: **SUCCEEDED ✅**
  - **Airflow DAG Run**: `state: success` 🎯

---

### 🛠️ Production Troubleshooting & Root Cause Analysis (RCA) Case Studies

During live end-to-end testing of the Medallion Pipeline in AWS Glue & Airflow, we encountered and resolved 3 real-world data engineering production issues:

#### 📌 Case Study 1: Missing S3 Bootstrap Asset (`LAUNCH ERROR`)
- **Symptom**: Airflow triggered `glue_data_quality`, which failed instantly on AWS Glue.
- **Root Cause**: `data_quality.py` was developed locally and not yet synced to `s3://musicbrainz-etl-project-luc/scripts/data_quality.py`.
- **Diagnosis Command**:
  ```bash
  aws glue get-job-run --job-name musicbrainz-data-quality --run-id <run_id> --query "JobRun.ErrorMessage"
  # Output: "LAUNCH ERROR | Error downloading from S3 ... key: scripts/data_quality.py"
  ```
- **Resolution**: Synced all Glue scripts to S3 using `aws s3 sync glue/ s3://musicbrainz-etl-project-luc/scripts/`.

#### 📌 Case Study 2: Distributed Glue Worker Dependency (`ModuleNotFoundError: watermark_manager`)
- **Symptom**: `glue_bronze_to_silver` failed during Spark execution on AWS Glue.
- **Root Cause**: On AWS Glue worker nodes, only the primary script defined in `script_location` is downloaded. Auxiliary Python modules (like `watermark_manager.py`) are not in the container's `sys.path` by default.
- **Diagnosis Command**:
  ```bash
  aws glue get-job-run --job-name musicbrainz-bronze-to-silver --run-id <run_id> --query "JobRun.ErrorMessage"
  # Output: "ModuleNotFoundError: No module named 'watermark_manager'"
  ```
- **Resolution**: Configured the AWS Glue standard `--extra-py-files` parameter in `airflow/dags/musicbrainz_etl_dag.py`:
  ```python
  glue_bronze_to_silver = GlueJobOperator(
      task_id='glue_bronze_to_silver',
      job_name='musicbrainz-bronze-to-silver',
      script_location='s3://musicbrainz-etl-project-luc/scripts/bronze_to_silver.py',
      script_args={
          '--extra-py-files': 's3://musicbrainz-etl-project-luc/scripts/watermark_manager.py'
      },
      ...
  )
  ```

#### 📌 Case Study 3: Real-World Data Anomaly & Domain Rule Tuning
- **Symptom**: `glue_data_quality` executed on live S3 data and triggered the Fail-Fast circuit breaker (`SystemExit: 1`), halting downstream Gold tasks.
- **Investigation Process**:
  1. Inspected CloudWatch log group `/aws-glue/jobs/output` in `ap-southeast-2`:
     ```text
     ❌ [songs] Range Check (length_ms >= 1000 and <= 3600000): Found 1 out-of-bound values!
     ```
  2. Queried the live S3 Parquet table using `boto3` and `pandas` to isolate the violating record:
     ```text
     recording_id: d07aade8-b659-4ac5-b767-5bcc75e47394
     title       : The Eras Tour (Taylor Swift)
     length_ms   : 10,140,000 ms (~2 hours 49 minutes)
     ```
- **Root Cause**: Our initial quality assumption assumed all songs were $< 1$ hour (`3,600,000 ms`). However, MusicBrainz indexes live concert recordings, tour films, and DJ sets that exceed 1 hour.
- **Resolution**:
  - Adjusted the business rule boundary in `glue/data_quality.py` to allow up to 5 hours (`18,000,000 ms`):
    ```python
    dq.check_range(songs_df, "songs", col_name="length_ms", min_val=1000, max_val=18000000)
    ```
  - Re-synced to S3 (`aws s3 sync`).
  - Result: All 24 rules passed (100%), allowing `glue_silver_to_gold` to execute successfully!

---

### 🔧 Production Data Engineering Troubleshooting Cheatsheet / Runbook

Keep these commands handy for fast pipeline diagnostics:

```bash
# 1. Quick Status Check across all 3 AWS Glue Jobs
python -c "import boto3; glue = boto3.client('glue', region_name='ap-southeast-2'); [print(f'{j:<32} -> {glue.get_job_runs(JobName=j, MaxResults=1)[\"JobRuns\"][0][\"JobRunState\"]}') for j in ['musicbrainz-bronze-to-silver', 'musicbrainz-data-quality', 'musicbrainz-silver-to-gold']]"

# 2. Get the High-Level Error Message from a Failed Glue Job Run
aws glue get-job-run --job-name <job_name> --run-id <run_id> --query "JobRun.ErrorMessage"

# 3. Read Real-Time Output & Tracebacks from CloudWatch Logs (ap-southeast-2)
python -c "import boto3; logs = boto3.client('logs', region_name='ap-southeast-2'); [print(e['message']) for s in logs.describe_log_streams(logGroupName='/aws-glue/jobs/output', logStreamNamePrefix='<run_id>')['logStreams'] for e in logs.get_log_events(logGroupName='/aws-glue/jobs/output', logStreamName=s['logStreamName'])['events']]"

# 4. Check Airflow DAG Run Status Table
docker exec airflow-airflow-webserver-1 airflow dags list-runs -d musicbrainz_etl_dag -o table

# 5. Inspect Live S3 Parquet Tables Directly (Bypass local Hadoop/Spark)
python -c "import io, boto3, pandas as pd; s3 = boto3.client('s3', region_name='ap-southeast-2'); objs = s3.list_objects_v2(Bucket='musicbrainz-etl-project-luc', Prefix='silver/songs/').get('Contents', []); [print(pd.read_parquet(io.BytesIO(s3.get_object(Bucket='musicbrainz-etl-project-luc', Key=o['Key'])['Body'].read())).head()) for o in objs if o['Key'].endswith('.parquet')]"
```

---

## 2026-08-22 / 2026-08-23 - Day 6: Incremental Loading & Watermarking (COMPLETED ✅)

### Deep Dive: What, Why, How, and Purpose

#### 1. WHAT is Incremental Loading & Watermarking?
Incremental loading is a pattern where the pipeline processes only **new or updated records** that arrived since the last successful execution, rather than reprocessing the entire historical dataset.
A **watermark** is a stateful checkpoint storing the maximum timestamp (`last_processed_timestamp`) successfully written to downstream storage.

```
BRONZE (Raw JSON)
   │
   ▼ Read Watermark (e.g., 2026-08-12 22:00:00)
   ├── Filter: extracted_at > watermark
   │    ├── delta_count == 0 ──► ⚡ EARLY EXIT (Save compute)
   │    └── delta_count > 0  ──► Transform Delta Records
   │
   ▼ Idempotent Merge (unionByName + dropDuplicates)
SILVER (Parquet Tables Updated Without Duplicates)
   │
   ▼ Atomic State Commit
WATERMARK STORE (watermarks.json updated to new max timestamp)
```

#### 2. WHY is It Essential?
- **Cost & Performance Scaling**: Re-reading 1,000,000 historical files every day to process 100 new rows is slow, expensive, and wasteful on cloud compute (Glue DPUs / EC2).
- **Idempotency**: Retrying a failed Airflow task or running historical backfills must not double-count or duplicate rows in Silver tables.
- **Fault Tolerance**: Updating state *only at the very end* ensures zero data loss if a write fails midway.

#### 3. HOW Did We Build It?
- **State Store (`glue/watermark_manager.py`)**: Hybrid JSON manager reading/writing to AWS S3 (`s3://.../state/watermarks.json`) or local disk (`data/state/watermarks.json`).
- **Delta Filtering (`glue/bronze_to_silver.py`)**: Filtered Bronze dataframe on `to_timestamp(col("extracted_at")) > to_timestamp(lit(last_watermark))`.
- **Early-Exit Optimization**: Terminated cleanly with `sys.exit(0)` when `delta_count == 0`.
- **Idempotent Merge (`merge_and_save`)**: Read existing Parquet, applied `unionByName(delta_df)`, deduplicated on Primary Keys (`artist_id`, `album_id`, `recording_id`), and overwrote destination.
- **Atomic State Commit**: Called `wm.update_watermark("bronze_to_silver", max_extracted_at)` only after all tables were written.

#### 4. PURPOSE in a Real DE Project
- **Scalability**: Enables the pipeline to run hourly or daily with sub-minute execution times.
- **Cost Efficiency**: Minimizes Glue DPU billing hours on AWS.
- **Robustness**: Prevents duplicate records during pipeline retries and network disruptions.

### Verification & Test Results:
- **Run 1 (Initial Load)**: Ingested 5 raw records, merged & deduplicated 6 artists, 1,323 albums, 250 songs, and advanced watermark to `2026-08-12 22:11:33.935796`.
- **Run 2 (Idempotency Check)**: Re-ran pipeline immediately; detected 0 new records and exited cleanly in < 2 seconds.
- **Gold Analytics**: Generated 6 artist summary rows and 341 yearly release trend rows.

---

## 2026-08-22 - Day 5: Medallion Architecture (Bronze → Silver → Gold with PySpark & Glue)

### Deep Dive: What, Why, How, and Purpose

#### 1. WHAT is the Medallion Architecture?
A multi-tier data design pattern that organizes a data lakehouse into three distinct layers of increasing data refinement:
1. **Bronze (Raw)**: Immutable, raw semi-structured JSON directly from APIs.
2. **Silver (Cleaned & Conformed)**: Relational, deduplicated, typed columnar Parquet tables (Dimensions & Facts).
3. **Gold (Aggregated Analytics)**: High-level business KPI summary tables optimized for Athena SQL and Power BI dashboards.

```
BRONZE LAYER (Raw API JSON in S3 / local)
       │
       ▼  PySpark Transformation (glue/bronze_to_silver.py)
       ├── explode_outer (recordings, artist-credit, releases)
       ├── Schema validation & field extraction
       ├── Multi-format date parsing (length-based normalization)
       ├── Entity deduplication by Primary Key
       │
       ▼
SILVER LAYER (Snappy Parquet in data/silver/)
       ├── artists/ (6 rows - Dimension)
       ├── albums/  (1,323 rows - Dimension)
       └── songs/   (250 rows - Fact)
       │
       ▼  PySpark Aggregations (glue/silver_to_gold.py)
       ├── artist_summary: Artist 360 KPIs (song count, avg duration min, album count, country reach)
       └── yearly_release_metrics: Global release trends by year and country (341 rows)
       │
       ▼
GOLD LAYER (Snappy Parquet in data/gold/ -> Athena & Power BI Ready)
```

#### 2. WHY is It Essential?
- **Decoupled Responsibilities**: Raw audit data is preserved unchanged; downstream transformations are isolated from upstream API quirks.
- **Columnar Performance**: Parquet format with Snappy compression reduces storage by 80% and query times by 10x compared to scanning raw JSON.
- **Pre-Aggregated Business Value**: Executives and BI dashboards query pre-calculated KPIs sub-second without executing expensive runtime joins across millions of rows.

#### 3. HOW Did We Build It?
- **PySpark ETL (`glue/bronze_to_silver.py`)**: Used `explode_outer()` to unnest multi-level arrays without dropping orphan parents; built length-based date parsing for partial strings (`'2001'`, `'2001-05'`).
- **Gold Business Layer (`glue/silver_to_gold.py`)**: Calculated `artist_summary` (lifetime songs, career span, country reach) and `yearly_release_metrics` (global trends by year and country).
- **Airflow Glue Integration**: Configured `GlueJobOperator` tasks with IAM roles and exponential backoff.

#### 4. PURPOSE in a Real DE Project
- **Dimensional Modeling**: Separates descriptive dimensions (`artists`, `albums`) from measurable facts (`songs`).
- **Query Cost Optimization**: Athena charges by data scanned; columnar Parquet scanning ensures queries cost pennies.

---

## 2026-08-20 - Day 4: Production Features (TaskGroups, Retries, Logging, Email Alerts)

### Deep Dive: What, Why, How, and Purpose

#### 1. WHAT are Production Pipeline Features?
Production engineering transforms a basic script into a **resilient, observable, and self-healing system** through structured task grouping, exponential backoff retries, error logging callbacks, and automated email alerting.

```
TaskGroup (Extract) ───► S3KeySensor ───► TaskGroup (Transform)
   │                           │                     │
   ▼                           ▼                     ▼
Retries (Backoff)           Timeout               Alerts (Gmail SMTP on Failure)
```

#### 2. WHY is It Essential?
- **APIs & Networks Fail**: External APIs (like MusicBrainz) return intermittent 503s, rate limits (429), or drop connections under load.
- **Prevent Thundering Herd**: Fixed retries hammer struggling servers; exponential backoff (`2m -> 4m -> 8m`) gives downstream systems time to recover.
- **Zero Silent Failures**: Engineers must be alerted immediately via email/Slack when terminal failures occur, without suffering alert fatigue from temporary retries.

#### 3. HOW Did We Build It?
- **TaskGroups**: Grouped DAG into `extract_data` and `transform_medallion` in `musicbrainz_etl_dag.py`.
- **Exponential Backoff**: Configured `retries=3`, `retry_delay=timedelta(minutes=2)`, `retry_exponential_backoff=True`.
- **Failure Alerting**: Connected Gmail SMTP over TLS (`AIRFLOW__SMTP__*`) in `docker-compose.yaml` with `on_failure_callback=notify_error`.

#### 4. PURPOSE in a Real DE Project
- **Observability**: Complete audit trail of errors with stack traces in Airflow logs.
- **Operational SLA**: Guarantees pipeline recovery without requiring manual engineer restarts for transient glitches.

---

## 2026-08-13 - Day 3: Complete ETL Pipeline & S3 Event Sensing

### Deep Dive: What, Why, How, and Purpose

#### 1. WHAT is Pipeline End-to-End Orchestration?
Connecting disparate cloud components (Extraction Lambda, Amazon S3 storage, S3 Sensors, and Transformation Lambda) into an automated, dependency-driven workflow.

```
Extract Lambda → S3 (raw_data/to_processed/) → S3KeySensor → Transform Lambda → S3 (transformed_data/)
```

#### 2. WHY is It Essential?
- **Asynchronous Decoupling**: Extraction and transformation have different compute and memory requirements; running them as decoupled steps prevents monolithic memory crashes.
- **Event Coordination**: Sensors ensure transformation never runs on missing or half-uploaded files.

#### 3. HOW Did We Build It?
- **S3KeySensor**: Polled S3 bucket every 60 seconds with wildcard matching (`raw_data/to_processed/*.json`).
- **Dual-Mode Lambda**: Enabled Lambda handler to process direct Airflow triggers as well as native S3 trigger events.
- **Data Verification**: Verified generated CSV entity files (`album_data/`, `artist_data/`, `song_data/`) in S3.

#### 4. PURPOSE in a Real DE Project
- **Pipeline Integrity**: Guarantees strict ordering of data processing stages.
- **Audit Archiving**: Raw JSON moved from `to_processed/` to `processed/` after successful transformation for historical compliance.

---

## 2026-08-12 - Day 2: Airflow Orchestration - AWS Lambda & S3

### Deep Dive: What, Why, How, and Purpose

#### 1. WHAT is Cloud Orchestration with Airflow?
Using Apache Airflow as an external control plane to trigger serverless AWS services (Lambda, S3) rather than executing heavy compute inside the Airflow worker containers.

#### 2. WHY is It Essential?
- **Separation of Concerns**: Airflow is an *orchestrator*, not a data processing engine. Running heavy transformations directly on the Airflow scheduler causes container crashes and scheduler lag.
- **Serverless Scalability**: AWS Lambda and Glue scale out dynamically on AWS infrastructure.

#### 3. HOW Did We Build It?
- **Airflow AWS Connection**: Configured `aws_default` connection with region `ap-southeast-2` and secure IAM credentials.
- **Lambda Operators**: Used `AwsLambdaInvokeFunctionOperator` to remotely invoke extraction Lambdas.
- **Security & Hygiene**: Configured `.gitignore` to protect `.env` secrets and credentials.

#### 4. PURPOSE in a Real DE Project
- **Enterprise Architecture**: Industry-standard pattern of Airflow as the lightweight scheduler orchestrating heavy cloud services.

---

## 2026-08-11 - Day 1: Containerized Airflow Infrastructure Setup

### Deep Dive: What, Why, How, and Purpose

#### 1. WHAT is Containerized Airflow Setup?
Running Apache Airflow locally using Docker and Docker Compose, spinning up isolated services for the Webserver, Scheduler, Postgres metadata database, and Triggerer.

#### 2. WHY is It Essential?
- **Reproducibility**: Eliminates "works on my machine" bugs across team members and operating systems (Windows, Mac, Linux).
- **Production Parity**: Local Docker setup mirrors cloud environments like AWS MWAA (Managed Workflows for Apache Airflow) and Google Cloud Composer.

#### 3. HOW Did We Build It?
- **Docker Compose**: Orchestrated `postgres`, `airflow-webserver`, and `airflow-scheduler` containers.
- **Hello World DAG**: Built and verified `musicbrainz_hello.py` to confirm scheduler execution and Web UI connectivity.

#### 4. PURPOSE in a Real DE Project
- **Development Foundation**: Provides a safe, isolated sandbox to develop and test DAGs before cloud deployment.
