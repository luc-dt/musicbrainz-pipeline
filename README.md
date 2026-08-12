# MusicBrainz ETL Pipeline

An end-to-end **scheduled batch ETL pipeline** that extracts music metadata from the MusicBrainz REST API, orchestrates the workflow with Apache Airflow, stores data in Amazon S3, and transforms nested JSON into analytics-ready CSV datasets.

---

## Introduction & Goals

This project demonstrates a production-oriented serverless ETL architecture on AWS. The MusicBrainz open-data API provides music metadata (artists, albums, recordings) without authentication requirements, allowing the project to focus entirely on data engineering fundamentals.

**Goal 1:** Extract music metadata from MusicBrainz API into raw JSON  
**How I know it worked:** Raw JSON files appear in `raw_data/to_processed/` in S3, ~5MB per run

**Goal 2:** Transform nested JSON into normalized CSV datasets  
**How I know it worked:** CSV files appear in `transformed_data/{album,artist,song}_data/` with deduplicated rows

**Goal 3:** Orchestrate the full pipeline via Apache Airflow  
**How I know it worked:** All three tasks complete with green status; DAG runs `@daily` on schedule

---

## Why This Matters

Music metadata is deeply nested in API responses—recordings contain releases, which contain artists, each with their own attributes. Direct SQL analysis requires flattening this structure. This pipeline automates that transformation, making the data queryable for analytics.

The architectural decision to use **Airflow as the orchestration layer** (rather than native EventBridge → Lambda chaining) demonstrates workflow management skills: retry logic, task dependencies, and observability in a single tool.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MUSICBRAINZ API                              │
│                     https://musicbrainz.org/ws/2                     │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │ HTTP GET (scheduled batch)
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW                                    │
│              musicbrainz_etl_dag.py  (@daily)                       │
│                                                                      │
│  trigger_extract ──▶ check_s3_file ──▶ trigger_transform           │
│       (Task 1)            (Task 2)              (Task 3)             │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
              ┌───────────────────┴───────────────────┐
              ▼                                       ▼
┌─────────────────────────────┐         ┌─────────────────────────────┐
│  AWS LAMBDA (Extract)       │         │   AWS LAMBDA (Transform)     │
│  musicbrainz-api-extract    │         │   musicbrainz_transformation │
│                             │         │   _load_function             │
│  • Fetches from API         │         │                             │
│  • Saves raw JSON to S3     │         │   • Reads raw JSON          │
│  • 5 artists per run        │         │   • Flattens to 3 CSVs      │
└─────────────┬───────────────┘         │   • Archives raw JSON       │
              │                           └──────────────┬──────────────┘
              ▼                                      │
┌─────────────────────────────────────────────────────┴───────────────┐
│                        AMAZON S3                                       │
│              bucket: musicbrainz-etl-project-luc                      │
│                                                                      │
│   raw_data/                    transformed_data/                       │
│   ├── to_processed/           ├── album_data/  (album_YYYY-MM-DD.csv) │
│   │   └── *.json              ├── artist_data/ (artist_YYYY-MM-DD.csv│
│   └── processed/              └── song_data/   (song_YYYY-MM-DD.csv) │
│       └── *.json                                                     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## The Data Set

**Source:** [MusicBrainz REST API](https://musicbrainz.org/doc/Development/XML_Web_Service/Version_2)  
**Format:** JSON → CSV  
**Scope:** 5 artists per extraction run

| Artist | Query |
|--------|-------|
| Coldplay | `artist:"Coldplay"` |
| Taylor Swift | `artist:"Taylor Swift"` |
| Dua Lipa | `artist:"Dua Lipa"` |
| James Blunt | `artist:"James Blunt"` |
| BTS | `artist:"BTS"` |

### How Much Data Is It?

- **Per run:** 5 API calls × 50 recordings max = ~250 recordings
- **Raw JSON:** ~5MB per extraction
- **Transformed CSV:** ~150KB total (album + artist + song)

---

## Constraints

- **Budget:** Operated within AWS free-tier limits (Lambda: 400K GB-s/month, S3: 5GB)
- **API Rate Limit:** MusicBrainz allows 1 request/second; the extract Lambda sleeps 1.1s between calls
- **No OAuth:** Using open MusicBrainz API 
- **Time:** Lambda timeout of 15 minutes; extraction completes in ~10 seconds

---

## Used Tools

### Connect

**Used Tool:** MusicBrainz REST API

**Why:** Open music metadata without authentication; consistent JSON structure; REST is well-understood

**Alternative:** Spotify Web API

**Why not:** Requires OAuth 2.0, rate limits are stricter, premium account needed for full data

---

### Orchestrate

**Used Tool:** Apache Airflow (Docker Compose)

**Why:** Industry-standard workflow orchestration; retry logic built-in; task dependencies via DAG; Web UI for monitoring

**Alternative:** AWS Step Functions

**Why not:** Tighter AWS lock-in; less portable; less familiar for portfolio demonstrations

---

### Ingest

**Used Tool:** AWS Lambda (Extract function)

**Why:** Serverless; scales automatically; cost-effective for periodic extractions; integrates with S3

**Alternative:** EC2 instance with cron job

**Why not:** More ops overhead; need to manage server; no built-in S3 integration

---

### Buffer

**Used Tool:** Amazon S3

**Why:** Durable object storage; integrates natively with Lambda and Glue; low cost per GB

**Alternative:** Amazon DynamoDB

**Why not:** DynamoDB is key-value, not ideal for JSON file storage; S3 is better for ETL workloads

---

### Transform

**Used Tool:** AWS Lambda (Transform function)

**Why:** Serverless compute for data transformation; Pandas for DataFrame operations; CSV output directly to S3

**Alternative:** AWS Glue with PySpark

**Why not:** Glue is designed for large-scale ETL (GB+); Lambda handles this dataset size faster and cheaper

---

### Store

**Used Tool:** Amazon S3 (same bucket, different prefixes)

**Why:** Medallion-style folder structure; raw JSON preserved; transformed CSV queryable

**Alternative:** Separate bucket per layer

**Why not:** Single bucket with prefixes is simpler for this scale; no cross-bucket data movement needed

---

## Pipelines

### Batch Processing

1. **Airflow DAG starts** (`@daily` schedule, 00:00 UTC)
2. **Trigger Extract Lambda** → Fetches 5 artists × 50 recordings → Saves `musicbrainz_raw_*.json` to S3
3. **S3KeySensor waits** → Pokes every 60s for JSON file in `raw_data/to_processed/`
4. **Trigger Transform Lambda** → Reads raw JSON → Outputs 3 CSV files → Archives raw JSON
5. **Done**

**Failure handling:** Extract Lambda has 2 retries with exponential backoff on 503 errors. Transform Lambda fails-fast if no file found. Airflow DAG has 2 task-level retries with 5-minute delay.

---

## Data Quality

| Check | Implementation |
|-------|----------------|
| Deduplication | `drop_duplicates(subset=["album_id"])` etc. in Pandas |
| Null handling | `errors="coerce"` on date parsing |
| Schema preservation | Fixed column set per output file |
| Idempotency | Re-running produces new timestamped files; raw JSON archived |

---

## Demo

*(Screenshots to be added after Power BI dashboard is implemented)*

---

## What Breaks

- **[Lambda Timeout]** If MusicBrainz API is slow, extract may exceed 15-minute Lambda limit  
  **Impact:** Partial extraction, no retry  
  **Solution:** Increase Lambda timeout or reduce `limit` parameter

- **[S3 Event Conflict]** If Lambda is triggered by both S3 Event and Airflow, duplicate processing occurs  
  **Impact:** CSV files processed twice (safe, but wasteful)  
  **Solution:** Disable S3 trigger when using Airflow orchestration

- **[API Rate Limit]** If 5 calls complete in <5 seconds, MusicBrainz may return 503  
  **Impact:** Extract fails, DAG retries  
  **Solution:** Exponential backoff decorator handles this automatically

- **[No Glue/Athena Yet]** Data is in CSV, not yet cataloged for SQL queries  
  **Impact:** Cannot query via Athena  
  **Solution:** Planned for Day 8 (Star Schema in Athena)

---

## Privacy & Security

- No PII stored; only public music metadata
- AWS credentials stored in Airflow Connections, not in code
- `.gitignore` excludes `.env` files with secrets
- MusicBrainz API requires `User-Agent` header with email (configured as Lambda env var)

---

## Conclusion

This project demonstrates core data engineering skills:

- **ETL pipeline development** (extract → transform → load)
- **Cloud architecture** (AWS Lambda, S3, IAM)
- **Workflow orchestration** (Apache Airflow DAGs)
- **Serverless patterns** (event-driven Lambda, S3 triggers)
- **Data modeling** (flattening nested JSON to relational CSVs)

### Technical Lessons

1. Lambda invocation modes matter: S3 Event vs direct invocation pass different `event` structures
2. Airflow task dependencies chain with `>>` operator; reading left-to-right means "then"
3. S3 folder structure (`to_processed/` → `processed/`) enables idempotent reprocessing
4. API rate limits require explicit backoff; the decorator pattern works well

### Next Iteration

- **Day 5:** Medallion architecture (Bronze/Silver/Gold) with Glue jobs
- **Day 7:** Data quality validation with Great Expectations
- **Day 8:** Star schema in Athena for SQL analytics
- **Day 9:** Power BI dashboard

---

## Appendix

### Repository Structure

```
musicbrainz-pipeline/
├── airflow/
│   ├── docker-compose.yaml
│   ├── .env.example
│   ├── dags/
│   │   └── musicbrainz_etl_dag.py
│   └── scripts/
├── lambda/
│   ├── extract/
│   │   └── lambda_function.py
│   └── transform/
│       └── lambda_function.py
├── images/
│   └── musicbrainz_etl_architecture.png
├── README.md
└── .gitignore
```

### Key Commands

```bash
# Start Airflow
cd airflow && docker compose up -d

# Trigger DAG manually
docker exec airflow-airflow-webserver-1 airflow dags trigger musicbrainz_etl_dag

# Check Lambda logs
aws logs tail /aws/lambda/musicbrainz-api-extract --region ap-southeast-2

# List S3 contents
aws s3 ls s3://musicbrainz-etl-project-luc/ --recursive
```

### References

- [MusicBrainz API Documentation](https://musicbrainz.org/doc/Development/XML_Web_Service/Version_2)
- [Airflow AWS Providers](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/)
- [AWS Lambda Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
