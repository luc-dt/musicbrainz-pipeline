# MusicBrainz ETL Pipeline

An end-to-end batch ETL pipeline that extracts music metadata from the MusicBrainz REST API, normalizes deeply nested JSON into analytics-ready CSV datasets, and orchestrates the workflow via Apache Airflow with AWS Lambda for compute and S3 for storage.

---

## Introduction & Goals

Music metadata from REST APIs arrives as deeply nested JSON — recordings contain releases, which contain artists, each with their own attributes. This structure is efficient for API responses but unsuitable for direct SQL analysis. The pipeline automates the extraction and normalization so analysts can query structured CSV data without writing flatten logic.

**Goal 1:** Extract music metadata from MusicBrainz API into raw JSON  
**How I know it worked:** Raw JSON files appear in `raw_data/to_processed/` in S3, ~5MB per run

**Goal 2:** Transform nested JSON into normalized CSV datasets  
**How I know it worked:** CSV files appear in `transformed_data/{album,artist,song}_data/` with deduplicated rows

**Goal 3:** Orchestrate the full pipeline via Apache Airflow  
**How I know it worked:** All three tasks complete with green status; DAG runs `@daily` on schedule

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
│   ├── to_processed/           ├── album_data/  (album_YYYY-MM-DD.csv)│
│   │   └── *.json              ├── artist_data/ (artist_YYYY-MM-DD.csv│
│   └── processed/              └── song_data/   (song_YYYY-MM-DD.csv)│
│       └── *.json                                                     │
└──────────────────────────────────────────────────────────────────────┘
```

**Flow:**
1. Airflow DAG triggers the Extract Lambda on a daily schedule
2. Extract Lambda fetches data for 5 artists from MusicBrainz API and writes raw JSON to S3
3. Airflow's S3KeySensor waits for the JSON file to appear
4. Airflow triggers the Transform Lambda
5. Transform Lambda reads raw JSON, normalizes to 3 CSV files, archives raw JSON

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

> 5 artists × 50 recordings max = 250 recordings per run  
> Each recording JSON: ~20KB (includes nested releases, artists, tags)  
> Raw JSON per run: ~5MB  
> Transformed CSV: ~150KB total (album + artist + song tables)  
> Over 1 year of daily runs: ~1.8GB raw, ~55MB transformed

---

## Constraints

- **Budget:** Operated within AWS free-tier limits (Lambda: 400K GB-s/month, S3: 5GB)
- **API Rate Limit:** MusicBrainz allows 1 request/second; the extract Lambda sleeps 1.1s between calls
- **No OAuth:** Using open MusicBrainz API — no authentication required
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

**Why:** Durable object storage; integrates natively with Lambda and Airflow; low cost per GB

**Alternative:** Amazon DynamoDB

**Why not:** DynamoDB is key-value, not ideal for JSON file storage; S3 is better for ETL workloads

---

### Transform

**Used Tool:** AWS Lambda (Transform function)

**Why:** Serverless compute for data transformation; Pandas for DataFrame operations; CSV output directly to S3

**Alternative:** AWS Glue with PySpark

**Why not:** Glue is designed for large-scale ETL (GB+); Lambda handles this dataset size faster and cheaper

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

## Sample Output

**Input** (MusicBrainz API excerpt — nested JSON):
```json
{
  "releases": [
    {
      "id": "abc123",
      "title": "A Rush of Blood to the Head",
      "date": "2002-08-26",
      "country": "GB",
      "artist-credit": [
        {
          "artist": {
            "id": "def456",
            "name": "Coldplay",
            "sort-name": "Coldplay"
          }
        }
      ]
    }
  ]
}
```

**Output** (Transformed CSV files):
```csv
# album_data/album_2024-01-15.csv
album_id,album_title,release_date,country,artist_id
abc123,A Rush of Blood to the Head,2002-08-26,GB,def456

# artist_data/artist_2024-01-15.csv
artist_id,artist_name,sort_name
def456,Coldplay,Coldplay

# song_data/song_2024-01-15.csv
recording_id,recording_title,duration_ms,album_id
rec001,Clocks,222000,abc123
rec002,The Scientist,189000,abc123
```

---

## Data Quality

| Check | Implementation |
|-------|----------------|
| Deduplication | `drop_duplicates(subset=["album_id"])` in Pandas |
| Null handling | `errors="coerce"` on date parsing |
| Schema preservation | Fixed column set per output file |
| Idempotency | Re-running produces timestamped files; raw JSON archived |

---

## Limitations

- **No automated tests** — Pipeline behavior validated manually via Airflow DAG runs. No unit or integration tests exist.
- **No data quality framework** — Deduplication via Pandas `drop_duplicates()`; no Great Expectations or similar validation library.
- **No monitoring/observability** — Pipeline health checked via Airflow Web UI task logs only; no Grafana or CloudWatch dashboards.
- **Small-scale data** — Processes 5 artists × 50 recordings per run (~250 records); not benchmarked at production volume.
- **Batch-only** — No streaming or real-time processing support.

---

## Future Work

1. **Medallion architecture** — Add Bronze/Silver/Gold layers with AWS Glue for incremental data quality enforcement.
2. **Data quality validation** — Integrate Great Expectations to catch schema and completeness issues before downstream processing.
3. **Athena integration** — Catalog CSV files in Glue Data Catalog for SQL analytics via Athena.
4. **Power BI dashboard** — Visualize artist popularity, album release trends, and song duration distributions.

---

## Quickstart

### Prerequisites

- Python 3.11+
- Docker Compose v2.0+
- AWS account with Lambda, S3, and IAM permissions

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/musicbrainz-pipeline.git
cd musicbrainz-pipeline

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Configure environment variables
cp airflow/.env.example airflow/.env
# Edit airflow/.env and set your AWS credentials:
#   AIRFLOW_VAR_AWS_ACCESS_KEY_ID=your_access_key
#   AIRFLOW_VAR_AWS_SECRET_ACCESS_KEY=your_secret_key
```

### Start Airflow

```bash
cd airflow
docker compose up -d

# Verify Airflow is running
docker compose ps

# Access Airflow UI at http://localhost:8080
# Default credentials: airflow / airflow
```

### Trigger the Pipeline

```bash
# Trigger the DAG manually
docker exec airflow-airflow-webserver-1 airflow dags trigger musicbrainz_etl_dag

# Monitor progress in Airflow UI at http://localhost:8080
```

### Verify Output

```bash
# Check Lambda logs
aws logs tail /aws/lambda/musicbrainz-api-extract --region ap-southeast-2

# List S3 contents
aws s3 ls s3://musicbrainz-etl-project-luc/ --recursive
```

---

## Testing

No automated tests exist for this pipeline. Pipeline behavior is validated manually:

1. Trigger the DAG via Airflow UI or CLI
2. Verify all three tasks complete with green status
3. Check S3 for raw JSON in `raw_data/to_processed/`
4. Check S3 for CSV files in `transformed_data/{album,artist,song}_data/`

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
├── requirements.txt
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
