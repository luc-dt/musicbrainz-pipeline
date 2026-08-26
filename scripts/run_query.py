"""
========================================================================================
CLI SQL RUNNER FOR AMAZON ATHENA (Like psql / sqlcmd)
========================================================================================
Usage:
  1. Run inline query:
     python scripts/run_query.py "SELECT * FROM musicbrainz_dw.dim_artist LIMIT 5;"

  2. Run queries from a .sql file:
     python scripts/run_query.py sql/analytics_queries.sql
========================================================================================
"""

import os
import sys
import time
import boto3

REGION = os.getenv("AWS_REGION", "ap-southeast-2")
S3_BUCKET = os.getenv("S3_BUCKET", "musicbrainz-etl-project-luc")
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
DATABASE = os.getenv("ATHENA_DATABASE", "musicbrainz_dw")

athena = boto3.client("athena", region_name=REGION)


def execute_sql(query_str: str):
    """Executes a single SQL query on Athena and displays results."""
    # Strip comments to check if empty
    clean_lines = [l for l in query_str.splitlines() if not l.strip().startswith("--")]
    clean_sql = "\n".join(clean_lines).strip()
    if not clean_sql:
        return

    # Extract comment title if available
    comment_title = ""
    for l in query_str.splitlines():
        if l.strip().startswith("-- Query"):
            comment_title = l.strip().lstrip("-").strip()
            break

    print("\n" + "=" * 90)
    if comment_title:
        print(f"📊 {comment_title}")
    else:
        print(f"SQL: {clean_sql[:75]}...")
    print("=" * 90)

    # 1. Start execution
    resp = athena.start_query_execution(
        QueryString=clean_sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    q_id = resp["QueryExecutionId"]

    # 2. Poll until complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=q_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ["FAILED", "CANCELLED"]:
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            print(f"❌ Error [{q_id}]: {reason}")
            return
        time.sleep(0.5)

    # 3. Fetch results
    results = athena.get_query_results(QueryExecutionId=q_id)
    rows = results["ResultSet"]["Rows"]
    if not rows:
        print("  (No rows returned)")
        return

    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
    data = []
    for r in rows[1:]:
        data.append([col.get("VarCharValue", "NULL") for col in r["Data"]])

    # 4. Print clean ASCII table
    col_widths = [len(h) for h in headers]
    for r in data:
        for i, val in enumerate(r):
            col_widths[i] = max(col_widths[i], len(str(val)))

    header_line = " | ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    sep_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    print(header_line)
    print(sep_line)
    for r in data:
        print(" | ".join(f"{str(val):<{col_widths[i]}}" for i, val in enumerate(r)))
    print(f"({len(data)} rows)\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_query.py <'SQL string' or path/to/file.sql>")
        sys.exit(1)

    target = sys.argv[1]

    # Check if target is a .sql file
    if os.path.exists(target) and target.endswith(".sql"):
        with open(target, "r", encoding="utf-8") as f:
            content = f.read()
        queries = [q.strip() for q in content.split(";") if q.strip()]
        for q in queries:
            execute_sql(q)
    else:
        execute_sql(target)


if __name__ == "__main__":
    main()
