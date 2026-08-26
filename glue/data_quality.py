"""
========================================================================================
MUSICBRAINZ ETL PIPELINE: DATA QUALITY VALIDATION GATE (PySpark)
========================================================================================
Medallion Architecture - Silver Layer Quality Assurance
Purpose:
  Validates the integrity, completeness, uniqueness, and schema conformity of
  conformed Silver Parquet tables (artists, albums, songs) before allowing downstream
  Gold layer aggregations to execute.
Quality Dimensions Tested:
  1. Row Count / Volume (No empty outputs)
  2. Schema Integrity (Expected columns and data types)
  3. Completeness (Zero nulls in Primary Keys & mandatory business fields)
  4. Uniqueness / Primary Key Integrity (Zero duplicate IDs)
  5. Validity & Range (Length > 0 ms, Score between 0-100, Track count >= 1)
========================================================================================
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when, lit
from pyspark.sql.types import *

# 1. Data Quality Engine Class (Importable by Unit Tests)

# 3. Data Quality Engine Class
class DataQualityChecker:
    """
    Executes automated data quality assertions on PySpark DataFrames
    and collects structured pass/fail results.
    """
    def __init__(self):
        self.results = []
    
    def _log_result(self, table_name, check_name, status, details):
        self.results.append({
            "table": table_name,
            "check": check_name,
            "status": status,
            "details": details
        })
        icon = "✅ PASS" if status == "PASS" else "❌ FAIL"
        print(f"  [{icon}] {table_name} -> {check_name}: {details}")

    # Check 1: Row count / Volume validation (no empty outputs)
    def check_row_count(self, df, table_name, min_expected=1):
        actual_count = df.count()
        if actual_count >= min_expected:
            self._log_result(table_name, "Row Count Validation", "PASS", f"{actual_count} rows found (>= {min_expected})")
        else:
            self._log_result(table_name, "Row Count Validation", "FAIL", f"Found {actual_count} rows, expected at least {min_expected}")

    # Check 2: Schema integrity (column names and data types)
    def check_schema(self, df, table_name, expected_schema):
        actual_dtypes = dict(df.dtypes)
        missing_cols = []
        mismatched_types = []

        for col_name, expected_type in expected_schema.items():
            if col_name not in actual_dtypes:
                missing_cols.append(col_name)
            elif actual_dtypes[col_name] != expected_type:
                mismatched_types.append(f"{col_name} (expected {expected_type}, got {actual_dtypes[col_name]})")

        if not missing_cols and not mismatched_types:
            self._log_result(table_name, "Schema Validaton", "PASS", f"All {len(expected_schema)} columns & data types match")
        else:
            err_msg = ""
            if missing_cols:
               err_msg += f"Missing columns: {missing_cols}. " 
            if mismatched_types:
                err_msg += f"Mismatched types: {mismatched_types}."
            self._log_result(table_name, "Schema Validation", "FAIL", err_msg)

    # Check 3: Null checks on mandatory fields
    def check_non_null(self, df, table_name, required_columns):
        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count == 0:
                self._log_result(table_name, f"Null Check ({col_name})", "PASS", "0 null values found")
            else:
                self._log_result(table_name, f"Null Check ({col_name})", "FAIL", f"Found {null_count} null rows!")
    
    # Check 4: Primary key uniqueness
    def check_uniqueness(self, df, table_name, primary_keys):
        total_rows = df.count()
        unique_rows = df.select(primary_keys).distinct().count()
        duplicate_count = total_rows - unique_rows

        if duplicate_count == 0:
            self._log_result(table_name, f"Uniqueness Check ({', '.join(primary_keys)})", "PASS", f"All {total_rows} rows are unique")
        else:
            self._log_result(table_name, f"Uniqueness Check ({', '.join(primary_keys)})", "FAIL", f"Found {duplicate_count} duplicate rows!")

    # Check 5: Range / Value bounds validation
    def check_range(self, df, table_name, col_name, min_val=None, max_val=None):
        condition = lit(False)
        rule_desc = []
        if min_val is not None:
            condition = condition | (col(col_name) < min_val)
            rule_desc.append(f">= {min_val}")
        if max_val is not None:
            condition = condition | (col(col_name) > max_val)
            rule_desc.append(f"<= {max_val}")
        invalid_count = df.filter(condition).count()
        rule_str = " and ".join(rule_desc)
        if invalid_count == 0:
            self._log_result(table_name, f"Range Check ({col_name} {rule_str})", "PASS", "All values within valid range")
        else:
            self._log_result(table_name, f"Range Check ({col_name} {rule_str})", "FAIL", f"Found {invalid_count} out-of-bound values!")
        



def run_data_quality_suite(spark_session=None):
    """
    Executes the complete data quality suite across all Silver tables.
    """
    spark = spark_session or (
        SparkSession.builder
        .appName("MusicBrainz_Data_Quality_Gate")
        .getOrCreate()
    )

    IS_GLUE = "GLUE_COMMAND_CRITERIA" in os.environ or "AWS_EXECUTION_ENV" in os.environ or "JOB_NAME" in os.environ

    if IS_GLUE:
        S3_BUCKET = "s3://musicbrainz-etl-project-luc"
        silver_base = f"{S3_BUCKET}/silver"
    else:
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
        silver_base = os.path.join(PROJECT_ROOT, "data", "silver").replace("\\", "/")

    print(f"📊 [Data Quality Engine] Target Silver Base: {silver_base}")

    print("\n" + "=" * 50)
    print("🔍 RUNNING DATA QUALITY TEST SUITE")
    print("=" * 50)

    artists_df = spark.read.parquet(f"{silver_base}/artists")
    albums_df  = spark.read.parquet(f"{silver_base}/albums")
    songs_df   = spark.read.parquet(f"{silver_base}/songs")

    # ----------------------------------------------------
    # Test Check 1: Initialize Checker & (Row Count)
    # ----------------------------------------------------
    dq = DataQualityChecker()

    print("\n--- Test Check 1: Row Count Validation ---")
    dq.check_row_count(artists_df, "artists", min_expected=1)
    dq.check_row_count(albums_df,  "albums",  min_expected=10)
    dq.check_row_count(songs_df,   "songs",   min_expected=50)

    # ----------------------------------------------------
    # Test Check 2: Schema integrity 
    # ----------------------------------------------------
    EXPECTED_SCHEMAS = {
        "artists": {
            "artist_id": "string",
            "artist_name": "string",
            "artist_sort_name": "string",
            "artist_disambiguation": "string",
            "artist_search": "string",
            "extracted_at": "timestamp",
            "artist_url": "string"
        },
        "albums": {
            "album_id": "string",
            "artist_id": "string",
            "album_name": "string",
            "country": "string",
            "status": "string",
            "track_count": "int",
            "artist_search": "string",
            "extracted_at": "timestamp",
            "release_date": "date",
            "album_url": "string"
        },
        "songs": {
            "recording_id": "string",
            "artist_id": "string",
            "title": "string",
            "length_ms": "bigint",
            "video": "boolean",
            "score": "int",
            "artist_search": "string",
            "extracted_at": "timestamp",
            "first_release_date": "date"
        }
    }

    print("\n--- Test Check 2: Schema integrity ---")
    dq.check_schema(artists_df, "artists", EXPECTED_SCHEMAS["artists"])
    dq.check_schema(albums_df,  "albums",  EXPECTED_SCHEMAS["albums"])
    dq.check_schema(songs_df,   "songs",   EXPECTED_SCHEMAS["songs"])

    # ----------------------------------------------------
    # Test Check 3: Null Checks on Mandatory Fields
    # ----------------------------------------------------
    print("\n--- Test Check 3: Null Checks on Required Fields ---")
    dq.check_non_null(artists_df, "artists", required_columns=["artist_id", "artist_name", "artist_search", "extracted_at"])
    dq.check_non_null(albums_df,  "albums",  required_columns=["album_id", "album_name", "artist_search", "extracted_at"])
    dq.check_non_null(songs_df,   "songs",   required_columns=["recording_id", "title", "artist_search", "extracted_at"])

    # ----------------------------------------------------
    # Test Check 4: Primary Key Uniqueness
    # ----------------------------------------------------
    print("\n--- Test Check 4: Primary Key Uniqueness ---")
    dq.check_uniqueness(artists_df, "artists", primary_keys=["artist_id"])
    dq.check_uniqueness(albums_df,  "albums",  primary_keys=["album_id"])
    dq.check_uniqueness(songs_df,   "songs",   primary_keys=["recording_id"])

    # ----------------------------------------------------
    # Test Check 5: Range & Value Bounds Validation
    # ----------------------------------------------------
    print("\n--- Test Check 5: Range & Value Bounds ---")
    dq.check_range(albums_df, "albums", col_name="track_count", min_val=1)
    dq.check_range(songs_df,  "songs",  col_name="length_ms", min_val=1000, max_val=18000000)
    dq.check_range(songs_df,  "songs",  col_name="score", min_val=0, max_val=100)

    # ====================================================
    # 6. Data Quality Gate: Summary & Fail-Fast Decision
    # ====================================================
    print("\n" + "=" * 50)
    print("📊 DATA QUALITY GATE SUMMARY")
    print("=" * 50)

    total_checks = len(dq.results)
    passed_checks = sum(1 for r in dq.results if r["status"] == "PASS")
    failed_checks = total_checks - passed_checks

    print(f"Total Rules Evaluated : {total_checks}")
    print(f"Passed Rules          : {passed_checks} ✅")
    print(f"Failed Rules          : {failed_checks} {'❌' if failed_checks > 0 else ''}")

    if failed_checks > 0:
        print("\n🚨 CRITICAL ERROR: Data Quality Gate FAILED!")
        print("The following quality checks failed:")
        for r in dq.results:
            if r["status"] == "FAIL":
                print(f"  ❌ [{r['table']}] {r['check']}: {r['details']}")
        print("\nStopping pipeline to prevent corrupted data from entering Gold layer.")
        sys.exit(1)
    else:
        print("\n🎉 ALL QUALITY GATES PASSED! Silver layer is certified clean for Gold analytics.")


if __name__ == "__main__":
    run_data_quality_suite()


