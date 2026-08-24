"""
Unit Tests for PySpark Data Quality Checker
Tests both happy paths (PASS) and failure edge cases (FAIL).
"""

import os
import sys

# 1. Windows PySpark environment configuration (MUST BE SET BEFORE IMPORTING PYSPARK)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Ensure glue modules can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "glue")))
from data_quality import DataQualityChecker


@pytest.fixture(scope="session")
def spark():
    """Session-scoped PySpark test fixture"""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("DataQuality_UnitTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def checker():
    """Fresh DataQualityChecker instance for each test"""
    return DataQualityChecker()


# ----------------------------------------------------
# 1. Test Row Count Validation
# ----------------------------------------------------
def test_check_row_count_pass_and_fail(spark, checker):
    # Pass: 2 rows >= 1
    df_pass = spark.createDataFrame([("art_1", "Coldplay"), ("art_2", "BTS")], ["artist_id", "artist_name"])
    checker.check_row_count(df_pass, "test_table", min_expected=1)
    assert checker.results[-1]["status"] == "PASS"

    # Fail: 0 rows < 1
    empty_schema = StructType([StructField("artist_id", StringType(), True)])
    df_empty = spark.createDataFrame([], empty_schema)
    checker.check_row_count(df_empty, "test_table", min_expected=1)
    assert checker.results[-1]["status"] == "FAIL"


# ----------------------------------------------------
# 2. Test Null Check on Required Fields
# ----------------------------------------------------
def test_check_non_null_pass_and_fail(spark, checker):
    # Pass: No nulls
    df_clean = spark.createDataFrame([("id_1", "Song A"), ("id_2", "Song B")], ["recording_id", "title"])
    checker.check_non_null(df_clean, "songs", required_columns=["recording_id"])
    assert checker.results[-1]["status"] == "PASS"

    # Fail: 1 null ID
    df_corrupted = spark.createDataFrame([(None, "Song A"), ("id_2", "Song B")], ["recording_id", "title"])
    checker.check_non_null(df_corrupted, "songs", required_columns=["recording_id"])
    assert checker.results[-1]["status"] == "FAIL"


# ----------------------------------------------------
# 3. Test Primary Key Uniqueness
# ----------------------------------------------------
def test_check_uniqueness_pass_and_fail(spark, checker):
    # Pass: Unique IDs
    df_unique = spark.createDataFrame([("alb_1",), ("alb_2",)], ["album_id"])
    checker.check_uniqueness(df_unique, "albums", primary_keys=["album_id"])
    assert checker.results[-1]["status"] == "PASS"

    # Fail: Duplicate IDs ('alb_1' appears twice)
    df_duplicate = spark.createDataFrame([("alb_1",), ("alb_1",)], ["album_id"])
    checker.check_uniqueness(df_duplicate, "albums", primary_keys=["album_id"])
    assert checker.results[-1]["status"] == "FAIL"


# ----------------------------------------------------
# 4. Test Range / Value Bounds Validation
# ----------------------------------------------------
def test_check_range_pass_and_fail(spark, checker):
    # Pass: Valid track counts (10, 15)
    df_valid = spark.createDataFrame([(10,), (15,)], ["track_count"])
    checker.check_range(df_valid, "albums", col_name="track_count", min_val=1)
    assert checker.results[-1]["status"] == "PASS"

    # Fail: Track count of 0 or negative (-5)
    df_invalid = spark.createDataFrame([(0,), (-5,)], ["track_count"])
    checker.check_range(df_invalid, "albums", col_name="track_count", min_val=1)
    assert checker.results[-1]["status"] == "FAIL"


# ----------------------------------------------------
# 5. Test Schema Validation
# ----------------------------------------------------
def test_check_schema_pass_and_fail(spark, checker):
    expected_schema = {"artist_id": "string", "score": "int"}
    schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("score", IntegerType(), True)
    ])

    # Pass: Explicitly typed matching schema
    df_matching = spark.createDataFrame([("id_1", 100)], schema)
    checker.check_schema(df_matching, "artists", expected_schema)
    assert checker.results[-1]["status"] == "PASS"

    # Fail: Missing 'score' column
    df_missing_col = spark.createDataFrame([("id_1",)], ["artist_id"])
    checker.check_schema(df_missing_col, "artists", expected_schema)
    assert checker.results[-1]["status"] == "FAIL"
