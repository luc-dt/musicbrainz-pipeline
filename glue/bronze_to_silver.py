"""
========================================================================================
MUSICBRAINZ ETL PIPELINE: BRONZE TO SILVER TRANSFORMATION (PySpark)
========================================================================================
Medallion Architecture - Silver Layer Processing

Purpose:
  Reads raw, deeply nested semi-structured JSON music metadata extracted from the
  MusicBrainz REST API (Bronze Layer), normalizes and cleans the records, enforces
  relational schemas, and writes conformed Snappy-compressed Parquet tables into
  the Silver Layer.

What this script accomplishes:
  1. Ingestion (Bronze -> Spark):
     - Reads raw multi-line JSON files from `airflow/data/raw/*.json` (or S3).
  2. Transformation & Entity Normalization (3 Relational Datasets):
     - ARTISTS (Dimension): Explodes recordings & artist-credits, extracts artist
       UUIDs, names, sort-names, disambiguations, generates URLs, and deduplicates.
     - ALBUMS (Dimension): Explodes recordings & releases, normalizes heterogeneous
       release dates ('YYYY', 'YYYY-MM', 'YYYY-MM-DD') using length-based date parsing,
       casts track counts to integers, generates URLs, and deduplicates.
     - SONGS (Fact): Explodes recordings, casts metrics (length_ms: LongType,
       video: BooleanType, score: IntegerType), normalizes first release dates,
       and deduplicates by recording_id.
  3. Storage & Idempotency (Silver Layer):
     - Writes partitioned datasets to `data/silver/{artists, albums, songs}/` as
       Snappy-compressed columnar Parquet files with overwrite mode for safe reruns.

Architecture Flow:
  Bronze (Raw JSON) ---> PySpark (bronze_to_silver.py) ---> Silver (Parquet Tables)
========================================================================================
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    explode_outer,
    to_date,
    to_timestamp,
    coalesce,
    lit,
    concat,
    length,
    when,
    max as spark_max              # to find the latest timestamp in the batch
)

from pyspark.sql.types import IntegerType, LongType, BooleanType

# Ensure Python can find watermark_manager.py whether running locally or on AWS Glue
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from watermark_manager import WatermarkManager


# 1. Create a local Spark session
spark = (
    SparkSession.builder
    .appName("MusicBrainz_Bronze_to_Silver")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

# 2. Dynamic Paths (Hybrid Local & AWS Glue S3)
IS_GLUE = "GLUE_COMMAND_CRITERIA" in os.environ or "AWS_EXECUTION_ENV" in os.environ or "JOB_NAME" in os.environ

if IS_GLUE:
    S3_BUCKET = "s3://musicbrainz-etl-project-luc"
    bronze_path = f"{S3_BUCKET}/raw_data/to_processed/*.json"
    silver_base = f"{S3_BUCKET}/silver"
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
    bronze_path = os.path.join(PROJECT_ROOT, "airflow", "data", "raw", "*.json").replace("\\", "/")
    silver_base = os.path.join(PROJECT_ROOT, "data", "silver").replace("\\", "/")

# 3. Initialize Watermark Manager and State (Step 1)
wm = WatermarkManager()
last_watermark = wm.get_watermark("bronze_to_silver", default="1970-01-01 00:00:00")
print(f"\n📊 [Incremental Engine] Active watermark for 'bronze_to_silver': {last_watermark}")

# 4. Read the raw JSON files from Bronze
bronze_df = spark.read.option("multiline", "true").json(bronze_path)
# # Let's inspect the raw schema Spark inferred
# bronze_df.printSchema()

# 5. Filter for Delta Records (Step 2)
delta_df = bronze_df.filter(
    to_timestamp(col("extracted_at")) > to_timestamp(lit(last_watermark))
)
delta_count = delta_df.count()
print(f"📊 [Incremental Engine] Found {delta_count} new bronze records since {last_watermark}")

# Early exit if no new data arrived
if delta_count == 0:
    print("✨ No new records to process. Pipeline is up to date. Exiting cleanly.")
    spark.stop()
    sys.exit(0)

# Calculate the new max timestamp from incoming batch
max_extracted_at = (
    delta_df
    .select(spark_max(to_timestamp(col("extracted_at"))).cast("string"))
    .collect()[0][0]
)
print(f"🎯 [Incremental Engine] Batch maximum extracted_at: {max_extracted_at}\n")

# 6. Transform Artists (Dimension Table)
def transform_artists(df):
    """
    Extract, flatten, and deduplicate artists into a conformed Silver table.
    """
    # Step 1: Explode recording array
    recording_df = df.select(
        col("artist_search"),
        to_timestamp(col("extracted_at")).alias("extracted_at"),
        explode_outer(col("data.recordings")).alias("recording")
    )

    # Step 2: Explode artist-credit array inside each recording
    artist_credit_df = recording_df.select(
        col("artist_search"),
        col("extracted_at"),
        explode_outer(col("recording.artist-credit")).alias("artist_credit")
    )

    # Step 3: Extract artist struct fields
    artists_raw = artist_credit_df.select(
        col("artist_credit.artist.id").alias("artist_id"),
        col("artist_credit.artist.name").alias("artist_name"),
        col("artist_credit.artist.sort-name").alias("artist_sort_name"),
        col("artist_credit.artist.disambiguation").alias("artist_disambiguation"),
        col("artist_search"),
        col("extracted_at")
    )

    # Step 4: Filter nulls, add URL, and deduplicatee
    artists_clean = (
        artists_raw
        .filter(col("artist_id").isNotNull())
        .withColumn(
            "artist_url",
            concat(lit("https://musicbrainz.org/artist/"), col("artist_id"))
        )
        .dropDuplicates(["artist_id"])
    )

    return artists_clean

# 6. Transform Albums (Dimension Table)
def transform_albums(df):
    """
    Extract, flatten, parse dates, and deduplicate albums into a conformed Silver table.
    """
    # Step 1: Explode recording array
    recording_df = df.select(
        col("artist_search"),
        to_timestamp(col("extracted_at")).alias("extracted_at"),
        explode_outer(col("data.recordings")).alias("recording")
    )

    # Step 2: Explode release array inside each recording
    release_df = recording_df.select(
        col("artist_search"),
        col("extracted_at"),
        explode_outer(col("recording.releases")).alias("release")
    )

    # Step 3: Extract album fields
    albums_raw = release_df.select(
        col("release.id").alias("album_id"),
        col("release.title").alias("album_name"),
        col("release.date").alias("release_date_raw"),
        col("release.country").alias("country"),
        col("release.status").alias("status"),
        col("release.track-count").cast(IntegerType()).alias("track_count"),
        col("artist_search"),
        col("extracted_at")
    )

    # Step 4: Clean dates, build URL, and deduplicate
    # Normalize partial dates (e.g., '2001' -> '2001-01-01', '2001-05' -> '2001-05-01')
    normalized_date = (
        when(length(col("release_date_raw")) == 10, col("release_date_raw"))
        .when(length(col("release_date_raw")) == 7, concat(col("release_date_raw"), lit("-01")))
        .when(length(col("release_date_raw")) == 4, concat(col("release_date_raw"), lit("-01-01")))
        .otherwise(None)
    )
    albums_clean = (
        albums_raw
        .filter(col("album_id").isNotNull())
        .withColumn("release_date", to_date(normalized_date, "yyyy-MM-dd"))
        .withColumn(
            "album_url",
            concat(lit("https://musicbrainz.org/release/"), col("album_id"))
        )
        .drop("release_date_raw")
        .dropDuplicates(["album_id"])
    )

    return albums_clean


# 7. Transform Songs (Fact Table)
def transform_songs(df):
    """
    Extract, cast types, clean dates, and deduplicate songs into a conformed Silver table.
    """
    # Step 1: Explode recordings array
    recordings_df = df.select(
        col("artist_search"),
        to_timestamp(col("extracted_at")).alias("extracted_at"),
        explode_outer(col("data.recordings")).alias("recording")
    )

    # Step 2: Extract song fields & cast types
    songs_raw = recordings_df.select(
        col("recording.id").alias("recording_id"),
        col("recording.title").alias("title"),
        col("recording.length").cast(LongType()).alias("length_ms"),
        col("recording.video").cast(BooleanType()).alias("video"),
        col("recording.score").cast(IntegerType()).alias("score"),
        col("recording.first-release-date").alias("first_release_date_raw"),
        col("artist_search"),
        col("extracted_at")
    )

    # Step 3: Clean first release date
    normalized_date = (
        when(length(col("first_release_date_raw")) == 10, col("first_release_date_raw"))
        .when(length(col("first_release_date_raw")) == 7, concat(col("first_release_date_raw"), lit("-01")))
        .when(length(col("first_release_date_raw")) == 4, concat(col("first_release_date_raw"), lit("-01-01")))
        .otherwise(None)
    )

    songs_clean = (
        songs_raw
        .filter(col("recording_id").isNotNull())
        .withColumn("first_release_date", to_date(normalized_date, "yyyy-MM-dd"))
        .drop("first_release_date_raw")
        .dropDuplicates(["recording_id"])
    )

    return songs_clean

# 8. Helper: Idempotent Merge & Save Function (Step 3)
def merge_and_save(delta_df, target_path, primary_keys):
    """
    Unions delta records with historical Parquet data, deduplicates on primary key,
    and overwrites the target directory safely.
    """
    try:
        existing_df = spark.read.parquet(target_path)
        combined_df = existing_df.unionByName(delta_df)
        final_df = combined_df.dropDuplicates(primary_keys)
        print(f"   ↳ Merged: {existing_df.count()} existing + {delta_df.count()} delta -> {final_df.count()} unique rows")
    except Exception:
        final_df = delta_df.dropDuplicates(primary_keys)
        print(f"   ↳ Initial write: {final_df.count()} rows")

    final_df.write.mode("overwrite").parquet(target_path)

# 9. Execute All Transformations & Save to Parquet

print("\n" + "=" * 50)
print("🚀 EXECUTING BRONZE TO SILVER PIPELINE")
print("=" * 50)

print("1. Transforming Delta Artists...")
artists_df = transform_artists(delta_df)

print("2. Transforming Delta Albums...")
albums_df = transform_albums(delta_df)

print("3. Transforming Delta Songs...")
songs_df = transform_songs(delta_df)


# Output paths
artists_out = f"{silver_base}/artists"
albums_out = f"{silver_base}/albums"
songs_out = f"{silver_base}/songs"

print(f"\n1. Writing Silver Artists -> {artists_out}")
merge_and_save(artists_df, artists_out, primary_keys=["artist_id"])
print(f"2. Writing Silver Albums  -> {albums_out}")
merge_and_save(albums_df, albums_out, primary_keys=["album_id"])
print(f"3. Writing Silver Songs   -> {songs_out}")
merge_and_save(songs_df, songs_out, primary_keys=["recording_id"])


# 10. Commit the new Watermark to State Store (Only after writes succeed!)
wm.update_watermark("bronze_to_silver", max_extracted_at)
print(f"\n🎯 [State Committed] Successfully updated watermark to: {max_extracted_at}")
print("\n" + "=" * 50)
print("🎉 SILVER LAYER DELTA MERGE COMPLETE!")
print("=" * 50)

