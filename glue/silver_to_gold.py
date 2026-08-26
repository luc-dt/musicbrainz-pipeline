"""
========================================================================================
MUSICBRAINZ ETL PIPELINE: SILVER TO GOLD TRANSFORMATION (PySpark)
========================================================================================
Medallion Architecture - Gold Layer Business Analytics
Purpose:
  Reads conformed, deduplicated Parquet tables from the Silver Layer (artists, albums, songs),
  computes business KPIs and analytical aggregations, and writes optimized Gold Parquet
  datasets ready for Amazon Athena and Power BI.
What this script accomplishes:
  1. Ingestion: Reads Silver Parquet tables from `data/silver/`.
  2. Aggregation:
     - Table 1: artist_summary (Artist 360 KPIs: song count, avg length, album count, country reach).
     - Table 2: yearly_release_metrics (Release trends by year and country).
  3. Storage: Writes Snappy Parquet tables to `data/gold/`.
========================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    min,
    max,
    round,
    year,
    current_timestamp,
    coalesce,
    lit
)

# 1. Create a local Spark session
spark = (
    SparkSession.builder
    .appName("MusicBrainz_Silver_to_Gold")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

# 2. Dynamic Paths (Hybrid Local & AWS Glue S3)
IS_GLUE = "GLUE_COMMAND_CRITERIA" in os.environ or "AWS_EXECUTION_ENV" in os.environ or "JOB_NAME" in os.environ

if IS_GLUE:
    S3_BUCKET = "s3://musicbrainz-etl-project-luc"
    silver_base = f"{S3_BUCKET}/silver"
    gold_base   = f"{S3_BUCKET}/gold"
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
    silver_base = os.path.join(PROJECT_ROOT, "data", "silver").replace("\\", "/")
    gold_base   = os.path.join(PROJECT_ROOT, "data", "gold").replace("\\", "/")


# 3. Read Silver Parquet Tables 
artists_df  = spark.read.parquet(f"{silver_base}/artists")
albums_df  = spark.read.parquet(f"{silver_base}/albums")
songs_df = spark.read.parquet(f"{silver_base}/songs")


print("=== SILVER TABLES LOADED ===")
print(f"Artists: {artists_df.count()} rows")
print(f"Albums : {albums_df.count()} rows")
print(f"Songs  : {songs_df.count()} rows")

# 4. Table 1: Artist 360 Summary
def build_artist_summary(artists_df, songs_df, albums_df):
    """
    Aggregates song metrics and album release stats for each artist.
    This table answers all high-level business questions about an artist in a single query:
        - How many songs and albums do they have?
        - What is their average song length in minutes?
        - Across how many countries have they released music?
        - What is their career timespan (earliest vs latest release)?
    """
    # Part 1: Song Aggregations
    song_stats = (
        songs_df
        .groupBy("artist_id")
        .agg(
            count("recording_id").alias("total_recordings"),
            round(avg(col("length_ms")) / 1000 / 60, 2).alias("avg_song_length_min"),
            min("first_release_date").alias("earliest_song_release"),
            max("first_release_date").alias("latest_song_release")
        )
    )

    # Part 2: Album Aggregations
    album_stats = (
        albums_df
        .groupBy("artist_id")
        .agg(
            count("album_id").alias("total_albums"),
            countDistinct("country").alias("distinct_release_countries"),
            min("release_date").alias("earliest_album_release"),
            max("release_date").alias("latest_album_release")
        )
    )

    # Part 3: Combine with Artists Dimension (joins on PK/FK artist_id)
    artist_summary = (
        artists_df
        .join(song_stats, on="artist_id", how="left")
        .join(album_stats, on="artist_id", how="left")
        .withColumn("created_at", current_timestamp())
    )

    return artist_summary

# 5. Table 2: Yearly & Country Release Metrics
def build_yearly_release_metrics(albums_df):
    """
    Computes yearly release trends and average track counts grouped by year and country.
    What Business Questions Does Table 2 Answer?
        - How many albums were released globally in each year (e.g. 2020 vs 2024)?
        - Which countries have the highest number of music releases (US, GB, JP, KR)?
        - What is the average track count on an album for each country and year?
    """
    yearly_metrics = (
        albums_df
        .withColumn("release_year", year(col("release_date")))
        .withColumn("country_code", coalesce(col("country"), lit("UNKNOWN")))
        .filter(col("release_year").isNotNull())
        .groupBy("release_year", "country_code")
        .agg(
            count("album_id").alias("total_releases"),
            round(avg("track_count"), 1).alias("avg_tracks_per_album"),
            countDistinct("artist_id").alias("distinct_artists")
        )
        .withColumn("created_at", current_timestamp())
    )

    return yearly_metrics


# 6. Execute All Gold Aggregations & Save to Parquet

print("\n" + "=" * 50)
print("🚀 EXECUTING SILVER TO GOLD PIPELINE")
print("=" * 50)

print("1. Building Artist Summary (Table 1)...")
artist_summary_df = build_artist_summary(artists_df, songs_df, albums_df)

print("2. Building Yearly Release Metrics (Table 2)...")
yearly_metrics_df = build_yearly_release_metrics(albums_df)

# Output paths
artist_summary_out = f"{gold_base}/artist_summary"
yearly_metrics_out = f"{gold_base}/yearly_release_metrics"

print(f"\nWriting Gold Artist Summary ({artist_summary_df.count()} rows) -> {artist_summary_out}...")
artist_summary_df.write.mode("overwrite").parquet(artist_summary_out)

print(f"Writing Gold Yearly Metrics ({yearly_metrics_df.count()} rows) -> {yearly_metrics_out}...")
yearly_metrics_df.write.mode("overwrite").parquet(yearly_metrics_out)

print("\n" + "=" * 50)
print("🎉 GOLD LAYER GENERATION COMPLETE!")
print("=" * 50)

