-- ========================================================================================
-- MUSICBRAINZ DATA LAKEHOUSE: ATHENA DDL (STAR SCHEMA & ANALYTICS LAYER)
-- ========================================================================================
-- Database: musicbrainz_dw
-- Engine: Amazon Athena (Presto / Trino)
-- Purpose:
--   Registers external Parquet tables in the AWS Glue Data Catalog for both Silver
--   (Conformed Dimensions) and Gold (Fact & KPI) layers.
-- ========================================================================================


-- 1. Create Analytics Database
CREATE DATABASE IF NOT EXISTS musicbrainz_dw
COMMENT 'MusicBrainz Medallion Data Lakehouse Analytics Database';

-- ========================================================================================
-- SILVER LAYER: CONFORMED DIMENSIONS
-- ========================================================================================

-- Dimension 1: Artists (Grain: 1 row = 1 unique artist entity)
CREATE EXTERNAL TABLE IF NOT EXISTS musicbrainz_dw.dim_artist (
    artist_id STRING,
    artist_name STRING,
    artist_sort_name STRING,
    artist_disambiguation STRING,
    artist_search STRING,
    extracted_at TIMESTAMP,
    artist_url STRING
)
STORED AS PARQUET
LOCATION 's3://musicbrainz-etl-project-luc/silver/artists/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- Dimension 2: Albums (Grain: 1 row = 1 unique album release)
CREATE EXTERNAL TABLE IF NOT EXISTS musicbrainz_dw.dim_album (
    album_id STRING,
    album_name STRING,
    country STRING,
    status STRING,
    track_count INT,
    artist_search STRING,
    extracted_at TIMESTAMP,
    release_date DATE,
    album_url STRING
)
STORED AS PARQUET
LOCATION 's3://musicbrainz-etl-project-luc/silver/albums/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- Dimension 3: Songs (Grain: 1 row = 1 unique recording track)
CREATE EXTERNAL TABLE IF NOT EXISTS musicbrainz_dw.dim_song (
    recording_id STRING,
    title STRING,
    length_ms BIGINT,
    video BOOLEAN,
    score INT,
    artist_search STRING,
    extracted_at TIMESTAMP,
    first_release_date DATE
)
STORED AS PARQUET
LOCATION 's3://musicbrainz-etl-project-luc/silver/songs/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ========================================================================================
-- GOLD LAYER: AGGREGATED BUSINESS FACTS & KPIS
-- ========================================================================================

-- Fact 1: Artist 360 Summary (Aggregate Summary Fact - Grain: 1 row = 1 artist career aggregate)
CREATE EXTERNAL TABLE IF NOT EXISTS musicbrainz_dw.fact_artist_summary (
    artist_search STRING,
    artist_id STRING,
    artist_name STRING,
    artist_sort_name STRING,
    artist_disambiguation STRING,
    extracted_at TIMESTAMP,
    artist_url STRING,
    total_recordings BIGINT,
    avg_song_length_min DOUBLE,
    earliest_song_release DATE,
    latest_song_release DATE,
    total_albums BIGINT,
    distinct_release_countries BIGINT,
    earliest_album_release DATE,
    latest_album_release DATE,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://musicbrainz-etl-project-luc/gold/artist_summary/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- Fact 2: Yearly & Country Release Metrics (Periodic Snapshot Fact - Grain: 1 row = 1 year x 1 country)
CREATE EXTERNAL TABLE IF NOT EXISTS musicbrainz_dw.fact_yearly_release_metrics (
    release_year INT,
    country_code STRING,
    total_releases BIGINT,
    avg_tracks_per_album DOUBLE,
    distinct_artists BIGINT,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://musicbrainz-etl-project-luc/gold/yearly_release_metrics/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');