-- ========================================================================================
-- MUSICBRAINZ ANALYTICS: 5 CORE BUSINESS INTELLIGENCE SQL QUERIES (ATHENA)
-- ========================================================================================
-- Database: musicbrainz_dw
-- Engine: Amazon Athena (Presto / Trino)
-- ========================================================================================

-- ----------------------------------------------------------------------------------------
-- Query 1: Artist 360 Leaderboard (Catalog Size, Track Count & Global Market Reach)
-- Business Value: Identifies the most productive artists and their international footprint.
-- ----------------------------------------------------------------------------------------
SELECT 
    artist_name,
    artist_disambiguation,
    total_recordings,
    total_albums,
    avg_song_length_min,
    distinct_release_countries,
    earliest_album_release,
    latest_album_release
FROM musicbrainz_dw.fact_artist_summary
ORDER BY total_recordings DESC;


-- ----------------------------------------------------------------------------------------
-- Query 2: Global Music Release Trends by Country (Top 10 Markets)
-- Business Value: Shows which countries dominate album releases and average tracks per album.
-- ----------------------------------------------------------------------------------------
SELECT 
    country_code,
    SUM(total_releases) AS cumulative_releases,
    ROUND(AVG(avg_tracks_per_album), 1) AS overall_avg_tracks_per_album,
    SUM(distinct_artists) AS total_active_artists
FROM musicbrainz_dw.fact_yearly_release_metrics
WHERE country_code != 'UNKNOWN'
GROUP BY country_code
ORDER BY cumulative_releases DESC
LIMIT 10;


-- ----------------------------------------------------------------------------------------
-- Query 3: Song Duration & Outlier Analysis (Longest Recordings & Concert Films)
-- Business Value: Identifies epic extended recordings, concert sets, and radio singles.
-- ----------------------------------------------------------------------------------------
SELECT 
    s.title,
    a.artist_name,
    ROUND(CAST(s.length_ms AS DOUBLE) / 1000 / 60, 2) AS duration_minutes,
    s.video,
    s.score,
    s.first_release_date
FROM musicbrainz_dw.dim_song s
JOIN musicbrainz_dw.dim_artist a ON s.artist_id = a.artist_id
WHERE s.length_ms IS NOT NULL
ORDER BY s.length_ms DESC
LIMIT 10;


-- ----------------------------------------------------------------------------------------
-- Query 4: Career Lifespan & Album Release Velocity
-- Business Value: Measures artist longevity and release consistency across years.
-- ----------------------------------------------------------------------------------------
SELECT 
    artist_name,
    earliest_album_release,
    latest_album_release,
    date_diff('year', earliest_album_release, latest_album_release) AS career_span_years,
    total_albums,
    ROUND(
        CAST(total_albums AS DOUBLE) / NULLIF(date_diff('year', earliest_album_release, latest_album_release), 0), 
        2
    ) AS albums_per_year
FROM musicbrainz_dw.fact_artist_summary
WHERE earliest_album_release IS NOT NULL AND latest_album_release IS NOT NULL
ORDER BY career_span_years DESC;


-- ----------------------------------------------------------------------------------------
-- Query 5: Star Schema Dimensional Join (Artist Dimension x Album Dimension)
-- Business Value: Analyzes album release status (Official vs Promotional vs Bootleg) by Artist.
-- ----------------------------------------------------------------------------------------
SELECT 
    a.artist_name,
    alb.status AS album_status,
    COUNT(alb.album_id) AS album_count,
    ROUND(AVG(alb.track_count), 1) AS avg_track_count
FROM musicbrainz_dw.dim_artist a
JOIN musicbrainz_dw.dim_album alb ON a.artist_id = alb.artist_id
WHERE alb.status IS NOT NULL
GROUP BY a.artist_name, alb.status
ORDER BY a.artist_name, album_count DESC;
