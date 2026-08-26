import json
import os
import time
from datetime import datetime, timezone
from functools import wraps

import boto3
import requests

# -----------------------------
# Global configuration
# -----------------------------
BASE_URL = "https://musicbrainz.org/ws/2"

TARGET_ARTISTS = [
    "Coldplay",
    "Taylor Swift",
    "Dua Lipa",
    "James Blunt",
    "BTS"
]

# Retry configuration for MusicBrainz API
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Reuse client between Lambda invocations
s3 = boto3.client("s3")


# -----------------------------
# Robust Retry Decorator
# -----------------------------
def retry_api_call(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """
    Decorator to retry on transient HTTP errors (503, 429, 502, 504)
    and network Timeouts/Connection drops with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code if e.response is not None else 0
                    if status in (429, 502, 503, 504) and attempt < max_retries - 1:
                        print(f"HTTP {status} on {func.__name__}, retrying in {current_delay}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(current_delay)
                        current_delay *= 2
                    else:
                        raise
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    if attempt < max_retries - 1:
                        print(f"{type(e).__name__} on {func.__name__}, retrying in {current_delay}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(current_delay)
                        current_delay *= 2
                    else:
                        raise
            return func(*args, **kwargs)
        return wrapper
@retry_api_call()
def fetch_artist_data(headers, params):
    """
    Fetches recording data for a specific query from the MusicBrainz API
    with automatic exponential backoff on transient network and HTTP errors.
    """
    response = requests.get(
        f"{BASE_URL}/recording",
        headers=headers,
        params=params,
        timeout=(10, 45)  # (10s connect, 45s read timeout)
    )
    response.raise_for_status()
    return response.json()


def lambda_handler(event, context):
    # Environment variables
    raw_bucket = os.environ.get("RAW_BUCKET")
    email = os.environ.get("USER_AGENT_EMAIL")

    if not raw_bucket:
        raise ValueError("Missing environment variable: RAW_BUCKET")

    if not email:
        raise ValueError("Missing environment variable: USER_AGENT_EMAIL")

    headers = {
        "User-Agent": f"musicbrainz-etl-project/1.0 ({email})"
    }

    raw_data = []

    # -----------------------------
    # Extract data from MusicBrainz
    # -----------------------------
    for artist in TARGET_ARTISTS:
        print(f"Extracting data for artist: {artist}")

        params = {
            "query": f'artist:"{artist}"',
            "limit": 50,
            "fmt": "json"
        }

        data = fetch_artist_data(headers, params)

        raw_data.append({
            "artist_search": artist,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "data": data
        })

        # Respect MusicBrainz rate limit
        time.sleep(1.1)

    # -----------------------------
    # Save raw JSON to S3
    # -----------------------------
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    file_name = f"musicbrainz_raw_{timestamp}.json"

    s3_key = f"raw_data/to_processed/{file_name}"

    s3.put_object(
        Bucket=raw_bucket,
        Key=s3_key,
        Body=json.dumps(raw_data),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Extraction completed successfully.",
            "bucket": raw_bucket,
            "key": s3_key,
            "artists_processed": len(TARGET_ARTISTS),
            "timestamp": timestamp
        })
    }