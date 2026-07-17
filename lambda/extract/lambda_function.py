import json
import os
import time
from datetime import datetime, timezone

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

# Reuse client between Lambda invocations
s3 = boto3.client("s3")


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

        params = {
            "query": f'artist:"{artist}"',
            "limit": 50,
            "fmt": "json"
        }

        response = requests.get(
            f"{BASE_URL}/recording",
            headers=headers,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        raw_data.append({
            "artist_search": artist,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "data": response.json()
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