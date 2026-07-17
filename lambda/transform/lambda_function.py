# Import library
import os
import json
import boto3
import pandas as pd 
from io import StringIO 
from datetime import datetime 

# Create S3 cient
s3 = boto3.client("s3")

# Read environment variables 
RAW_BUCKET = os.environ["RAW_BUCKET"]

def album(data):
    album_list = []

    for row in data:
        artist_search = row.get("artist_search")
        extracted_at = row.get("extracted_at")

        recordings = row.get("data", {}).get("recordings", [])

        for recording in recordings:
            releases = recording.get("releases", [])

            for release in releases:
                album_id = release.get("id")

                album_list.append({
                    "album_id": album_id,
                    "album_name": release.get("title"),
                    "release_date": release.get("date"),
                    "country": release.get("country"),
                    "status": release.get("status"),
                    "track_count": release.get("track-count"),
                    "album_url": f"https://musicbrainz.org/release/{album_id}" if album_id else None,
                    "artist_search": artist_search,
                    "extracted_at": extracted_at
                })

    return album_list

def artist(data):
    artist_list = []

    for row in data:
        artist_search = row.get("artist_search")
        extracted_at = row.get("extracted_at")

        recordings = row.get("data", {}).get("recordings", [])

        for recording in recordings:
            artist_credits = recording.get("artist-credit", [])

            for artist_info in artist_credits:
                artist = artist_info.get("artist", {})
                artist_id = artist.get("id")

                artist_list.append({
                    "artist_id": artist_id,
                    "artist_name": artist.get("name"),
                    "artist_sort_name": artist.get("sort-name"),
                    "artist_type": artist.get("type"),
                    "artist_country": artist.get("country"),
                    "artist_disambiguation": artist.get("disambiguation"),
                    "artist_url": f"https://musicbrainz.org/artist/{artist_id}" if artist_id else None,
                    "artist_search": artist_search,
                    "extracted_at": extracted_at
                })

    return artist_list

def song(data):
    song_list = []

    for row in data:
        artist_search = row.get("artist_search")
        extracted_at = row.get("extracted_at")

        recordings = row.get("data", {}).get("recordings", [])

        for recording in recordings:
           song_list.append({
            "recording_id": recording.get("id"),
            "title": recording.get("title"),
            "length_ms": recording.get("length"),
            "video": recording.get("video"),
            "score": recording.get("score"),
            "first_release_date": recording.get("first-release-date"),
            "artist_search": artist_search,
            "extracted_at": extracted_at
           })

    return song_list


def lambda_handler(event, context):
    # Parse the S3 event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    print(f"Bucket: {bucket_name}")
    print(f"Object: {object_key}")

    # Download the JSON from S3
    response = s3.get_object(
        Bucket = bucket_name,
        Key = object_key
    )
    # Read the JSON into Python
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)
   
    # Transform the data
    album_list = album(data)
    artist_list = artist(data)
    song_list = song(data)

    # Create DataFrame
    album_df = pd.DataFrame(album_list).drop_duplicates(subset=["album_id"]).reset_index(drop=True)
    artist_df = pd.DataFrame(artist_list).drop_duplicates(subset=["artist_id"]).reset_index(drop=True)
    song_df = pd.DataFrame(song_list).drop_duplicates(subset=["recording_id"]).reset_index(drop=True)

    print(album_df.shape)
    print(album_df.head())
    print("---")
    print(artist_df.shape)
    print(artist_df.head())
    print("---")
    print(song_df.shape)
    print(song_df.head())
    
    # Convert dates
    album_df["release_date"] = pd.to_datetime(album_df["release_date"], errors="coerce")
    song_df["first_release_date"] = pd.to_datetime(song_df["first_release_date"], errors="coerce")
    
    # Write transformed data to S3 as CSV 
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    for df, name in [(album_df, "album"), (artist_df, "artist"), (song_df, "song")]:
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key=f"transformed_data/{name}_data/{name}_{timestamp}.csv",
                Body=buffer.getvalue()
            )
    # Move processed JSON
    copy_source = {'Bucket': bucket_name, 'Key': object_key}
    s3.copy_object(Bucket=bucket_name, Key=object_key.replace("to_processed", "processed"), CopySource=copy_source)
    s3.delete_object(Bucket=bucket_name, Key=object_key)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

   