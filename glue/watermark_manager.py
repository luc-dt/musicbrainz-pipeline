"""
Watermark Manager for Incremental Loading
Handles state persistence both locally (JSON file) and on AWS S3.
"""
import json
import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

class WatermarkManager:
    """Manages pipeline watermarks for delta batch processing."""

    def __init__(self, s3_bucket=None, local_state_path=None):
        self.s3_bucket = s3_bucket or "musicbrainz-etl-project-luc"

        # S3 Key for watermark state
        self.s3_key = "state/watermarks.json"

        # Local fallback path for local development
        if local_state_path:
            self.local_path = local_state_path
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.abspath(os.path.join(script_dir, ".."))
            self.local_path = os.path.join(project_root, "data", "state", "watermarks.json")

        self.s3_client = boto3.client("s3")

    def _is_cloud_env(self):
        """Detect if running in AWS Glue or Lambda environment."""
        return (
            "GLUE_COMMAND_CRITERIA" in os.environ
            or "AWS_EXECUTION_ENV" in os.environ
            or "JOB_NAME" in os.environ
        )

    def get_watermark(self, job_name, default="1970-01-01 00:00:00"):
        """
        Retrieves the last processed timestamp for a specific job.
        Returns default timestamp if no state exists yet.
        """
        state = self._read_state()
        job_state = state.get(job_name, {})
        timestamp = job_state.get("last_processed_timestamp", default)
        print(f"[{job_name}] Retrieved watermark: {timestamp}")
        return timestamp 

    def update_watermark(self, job_name, new_timestamp):
        state = self._read_state()
        state[job_name] = {
            "last_processed_timestamp": new_timestamp,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }
        self._write_state(state)
        print(f"[{job_name}] ✅ Updated watermark to: {new_timestamp}")

    def _read_state(self):
        """Reads state JSON from S3 (if cloud) or local disk (if local)."""
        if self._is_cloud_env():
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
                content = response["Body"].read().decode("utf-8")
                return json.loads(content)
            except ClientError as e:
                # File not found on first run
                if e.response["Error"]["Code"] == "NoSuchKey":
                    return {}
                raise e
        else:
            if os.path.exists(self.local_path):
                with open(self.local_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            return {}
        
    def _write_state(self, state):
        """Writes state JSON to S3 (if cloud) or local disk (if local)."""
        payload = json.dumps(state, indent=2)
        
        if self._is_cloud_env():
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.s3_key,
                Body=payload.encode("utf-8"),
                ContentType="application/json"
            )
        else:
            os.makedirs(os.path.dirname(self.local_path), exist_ok=True)
            with open(self.local_path, "w", encoding="utf-8") as f:
                f.write(payload)
       


# Quick local test if run directly
if __name__ == "__main__":
    print("=== Testing Watermark Manager Locally ===")
    wm = WatermarkManager()
    
    # 1. Get initial watermark
    initial_wm = wm.get_watermark("bronze_to_silver")
    print(f"Initial: {initial_wm}")

    # 2. Update watermark
    test_time = "2026-08-22 17:00:00"
    wm.update_watermark("bronze_to_silver", test_time)

    # 3. Read it back
    updated_wm = wm.get_watermark("bronze_to_silver")
    assert updated_wm == test_time, "Watermark read-back failed!"
    print(f"Verified: {updated_wm}")
    print("🎉 WatermarkManager test passed successfully!")