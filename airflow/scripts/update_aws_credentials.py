#!/usr/bin/env python3
"""
Script to update AWS credentials in Airflow connection.
Run this after starting Airflow with real credentials in .env

Usage:
    python update_aws_credentials.py --access-key AKIAxxx --secret-key your-secret
"""
import argparse
import requests
from requests.auth import HTTPBasicAuth
import json

AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USER = "airflow"
AIRFLOW_PASS = "airflow"  # Change if you changed default

def update_aws_credentials(access_key: str, secret_key: str, region: str = "ap-southeast-2"):
    """Update the aws_default connection with new credentials."""

    # First try to get existing connection
    response = requests.get(
        f"{AIRFLOW_URL}/api/v1/connections/aws_default",
        auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS)
    )

    if response.status_code == 404:
        # Create new connection
        data = {
            "connection_id": "aws_default",
            "conn_type": "aws",
            "login": access_key,
            "password": secret_key,
            "extra": json.dumps({"region_name": region})
        }
        method = "POST"
        url = f"{AIRFLOW_URL}/api/v1/connections"
    else:
        # Update existing connection
        data = {
            "login": access_key,
            "password": secret_key,
            "extra": json.dumps({"region_name": region})
        }
        method = "PATCH"
        url = f"{AIRFLOW_URL}/api/v1/connections/aws_default"

    response = requests.request(
        method,
        url,
        auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS),
        headers={"Content-Type": "application/json"},
        json=data
    )

    if response.status_code in [200, 201]:
        print("✅ AWS connection updated successfully!")
        print(f"   Access Key: {access_key[:8]}...")
        print(f"   Region: {region}")
    else:
        print(f"❌ Error: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update AWS credentials in Airflow")
    parser.add_argument("--access-key", required=True, help="AWS Access Key ID")
    parser.add_argument("--secret-key", required=True, help="AWS Secret Access Key")
    parser.add_argument("--region", default="ap-southeast-2", help="AWS Region")

    args = parser.parse_args()
    update_aws_credentials(args.access_key, args.secret_key, args.region)
