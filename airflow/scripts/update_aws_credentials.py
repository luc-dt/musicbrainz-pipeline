#!/usr/bin/env python3
"""
Update the aws_default Airflow connection.

AWS credentials are read from environment variables:

    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

Usage:
    python update_aws_credentials.py
"""

import json
import os

import requests
from requests.auth import HTTPBasicAuth


AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")


def update_aws_credentials():
    """Create or update the aws_default Airflow connection."""

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "must be set as environment variables."
        )

    auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS)

    # Check whether connection already exists
    response = requests.get(
        f"{AIRFLOW_URL}/api/v1/connections/aws_default",
        auth=auth,
        timeout=10,
    )

    if response.status_code == 404:
        data = {
            "connection_id": "aws_default",
            "conn_type": "aws",
            "login": access_key,
            "password": secret_key,
            "extra": json.dumps({
                "region_name": region
            }),
        }

        response = requests.post(
            f"{AIRFLOW_URL}/api/v1/connections",
            auth=auth,
            headers={"Content-Type": "application/json"},
            json=data,
            timeout=10,
        )

    elif response.status_code == 200:
        data = {
            "login": access_key,
            "password": secret_key,
            "extra": json.dumps({
                "region_name": region
            }),
        }

        response = requests.patch(
            f"{AIRFLOW_URL}/api/v1/connections/aws_default",
            auth=auth,
            headers={"Content-Type": "application/json"},
            json=data,
            timeout=10,
        )

    else:
        raise RuntimeError(
            f"Failed to check Airflow connection: "
            f"{response.status_code} {response.text}"
        )

    if response.status_code in (200, 201):
        print("AWS connection updated successfully.")
        print(f"Access Key: {access_key[:8]}...")
        print(f"Region: {region}")
    else:
        raise RuntimeError(
            f"Failed to update AWS connection: "
            f"{response.status_code} {response.text}"
        )


if __name__ == "__main__":
    update_aws_credentials()
