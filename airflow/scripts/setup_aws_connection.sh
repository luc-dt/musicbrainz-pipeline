#!/bin/bash
# Setup AWS connection for Airflow on startup

echo "Setting up AWS connection..."

# Wait for Airflow to be ready
until airflow connections list > /dev/null 2>&1; do
    echo "Waiting for Airflow to be ready..."
    sleep 5
done

# Check if aws_default already exists
if airflow connections get aws_default > /dev/null 2>&1; then
    echo "aws_default connection already exists"
else
    echo "Creating aws_default connection..."
    airflow connections add 'aws_default' \
        --conn-type 'aws' \
        --conn-login "${AWS_ACCESS_KEY_ID}" \
        --conn-password "${AWS_SECRET_ACCESS_KEY}" \
        --conn-extra "{\"region_name\": \"${AWS_DEFAULT_REGION}\"}"
    echo "AWS connection created successfully!"
fi
