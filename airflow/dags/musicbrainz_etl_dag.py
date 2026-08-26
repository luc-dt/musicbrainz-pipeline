"""
MusicBrainz ETL DAG 
Triggers Extract Lambda → Waits for S3 → Triggers Glue Medallion Pipeline (Bronze -> Silver -> Gold)
"""

import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


logger = logging.getLogger(__name__)

# Configuration via Environment Variables with Fallbacks
S3_BUCKET = os.getenv("S3_BUCKET", "musicbrainz-etl-project-luc")
GLUE_IAM_ROLE = os.getenv("GLUE_IAM_ROLE", "AWSGlueServiceRole-musicbrainz-s3-glue-role")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "airflow-alerts@example.com")


# Error handler callback
def notify_error(context):
    """Logs structured alert metadata when a DAG task fails."""
    dag_id = context.get("dag_run").dag_id
    task_id = context.get("task_instance").task_id
    error = context.get("exception")

    logger.error(f"❌ ETL Failure Alert | DAG: {dag_id} | Task: {task_id} | Error: {error}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_error,
    'email': [ALERT_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='musicbrainz_etl_dag',
    default_args=default_args,
    description='MusicBrainz Medallion Lakehouse Pipeline - Lambda -> S3 -> Glue -> Athena',
    schedule='@daily',
    catchup=False,
    tags=['musicbrainz', 'etl', 'aws', 'lakehouse'],
)

# Group 1: EXTRACT (API -> S3 Bronze)
with TaskGroup("extract_data", dag=dag) as extract_data:
    trigger_extract = AwsLambdaInvokeFunctionOperator(
        task_id='trigger_extract_lambda',
        function_name='musicbrainz-api-extract',
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        dag=dag,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )

# Group 2: TRANSFORM MEDALLION (S3 Sensor -> Bronze to Silver -> Quality Gate -> Silver to Gold)
with TaskGroup("transform_medallion", dag=dag) as transform_medallion:
    check_s3_file = S3KeySensor(
        task_id='check_s3_file',
        bucket_name=S3_BUCKET,
        bucket_key='raw_data/to_processed/*.json',
        wildcard_match=True,
        aws_conn_id='aws_default',
        timeout=60 * 60,
        poke_interval=60,
        dag=dag,
        retries=20,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )

    glue_bronze_to_silver = GlueJobOperator(
        task_id='glue_bronze_to_silver',
        job_name='musicbrainz-bronze-to-silver',
        script_location=f's3://{S3_BUCKET}/scripts/bronze_to_silver.py',
        script_args={
            '--extra-py-files': f's3://{S3_BUCKET}/scripts/watermark_manager.py'
        },
        s3_bucket=S3_BUCKET,
        iam_role_name=GLUE_IAM_ROLE,
        aws_conn_id='aws_default',
        wait_for_completion=True,
        retries=2,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        dag=dag,
    )

    glue_data_quality = GlueJobOperator(
        task_id='glue_data_quality',
        job_name='musicbrainz-data-quality',
        script_location=f's3://{S3_BUCKET}/scripts/data_quality.py',
        s3_bucket=S3_BUCKET,
        iam_role_name=GLUE_IAM_ROLE,
        aws_conn_id='aws_default',
        wait_for_completion=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        dag=dag,
    )

    glue_silver_to_gold = GlueJobOperator(
        task_id='glue_silver_to_gold',
        job_name='musicbrainz-silver-to-gold',
        script_location=f's3://{S3_BUCKET}/scripts/silver_to_gold.py',
        s3_bucket=S3_BUCKET,
        iam_role_name=GLUE_IAM_ROLE,
        aws_conn_id='aws_default',
        wait_for_completion=True,
        retries=2,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
        dag=dag,
    )

    # Dependency inside group: Fail-Fast Quality Gate protects Gold
    check_s3_file >> glue_bronze_to_silver >> glue_data_quality >> glue_silver_to_gold

# Group-level dependency
extract_data >> transform_medallion