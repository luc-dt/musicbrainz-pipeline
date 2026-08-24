"""
MusicBrainz ETL DAG 
Triggers Extract Lambda → Waits for S3 → Triggers Glue Medallion Pipeline (Bronze -> Silver -> Gold)
"""

import logging
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


logger = logging.getLogger(__name__)

# Error handler function
def notify_error(context):
    """Send alert when task fails."""
    dag_id = context.get("dag_run").dag_id
    task_id = context.get("task_instance").task_id
    error = context.get("exception")

    logger.error(f"DAG: {dag_id} | Task: {task_id} | Error: {error}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_error,
    'email': ['nguyentluc19@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='musicbrainz_etl_dag',
    default_args=default_args,
    description='MusicBrainz ETL Pipeline - Extract from API, Load to S3, Transform to Parquet',
    schedule_interval='@daily',
    catchup=False,
    tags=['musicbrainz', 'etl', 'aws'],
)

# Group 1: EXTRACT (gets data)
with TaskGroup("extract_data", dag=dag) as extract_data:
    trigger_extract = AwsLambdaInvokeFunctionOperator(
        task_id='trigger_extract_lambda',
        function_name='musicbrainz-api-extract',
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',           # Wait for completion
        dag=dag,
        retries=3,                                   # How many retries
        retry_delay=timedelta(minutes=2),            # Initial delay
        retry_exponential_backoff=True,              # Enable exponential backoff
        max_retry_delay=timedelta(minutes=10),       # Cap the delay    
    )

# Group 2: TRANSFORM MEDALLION (S3 Sensor -> Bronze to Silver -> Silver to Gold)
with TaskGroup("transform_medallion", dag=dag) as transform_medallion:
    check_s3_file = S3KeySensor(
        task_id='check_s3_file',
        bucket_name='musicbrainz-etl-project-luc',
        bucket_key='raw_data/to_processed/*.json',
        wildcard_match=True,
        aws_conn_id='aws_default',
        timeout=60 * 60,                         # 60 minutes max
        poke_interval=60,                        # Check every 60 seconds
        dag=dag,
        retries=20,                              # Many retries for waiting
        retry_delay=timedelta(minutes=1),        # Initial delay
        retry_exponential_backoff=True,          # Enable exponential backoff
        )

    glue_bronze_to_silver = GlueJobOperator(
        task_id='glue_bronze_to_silver',
        job_name='musicbrainz-bronze-to-silver',
        script_location='s3://musicbrainz-etl-project-luc/scripts/bronze_to_silver.py',
        script_args={
            '--extra-py-files': 's3://musicbrainz-etl-project-luc/scripts/watermark_manager.py'
        },
        s3_bucket='musicbrainz-etl-project-luc',
        iam_role_name='AWSGlueServiceRole-musicbrainz-s3-glue-role', 
        aws_conn_id='aws_default',
        wait_for_completion=True,
        retries=2,                             # How many retries
        retry_delay=timedelta(minutes=3),       # Initial delay
        retry_exponential_backoff=True,         # Enable exponential backoff
        max_retry_delay=timedelta(minutes=10),  # Cap the delay
        dag=dag,
    )

    glue_data_quality = GlueJobOperator(
        task_id='glue_data_quality',
        job_name='musicbrainz-data-quality',
        script_location='s3://musicbrainz-etl-project-luc/scripts/data_quality.py',
        s3_bucket='musicbrainz-etl-project-luc',
        iam_role_name='AWSGlueServiceRole-musicbrainz-s3-glue-role', 
        aws_conn_id='aws_default',
        wait_for_completion=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        dag=dag,
    )


    glue_silver_to_gold = GlueJobOperator(
        task_id='glue_silver_to_gold',
        job_name='musicbrainz-silver-to-gold',
        script_location='s3://musicbrainz-etl-project-luc/scripts/silver_to_gold.py',
        s3_bucket='musicbrainz-etl-project-luc',
        iam_role_name='AWSGlueServiceRole-musicbrainz-s3-glue-role', 
        aws_conn_id='aws_default',
        wait_for_completion=True,
        retries=2,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
        dag=dag,
    )

    # Dependency inside group
    check_s3_file >> glue_bronze_to_silver >> glue_data_quality  >> glue_silver_to_gold

# Group-level dependency
extract_data >> transform_medallion