"""
MusicBrainz ETL DAG - Day 2
Triggers Extract Lambda → Waits for S3 → Triggers Transform Lambda

"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='musicbrainz_etl_dag',
    default_args=default_args,
    description='MusicBrainz ETL Pipeline - Extract from API, Load to S3, Transform to Parquet',
    schedule_interval='@daily',
    catchup=False,
    tags=['musicbrainz', 'etl', 'aws'],
)

# === TASK 1: Trigger Extract Lambda ===
trigger_extract = AwsLambdaInvokeFunctionOperator(
    task_id='trigger_extract_lambda',
    function_name='musicbrainz-api-extract',
    aws_conn_id='aws_default',
    invocation_type='RequestResponse',  # Wait for completion
    dag=dag,
)

# === TASK 2: Wait for S3 File ===
check_s3_file = S3KeySensor(
    task_id='check_s3_file',
    bucket_name='musicbrainz-etl-project-luc',
    bucket_key='raw_data/to_processed/*.json',
    wildcard_match=True,
    aws_conn_id='aws_default',
    timeout=60 * 60,  # 60 minutes max
    poke_interval=60,  # Check every 60 seconds
    dag=dag,
)

# === TASK 3: Trigger Transform Lambda ===
trigger_transform = AwsLambdaInvokeFunctionOperator(
    task_id='trigger_transform_lambda',
    function_name='musicbrainz_transformation_load_function',
    aws_conn_id='aws_default',
    invocation_type='RequestResponse',
    dag=dag,
)


# === TASK DEPENDENCIES ===
# Read this as: "then" or "after"
trigger_extract >> check_s3_file >> trigger_transform