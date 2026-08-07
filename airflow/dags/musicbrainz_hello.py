from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments 
default_args = {
    'owner': 'luc',
    'start_date': datetime(2025, 1, 1)
}

# Create the DAG 
dag = DAG(
    'musicbrainz_hello',
    default_args = default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define the task
def say_hello():
    print("Hello from MusicBrainz Pipeline!")
    return "Success!"

# Create the task
hello_task = PythonOperator(
    task_id = 'say_hello',
    python_callable=say_hello,
    dag=dag
)