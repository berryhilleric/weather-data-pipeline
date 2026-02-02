from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')

from extract_weather import extract_weather_data, load_to_database as load_weather
from extract_air_quality import extract_air_quality_data, load_to_database as load_air_quality

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Extract weather',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
)

def extract_and_load_weather():
    df = extract_weather_data()
    load_weather(df, 'weather_data')

def extract_and_load_air_quality():
    df = extract_air_quality_data()
    load_air_quality(df, 'air_quality_data')

task_extract_weather = PythonOperator(
    task_id='extract_weather',
    python_callable=extract_and_load_weather,
    dag=dag,
)

task_extract_air_quality = PythonOperator(
    task_id='extract_air_quality',
    python_callable=extract_and_load_air_quality,
    dag=dag,
)

# Run dbt directly in the Airflow container
run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /dbt && /home/airflow/.local/bin/dbt run',
    dag=dag,
)

# Set task dependencies: both extract tasks must finish before dbt runs
[task_extract_weather, task_extract_air_quality] >> run_dbt