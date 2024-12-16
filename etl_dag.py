from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys

# Insert the path of your extract.py, dump.py, transform.py, load_db.py, load_sheets.py, and fetch_sql.py files to ensure they're found by Airflow
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import the functions from the respective modules
from extract import fetch_data as extract_fetch_data
from dump import dump_data
from transform import transform_data
from load_db import load_transformed_data
from load_sheets import load_to_sheets
from fetch_sql import fetch_data as fetch_sql_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'cfpb_etl',
    default_args=default_args,
    description='ETL DAG for CFPB data extraction, dumping, transformation, loading into MySQL, and Google Sheets',
    schedule_interval='@daily',  # Adjust the schedule as needed
)

# Task 1: Extract data using fetch_data function from extract.py
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_fetch_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Dump data (including loading into MySQL) using dump_data function from dump.py
dump_task = PythonOperator(
    task_id='dump_data',
    python_callable=dump_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Fetch SQL data using fetch_data function from fetch_sql.py
fetch_sql_task = PythonOperator(
    task_id='fetch_sql_data',
    python_callable=fetch_sql_data,
    provide_context=True,
    dag=dag,
)

# Task 4: Transform data using transform_data function from transform.py
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 5: Load transformed data into Google Sheets and store URL in a file
load_to_sheets_task = PythonOperator(
        task_id='load_to_sheets',
        python_callable=load_to_sheets,
        provide_context=True,  # This ensures the context is passed
        dag=dag,
)


# Task 6: Load transformed data into MySQL using load_transformed_data from load_db.py
load_to_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_transformed_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
extract_task >> dump_task >> fetch_sql_task >> transform_task >> load_to_sheets_task >> load_to_db_task
