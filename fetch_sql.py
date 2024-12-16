import mysql.connector
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def fetch_data(ti):
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Saeeda2024!',
            database='consumer_complaints_db'
        )
        
        # Create a cursor object
        cursor = conn.cursor(dictionary=True)
        
        # Define the SQL query
        query = "SELECT * FROM Raw_Complaints"
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch all rows from the executed query
        rows = cursor.fetchall()
        
        # Convert to a DataFrame
        df = pd.DataFrame(rows)
        
        # Save the DataFrame to a JSON file
        json_file_path = '/home/saeeda/airflow/dags/mysql_rawdata.json'
        df.to_json(json_file_path, orient='records', lines=True)
        print(f"Data saved to {json_file_path}")
        
        # Load the JSON data into a variable
        with open(json_file_path, 'r') as file:
            json_data = file.read()
        
        # Push the JSON data to XCom
        ti.xcom_push(key='mysql_rawdata', value=json_data)
    
    except mysql.connector.Error as e:
        print(f"Error: {e}")
    
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Define the DAG
with DAG(
    'mysql_to_xcom',
    default_args={
        'owner': 'saeeda',
        'retries': 1,
    },
    description='Fetch MySQL data and push to XCom',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Define the PythonOperator
    fetch_sql_data = PythonOperator(
        task_id='fetch_sql_data',
        python_callable=fetch_data,
        provide_context=True,
    )

fetch_sql_data
