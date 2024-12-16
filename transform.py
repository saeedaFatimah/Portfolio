import pandas as pd
import json
import os
from airflow.models import Variable

def transform_data(**kwargs):
    file_path = '/home/saeeda/airflow/dags/mysql_rawdata.json'
    transformed_json_file_path = '/home/saeeda/airflow/dags/transformed_data.json'
    transformed_csv_file_path = '/home/saeeda/airflow/dags/transformed_data.csv'

    # Check if the file exists and is not empty
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return

    if os.path.getsize(file_path) == 0:
        print(f"File is empty: {file_path}")
        return

    try:
        # Load the data
        with open(file_path, 'r') as file:
            data = [json.loads(line) for line in file]  # Load each line as a separate JSON object

        # Convert the data to a DataFrame
        df = pd.json_normalize(data)

        # Drop unnecessary columns
        columns_to_drop = [
            'complaint_what_happened',
            'date_sent_to_company',
            'zip_code',
            'tags',
            'has_narrative',
            'consumer_disputed',
            'company_public_response'
        ]
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

        # Transform date_received to month_year
        df['date_received'] = pd.to_datetime(df['date_received'])
        df['month_year'] = df['date_received'].dt.to_period('M').astype(str)

        # Group by and summarize distinct complaint_id
        df_summary = df.groupby([ 
            'product',
            'issue',
            'sub_product',
            'company_response',
            'company',
            'month_year',
            'state',
            'sub_issue',
            'timely',
            'submitted_via'
        ]).agg(complaint_count=('complaint_id', 'nunique')).reset_index()

        # Save to CSV (append mode for batch processing)
        df_summary.to_csv(transformed_csv_file_path, mode='a', header=not os.path.exists(transformed_csv_file_path), index=False)
        print(f"Transformed data saved to {transformed_csv_file_path}")

        # Push transformed data to XCom
        transformed_json = df_summary.to_json(orient='records', lines=True)
        kwargs['ti'].xcom_push(key='transformed_data', value=transformed_json)

    except json.JSONDecodeError as e:
        print(f"JSON decoding failed: {e}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
