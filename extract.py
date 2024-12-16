import requests
import json
from datetime import datetime, timedelta
import time
import pandas as pd
import os

def fetch_data(**kwargs):
    # Fetch list of states
    states_url = "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json"
    try:
        response = requests.get(states_url)
        response.raise_for_status()
        all_states = list(response.json().keys())
    except requests.exceptions.RequestException as e:
        print(f"Error fetching states list: {e}")
        all_states = []  # Set to empty to avoid further processing

    # Parameters for the API request
    size = 500
    time_delta = 365
    batch_size = 5  # Number of states to process in each batch

    # Set a fixed anchor date of 31-Aug-2024
    anchor_date = datetime(2024, 8, 31)
    max_date = anchor_date.strftime("%Y-%m-%d")
    min_date = (anchor_date - timedelta(days=time_delta)).strftime("%Y-%m-%d")

    combined_data = {}
    failed_states = []
    all_hits = []

    for i in range(0, len(all_states), batch_size):
        batch_states = all_states[i:i + batch_size]
        batch_data = {}

        for state in batch_states:
            url = f'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={size}&date_received_max={max_date}&date_received_min={min_date}&state={state}'
            print(f"Request URL: {url}")  # Log URL

            retry_attempts = 3
            for attempt in range(retry_attempts):
                try:
                    # Make the API request
                    response = requests.get(url, timeout=10)  # Added timeout for the request
                    response.raise_for_status()  # Raises HTTPError for bad responses
                    batch_data[state] = response.json()
                    combined_data[state] = response.json()  # Save to combined data
                    all_hits.extend(response.json().get("hits", {}).get("hits", []))  # Accumulate all hits
                    break  # Exit retry loop if successful
                except requests.exceptions.HTTPError as http_err:
                    print(f"HTTP error occurred for state {state}: {http_err}")
                    if response.status_code == 400:  # Bad Request
                        print(f"Response content: {response.text}")  # Log response content
                        failed_states.append(state)
                    break  # Exit retry loop on HTTP error
                except requests.exceptions.ConnectionError as conn_err:
                    print(f"Connection error occurred for state {state}: {conn_err}")
                except requests.exceptions.Timeout as timeout_err:
                    print(f"Timeout error occurred for state {state}: {timeout_err}")
                except requests.exceptions.RequestException as req_err:
                    print(f"Request exception occurred for state {state}: {req_err}")
                except Exception as err:
                    print(f"Other error occurred for state {state}: {err}")
                time.sleep(5)  # Wait for 5 seconds before retrying

        # Push batch data to XCom
        ti = kwargs['ti']
        if batch_data:
            ti.xcom_push(key=f'extracted_data_batch_{i//batch_size}', value=batch_data)

    # Save the combined data to raw_data.json
    raw_json_file_path = '/home/saeeda/airflow/dags/raw_data.json'
    with open(raw_json_file_path, 'w') as json_file:
        json.dump(combined_data, json_file, indent=2)
    print(f"Raw data saved to {raw_json_file_path}")

    # Normalize data and save to raw_data.csv
    if all_hits:
        df = pd.json_normalize([hit["_source"] for hit in all_hits])
        raw_csv_file_path = '/home/saeeda/airflow/dags/raw_data.csv'
        df.to_csv(raw_csv_file_path, index=False)
        print(f"Raw data saved to {raw_csv_file_path}")
    
    # Save failed states for further investigation
    failed_states_file = '/home/saeeda/airflow/dags/failed_states.json'
    with open(failed_states_file, 'w') as file:
        json.dump(failed_states, file, indent=2)
    print(f"Failed states saved to {failed_states_file}")
