import pandas as pd
import json
import os
import mysql.connector

def dump_data(**kwargs):
    # File paths
    file_path = '/home/saeeda/airflow/dags/raw_data.json'
    transformed_csv_file_path = '/home/saeeda/airflow/dags/raw_data.csv'
    transformed_json_file_path = '/home/saeeda/airflow/dags/transformed_raw_data.json'
    
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
            data = json.load(file)

        # Extract the hits from each state's data
        all_states_data = list(data.values())
        all_hits = []
        for state_data in all_states_data:
            all_hits.extend(state_data.get("hits", {}).get("hits", []))

        # Convert the data to a DataFrame
        df = pd.json_normalize([hit["_source"] for hit in all_hits])

        # Ensure timely column is treated as a string
        df['timely'] = df['timely'].astype(str)

        # Select only the required columns
        columns_to_keep = [
            "product",
            "complaint_what_happened",
            "date_sent_to_company",
            "issue",
            "sub_product",
            "zip_code",
            "tags",
            "has_narrative",
            "complaint_id",
            "timely",
            "consumer_consent_provided",
            "company_response",
            "submitted_via",
            "company",
            "date_received",
            "state",
            "consumer_disputed",
            "company_public_response",
            "sub_issue"
        ]
        df = df[columns_to_keep]

        # Save the DataFrame to CSV
        df.to_csv(transformed_csv_file_path, index=False)
        print(f"Data transformed and saved to CSV at {transformed_csv_file_path}")

        # Save the DataFrame to JSON
        df.to_json(transformed_json_file_path, orient='records', lines=True)
        print(f"Data transformed and saved to JSON at {transformed_json_file_path}")

        # Load data into MySQL
        load_data_to_mysql(df)

        # Load the transformed JSON file content
        with open(transformed_json_file_path, 'r') as json_file:
            transformed_json_data = json_file.read()

        # Push the transformed JSON data to XCom
        return transformed_json_data  # This will be pushed to XCom

    except json.JSONDecodeError as e:
        print(f"JSON decoding failed: {e}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def load_data_to_mysql(df):
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Saeeda2024!',
            database='consumer_complaints_db'
        )
        cursor = conn.cursor()

        # Create table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Raw_Complaints (
            product VARCHAR(100),
            complaint_what_happened TEXT,
            date_sent_to_company VARCHAR(50),
            issue VARCHAR(100),
            sub_product VARCHAR(100),
            zip_code VARCHAR(10),
            tags VARCHAR(50),
            has_narrative VARCHAR(10),
            complaint_id VARCHAR(50) PRIMARY KEY,
            timely ENUM('Yes', 'No') DEFAULT NULL,
            consumer_consent_provided VARCHAR(50),
            company_response VARCHAR(100),
            submitted_via VARCHAR(50),
            company VARCHAR(100),
            date_received VARCHAR(50),
            state CHAR(2),
            consumer_disputed ENUM('Yes', 'No', 'N/A') DEFAULT 'N/A',
            company_public_response TEXT,
            sub_issue VARCHAR(200)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Insert data into MySQL with ON DUPLICATE KEY UPDATE
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Raw_Complaints (
                    product,
                    complaint_what_happened,
                    date_sent_to_company,
                    issue,
                    sub_product,
                    zip_code,
                    tags,
                    has_narrative,
                    complaint_id,
                    timely,
                    consumer_consent_provided,
                    company_response,
                    submitted_via,
                    company,
                    date_received,
                    state,
                    consumer_disputed,
                    company_public_response,
                    sub_issue
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    product=VALUES(product),
                    complaint_what_happened=VALUES(complaint_what_happened),
                    date_sent_to_company=VALUES(date_sent_to_company),
                    issue=VALUES(issue),
                    sub_product=VALUES(sub_product),
                    zip_code=VALUES(zip_code),
                    tags=VALUES(tags),
                    has_narrative=VALUES(has_narrative),
                    timely=VALUES(timely),
                    consumer_consent_provided=VALUES(consumer_consent_provided),
                    company_response=VALUES(company_response),
                    submitted_via=VALUES(submitted_via),
                    company=VALUES(company),
                    date_received=VALUES(date_received),
                    state=VALUES(state),
                    consumer_disputed=VALUES(consumer_disputed),
                    company_public_response=VALUES(company_public_response),
                    sub_issue=VALUES(sub_issue)
            """, (
                row['product'],
                row['complaint_what_happened'],
                row['date_sent_to_company'],
                row['issue'],
                row['sub_product'],
                row['zip_code'],
                row['tags'],
                row['has_narrative'],
                row['complaint_id'],
                row['timely'],
                row['consumer_consent_provided'],
                row['company_response'],
                row['submitted_via'],
                row['company'],
                row['date_received'],
                row['state'],
                row['consumer_disputed'],
                row['company_public_response'],
                row['sub_issue']
            ))

        conn.commit()
        print("Data successfully loaded into MySQL database.")
    
    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    dump_data()
