import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def load_to_sheets(**kwargs):
    # Load transformed data from CSV
    df = pd.read_csv('/home/saeeda/airflow/dags/transformed_data.csv')

    # Set up Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/saeeda/airflow/dags/credentials.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open("data").sheet1

    # Update Google Sheet with DataFrame
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

    # Google Sheets link to save to a file
    sheet_url = "https://docs.google.com/spreadsheets/d/1iCGAmz-3XagkUinf5mC5Hs7DLcjhVWP6TonWzio18-I/edit?usp=sharing"
    
    # File path to store the Google Sheets link
    file_path = '/home/saeeda/airflow/dags/sheet_link.txt'

    # Write the Google Sheets link to a text file
    with open(file_path, 'w') as f:
        f.write(sheet_url)

    # Push the file path to XCom
    ti = kwargs['ti']  # TaskInstance object from the kwargs
    ti.xcom_push(key='file_path', value=file_path)

    return file_path  # Return the file path for reference
