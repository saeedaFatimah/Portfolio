import pandas as pd
import mysql.connector

def load_transformed_data():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Saeeda2024!',
        database='consumer_complaints_db'
    )
    cursor = conn.cursor()

    # Alter the table to increase column size
    cursor.execute("""
        ALTER TABLE Transformed_Complaints
        MODIFY COLUMN sub_issue VARCHAR(255);
    """)

    # Load transformed data from CSV
    df = pd.read_csv('/home/saeeda/airflow/dags/transformed_data.csv')

    # Create table if not exists with updated column size
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Transformed_Complaints (
            product VARCHAR(100),
            issue VARCHAR(100),
            sub_product VARCHAR(100),
            company_response VARCHAR(100),
            company VARCHAR(100),
            month_year VARCHAR(7),
            state CHAR(2),
            sub_issue VARCHAR(255),  -- Updated size
            timely VARCHAR(50),
            submitted_via VARCHAR(100),
            complaint_count INT,
            PRIMARY KEY (product, issue, sub_product, company_response, company, month_year, state, sub_issue, timely, submitted_via)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """)

    # Insert data into MySQL with ON DUPLICATE KEY UPDATE
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Transformed_Complaints (
                product,
                issue,
                sub_product,
                company_response,
                company,
                month_year,
                state,
                sub_issue,
                timely,
                submitted_via,
                complaint_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                product=VALUES(product),
                issue=VALUES(issue),
                sub_product=VALUES(sub_product),
                company_response=VALUES(company_response),
                company=VALUES(company),
                month_year=VALUES(month_year),
                state=VALUES(state),
                sub_issue=VALUES(sub_issue),
                timely=VALUES(timely),
                submitted_via=VALUES(submitted_via),
                complaint_count=VALUES(complaint_count)
        """, (
            row['product'],
            row['issue'],
            row['sub_product'],
            row['company_response'],
            row['company'],
            row['month_year'],
            row['state'],
            row['sub_issue'],
            row['timely'],
            row['submitted_via'],
            row['complaint_count']
        ))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_transformed_data()