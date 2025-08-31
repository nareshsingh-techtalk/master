from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import time

CSV_FILE = "/opt/airflow/data/sample_data.csv"

def load_csv_to_postgres():
    # Wait for Postgres to be ready
    time.sleep(5)
    conn = psycopg2.connect(
        dbname="airflow_db",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS people (
            id SERIAL PRIMARY KEY,
            name TEXT,
            age INT
        )
    """)
    df = pd.read_csv(CSV_FILE)
    for _, row in df.iterrows():
        cur.execute("INSERT INTO people (name, age) VALUES (%s, %s)", (row['name'], row['age']))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="postgres_csv_pipeline",
    start_date=datetime(2025, 8, 14),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

