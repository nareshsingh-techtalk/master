from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import requests
from urllib.parse import urlparse
import time

CSV_FILE = "/opt/airflow/data/api_data.csv"  # Updated CSV file

def fetch_and_store_markup():
    # Wait for Postgres in case container just started
    time.sleep(5)

    conn = psycopg2.connect(
        dbname="airflow_db",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atomdata (
            id SERIAL PRIMARY KEY,
            variant TEXT,
            atom TEXT,
            data TEXT
        )
    """)

    # Read CSV containing API URLs
    df = pd.read_csv(CSV_FILE, header=None, names=["api_url"])

    for _, row in df.iterrows():
        api_url = row["api_url"]

        # Parse URL
        parsed = urlparse(api_url)

        # Variant is fixed for this API
        variant = "full-text"

        # Derive .atom path from .full.html
        atom_path = parsed.path.replace(".full.html", ".atom")

        try:
            # Fetch data from API
            resp = requests.get(api_url, timeout=30)
            resp.raise_for_status()
            api_data = resp.text
        except Exception as e:
            api_data = f"ERROR: {str(e)}"

        # Insert into Postgres
        cur.execute(
            "INSERT INTO atomdata (variant, atom, data) VALUES (%s, %s, %s)",
            (variant, atom_path, api_data)
        )

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="postgres_atom_pipeline",
    start_date=datetime(2025, 8, 14),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id="fetch_and_store_atomdata",
        python_callable=fetch_and_store_markup,
        retries=3,
        retry_delay=timedelta(seconds=10)
    )
