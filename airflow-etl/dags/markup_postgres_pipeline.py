from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import requests
from urllib.parse import urlparse, parse_qs
import time

CSV_FILE = "/opt/airflow/data/sample_data.csv"

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
        CREATE TABLE IF NOT EXISTS markup (
            id SERIAL PRIMARY KEY,
            variant TEXT,
            atom TEXT,
            data TEXT
        )
    """)

    # Read CSV containing markup URLs
    df = pd.read_csv(CSV_FILE, header=None, names=["markup_url"])

    for _, row in df.iterrows():
        markup_url = row["markup_url"]

        # Parse URL
        parsed = urlparse(markup_url)
        query_params = parse_qs(parsed.query)

        variant = query_params.get("variant", [""])[0]  # default empty if missing
        atom = parsed.path  # path part like /cmaj/196/43/E1401.atom

        try:
            # Fetch markup data
            resp = requests.get(markup_url, timeout=30)
            resp.raise_for_status()
            markup_data = resp.text
        except Exception as e:
            markup_data = f"ERROR: {str(e)}"

        # Insert into Postgres
        cur.execute(
            "INSERT INTO markup (variant, atom, data) VALUES (%s, %s, %s)",
            (variant, atom, markup_data)
        )

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="postgres_markup_pipeline",
    start_date=datetime(2025, 8, 14),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id="fetch_and_store_markup",
        python_callable=fetch_and_store_markup,
        retries=3,
        retry_delay=timedelta(seconds=10)
    )
