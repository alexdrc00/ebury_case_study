from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# Configuration
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
CSV_PATH = "/opt/airflow/data/customer_transactions.csv"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project/ebury_transform"

def ingest_csv_to_postgres():
    """Efficiently loads CSV data into the 'public' schema using chunks."""
    engine = create_engine(DB_CONN)
    
    # Reading in chunks to satisfy memory efficiency requirement 
    df_iter = pd.read_csv(CSV_PATH, chunksize=1000)
    
    for i, chunk in enumerate(df_iter):
        # The first chunk 'replaces' the table, subsequent chunks 'append'
        if_exists_val = 'replace' if i == 0 else 'append'
        chunk.to_sql('raw_transactions', engine, if_exists=if_exists_val, index=False)
        print(f"Ingested chunk {i}")

with DAG(
    dag_id='ebury_full_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)} # [cite: 30]
) as dag:

    # Task 1: Ingest Data [cite: 27]
    ingest_task = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_csv_to_postgres
    )

    # Task 2: Transform Data with dbt 
    # We use the local profiles-dir for portability as we discussed
    dbt_run = BashOperator(
        task_id='dbt_transform',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir ."
    )

    ingest_task  # >> dbt_run
