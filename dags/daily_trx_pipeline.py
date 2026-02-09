# Standard Library
import os
import logging
from datetime import datetime, timedelta

# Thrid party
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup


logger = logging.getLogger(__name__)

# Paths
DATA_PATH = "/opt/airflow/data/"
FILENAME = "customer_transactions.csv"
FILEPATH = f"{DATA_PATH}{FILENAME}"

# Postgres
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

WAREHOUSE_DB = os.getenv("WAREHOUSE_DB")
RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw")
RAW_TABLE = os.getenv("RAW_TABLE", "customer_transactions")

# Metadata
CSV_COLS = [
    "transaction_id",
    "customer_id",
    "transaction_date",
    "product_id",
    "product_name",
    "quantity",
    "price",
    "tax",
]


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=WAREHOUSE_DB,
    )


def create_raw_table(    
    schema:str = RAW_SCHEMA,
    table:str = RAW_TABLE
):
    """
    Raw is the landing zone: keep everything as TEXT so COPY never fails.
    Contract will be enforced in dbt staging.
    """
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      transaction_id   TEXT,
      customer_id      TEXT,
      transaction_date TEXT,
      product_id       TEXT,
      product_name     TEXT,
      quantity         TEXT,
      price            TEXT,
      tax              TEXT,
      ingested_at      DATE NOT NULL
    );
    """
    with get_conn() as c:
        with c.cursor() as cur:
            cur.execute(ddl)
        c.commit()


def raw_ingestion(ingestion_mode: str = "ingestion_at"):
    """
    """
    logical_date = context["logical_date"]
    ingested_at = logical_date.date()
    cols_csv = ",".join(CSV_COLS)

    tmp_table = f"{RAW_TABLE}__tmp"
    with _conn() as c:
        with c.cursor() as cur:
            if ingestion_mode == "truncate":
                pass
            #     cur.execute(f"TRUNCATE TABLE {RAW_SCHEMA}.{RAW_TABLE};")

            #     copy_sql = f"""
            #     COPY {RAW_SCHEMA}.{RAW_TABLE}
            #     ({cols_csv})
            #     FROM STDIN WITH (FORMAT csv, HEADER true)
            #     """
            #     with open(FILEPATH, "r", encoding="utf-8") as f:
            #         cur.copy_expert(copy_sql, f)
            elif ingestion_mode == "ingested_at":
                cur.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{tmp_table};")
                cur.execute(
                    f"""
                    CREATE TABLE {RAW_SCHEMA}.{tmp_table} (
                    transaction_id   TEXT,
                    customer_id      TEXT,
                    transaction_date TEXT,
                    product_id       TEXT,
                    product_name     TEXT,
                    quantity         TEXT,
                    price            TEXT,
                    tax              TEXT
                    );
                    """
                )

                # Idempotent: remove today's batch
                cur.execute(
                    f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE ingested_date = %s;",
                    (ingested_date,),
                )

                copy_sql = f"""
                COPY {RAW_SCHEMA}.{tmp_table} ({cols_csv})
                FROM STDIN WITH (FORMAT csv, HEADER true)
                """
                with open(FILEPATH, "r", encoding="utf-8") as f:
                    cur.copy_expert(copy_sql, f)

                cur.execute(
                    f"""
                    INSERT INTO {RAW_SCHEMA}.{RAW_TABLE} ({cols_csv}, ingested_date)
                    SELECT {cols_select}, %s
                    FROM {RAW_SCHEMA}.{tmp_table};
                    """,
                    (ingested_date,),
                )

                cur.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{tmp_table};")

        c.commit()

@task.sensor(
    task_id="check_trx_file",
    retries=0,  # It shouldn't fail under no circumstance, we should instantly see it red in that case
    poke_interval=1 * 60 * 60,  # 1 hour
    timeout=7 * 60 * 60,  # 7 hours
    mode="reschedule",  # Let's free up the worker once finished poking
    soft_fail=False  # Let's skip on no delivery
)
def check_file(**kwargs) -> bool:
    reference_date = kwargs["ds_nodash"]

    if os.path.exists(FILEPATH):
        logger.info(f"Found file delivery!")
        return True
    else:
        logger.warning("File not found!")
        return False


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="daily_transactions_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["case-study", "daily"],
) as dag:

    with TaskGroup("ingest") as ingest:
        wait_for_file = check_file()

        create_raw = PythonOperator(
            task_id="create_raw_table",
            python_callable=create_raw_table,
        )

        ingest_raw = PythonOperator(
            task_id="raw_ingestion",
            python_callable=raw_ingestion,
        )

        wait_for_file >> create_raw >> ingest_raw

    # with TaskGroup("transform") as transform:
    #     dbt_run = BashOperator(
    #         task_id="dbt_run",
    #         bash_command=(
    #             "cd /opt/airflow/dbt_project && "
    #             "dbt run --profiles-dir ."
    #         ),
    #     )

    #     dbt_test = BashOperator(
    #         task_id="dbt_test",
    #         bash_command=(
    #             "cd /opt/airflow/dbt_project && "
    #             "dbt test --profiles-dir ."
    #         ),
    #     )

    #     dbt_run >> dbt_test

    ingest  # >> transform
