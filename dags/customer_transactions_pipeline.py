"""
Customer Transactions Data Pipeline

This DAG orchestrates the end-to-end data pipeline for customer transaction data:
1. Extract & Load raw CSV data to PostgreSQL
2. Run dbt transformations (staging layer)
3. Execute data quality tests
4. Build dimensional model (mart layer)
5. Generate data quality report
"""
# Standard Library
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Third-party
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Project
# sys.path.insert(0, '/opt/airflow')
from utils.db_utils import get_db_manager, get_quality_checker


# DAG CONFIG
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "fallback@example.com")
DEFAULT_ARGS = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email': [ADMIN_EMAIL],
    'email_on_failure': True,  # Requires filling up SMTP env variables
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'catchup': False
}

# COLUMNS
CSV_COLS = [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "product_id",
        "product_name",
        "quantity",
        "price",
        "tax"
]

# FILE PATHS
DATA_DIR = Path('/opt/airflow/data')
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
SOURCE_FILE = DATA_DIR / 'customer_transactions.csv'

# DB CONFIG
POSTGRES_CONN_ID = 'postgres_default'
RAW_SCHEMA = 'raw'
RAW_TABLE = 'customer_transactions'


# --- HELPER FUNCTIONS ---

def log_data_quality_metrics(df: pd.DataFrame, stage: str) -> dict:
    """
    Log data quality metrics for observability.
    
    Args:
        df: DataFrame to analyze
        stage: Pipeline stage name
        
    Returns:
        Dictionary of metrics
    """
    metrics = {
        'stage': stage,
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'null_counts': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2
    }
    
    logging.info(f"Data Quality Metrics - {stage}:")
    logging.info(f"  Total Rows: {metrics['total_rows']:,}")
    logging.info(f"  Total Columns: {metrics['total_columns']}")
    logging.info(f"  Duplicate Rows: {metrics['duplicate_rows']}")
    logging.info(f"  Memory Usage: {metrics['memory_usage_mb']:.2f} MB")
    logging.info(f"  Null Counts: {json.dumps(metrics['null_counts'], indent=4)}")
    
    return metrics


# --- EXTRACT & LOAD TASKS ---

# Could be turned to a PythonSensor
def validate_source_file(**context):
    """
    Validate that the source CSV file exists and is readable.
    Performs basic file validation before processing.
    """
    logging.info(f"Validating source file: {SOURCE_FILE}")
    
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    if SOURCE_FILE.stat().st_size == 0:
        raise ValueError(f"Source file is empty: {SOURCE_FILE}")
    
    # Try to read first few lines to validate format
    try:
        df_sample = pd.read_csv(SOURCE_FILE, nrows=5)
        logging.info(f"File validation successful. Columns: {list(df_sample.columns)}")
        logging.info(f"File size: {SOURCE_FILE.stat().st_size / 1024:.2f} KB")
        
        # Push metadata to XCom for downstream tasks
        context['ti'].xcom_push(key='source_file_path', value=str(SOURCE_FILE))
        context['ti'].xcom_push(key='source_file_size_kb', value=SOURCE_FILE.stat().st_size / 1024)
        
    except Exception as e:
        raise ValueError(f"Failed to read CSV file: {str(e)}")

def extract_and_load_raw_data(**context):
    """
    Extract data from CSV and load to raw schema.

    Performs the following steps:
    - Load CSV with minimal transformations
    - Add metadata columns (ingested_at, source_file)
    - Use chunked loading for memory efficiency
    - Implement truncate-and-load pattern for idempotency
    """
    execution_date = context['execution_date']

    logging.info(f"Starting raw data extraction from: {SOURCE_FILE}")
    logging.info(f"Execution date: {execution_date}")

    # Read CSV with explicit dtype for problematic columns
    # Keep as strings initially to preserve raw data
    dtype_spec = {
        col: str
        for col in CSV_COLS
    }

    try:
        df = pd.read_csv(SOURCE_FILE, dtype=dtype_spec)
        logging.info(f"Successfully loaded {len(df)} rows from CSV")

        # Log initial data quality metrics
        log_data_quality_metrics(df, 'Raw CSV Load')

        # Add metadata columns
        df['ingested_at'] = execution_date.isoformat()
        df['source_file'] = SOURCE_FILE.name
        df['pipeline_run_id'] = context['run_id']

        # Get database connection
        db = get_db_manager(POSTGRES_CONN_ID)

        try:
            # Create raw table if not exists
            csv_cols_with_types = ", ".join([f"{col} TEXT" for col in CSV_COLS])
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
                {csv_cols_with_types},
                ingested_at TIMESTAMP,
                source_file VARCHAR(255),
                pipeline_run_id VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            db.execute_ddl(create_table_sql)
            logging.info(f"Ensured table exists: {RAW_SCHEMA}.{RAW_TABLE}")

            # Truncate table for idempotent loads
            db.truncate_table(RAW_SCHEMA, RAW_TABLE)
            logging.info(f"Truncated table: {RAW_SCHEMA}.{RAW_TABLE}")

            # Load data in chunks for memory efficiency
            rows_inserted = db.bulk_insert_dataframe(
                df=df,
                schema=RAW_SCHEMA,
                table=RAW_TABLE,
                if_exists='append',
                chunk_size=1000
            )

            # Verify row count
            db_count = db.get_row_count(RAW_SCHEMA, RAW_TABLE)
        
            if db_count != len(df):
                raise ValueError(
                    f"Row count mismatch! CSV: {len(df)}, Database: {db_count}"
                )

            logging.info(f"Successfully loaded {db_count} rows to {RAW_SCHEMA}.{RAW_TABLE}")

            # Push metrics to XCom
            context['ti'].xcom_push(key='raw_rows_loaded', value=db_count)
            context['ti'].xcom_push(key='raw_load_timestamp', value=str(execution_date))

        except Exception as e:
            logging.error(f"Failed to load raw data: {str(e)}")
            raise

    except Exception as e:
        logging.error(f"Failed to load raw data: {str(e)}")
        raise

def validate_raw_data_loaded(**context):
    """
    Validate that raw data was successfully loaded.
    Performs basic sanity checks on the raw table:
    - Table exists and is accessible
    - Required columns are present
    - Can actually SELECT from it (permissions, locks)
    - Sample data looks reasonable
    """
    db = get_db_manager(POSTGRES_CONN_ID)

    # Check row count
    row_count = db.get_row_count('raw', 'customer_transactions')
    if row_count == 0:
        raise ValueError("No data found in raw.customer_transactions")

    logging.info(f"✓ Validation passed: {row_count} rows in raw table")

    # Check for required columns
    columns = db.get_table_columns('raw', 'customer_transactions')
    required_columns = CSV_COLS

    missing_columns = set(required_columns) - set(columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    logging.info(f"All required columns present: {required_columns}")

    # Get sample data for logging
    sample_query = "SELECT * FROM raw.customer_transactions LIMIT 3;"
    sample_rows = db.execute_query(sample_query)

    logging.info("Sample data (first 3 rows):")
    for i, row in enumerate(sample_rows, 1):
        logging.info(f"  Row {i}: {row}")
   

# --- DBT TRANSFORMATION TASKS ---

def run_dbt_command(command: str, project_dir: str = DBT_PROJECT_DIR) -> str:
    """
    Helper function to construct dbt commands.

    Args:
        command: dbt command (e.g., 'run', 'test')
        project_dir: Path to dbt project

    Returns:
        Full bash command string
    """
    return f"""
        cd {project_dir} && \
        dbt {command} --profiles-dir . --target prod
    """


# --- DATA QUALITY REPORT ---

def generate_data_quality_report(**context):
    """
    Generate comprehensive data quality report using DataQualityChecker.
    
    Runs platform-standard quality checks across all data layers:
    - Raw layer: Basic completeness checks
    - Staging layer: Comprehensive data quality validation
    - Mart layer: Business rule validation and referential integrity
    """
    checker = get_quality_checker()
    db = get_db_manager()

    overall_report = {
        'execution_date': str(context['execution_date']),
        'pipeline_run_id': context['run_id'],
        'layers': {},
        'summary': {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warned_checks': 0
        }
    }

    logging.info("=" * 80)
    logging.info("COMPREHENSIVE DATA QUALITY REPORT")
    logging.info("=" * 80)
    logging.info(f"Execution Date: {overall_report['execution_date']}")
    logging.info(f"Pipeline Run ID: {overall_report['pipeline_run_id']}")
    logging.info("=" * 80)

    # --- LAYER 1: RAW DATA CHECKS ---
    try:
        logging.info("\n[RAW LAYER] Checking data completeness...")

        raw_count = db.get_row_count(RAW_SCHEMA, RAW_TABLE)
        overall_report['layers']['raw'] = {
            'row_count': raw_count,
            'status': 'PASS' if raw_count > 0 else 'FAIL'
        }

        logging.info(f"✓ Raw data loaded: {raw_count} rows")

    except Exception as e:
        logging.error(f"✗ Raw layer check failed: {str(e)}")
        overall_report['layers']['raw'] = {
            'row_count': 0,
            'status': 'FAIL',
            'error': str(e)
        }

    # --- LAYER 2: STAGING CHECKS ---
    try:
        # Check if staging table exists
        if db.table_exists('staging', 'stg_transactions'):
            logging.info("\n[STAGING LAYER] Running comprehensive quality checks...")

            # Define staging quality suite
            staging_checks = [
                # Primary key checks
                {'type': 'not_null', 'column': 'transaction_id'},
                {'type': 'unique', 'column': 'transaction_id'},

                # Timestamp columns checks
                {'type': 'not_null', 'column': 'transaction_date'},

                # Product checks
                {'type': 'not_null', 'column': 'product_id'},
                {'type': 'not_null', 'column': 'product_name'},

                # Numeric range validations
                {'type': 'range', 'column': 'price', 'min_value': 0, 'max_value': 10000},
                {'type': 'range', 'column': 'quantity', 'min_value': 0, 'max_value': 100},
                {'type': 'range', 'column': 'tax', 'min_value': 0, 'max_value': 1000},

                # Calculated field validations
                {'type': 'range', 'column': 'line_subtotal', 'min_value': 0},
                {'type': 'range', 'column': 'line_total', 'min_value': 0},
            ]

            # Run quality suite
            staging_results = checker.run_quality_suite(
                schema='staging',
                table='stg_transactions',
                checks_config=staging_checks
            )

            overall_report['layers']['staging'] = staging_results

            # Update summary
            overall_report['summary']['total_checks'] += staging_results['total_checks']
            overall_report['summary']['passed_checks'] += staging_results['passed_checks']
            overall_report['summary']['failed_checks'] += staging_results['failed_checks']

            # Log staging results
            logging.info(
                f"Staging Checks: {staging_results['passed_checks']}/{staging_results['total_checks']} passed"
            )

            # Log failed checks in detail
            for check in staging_results['checks']:
                if not check['passed']:
                    logging.warning(
                        f"WARNING - {check['check_type'].upper()} check failed on "
                        f"{check['table']}.{check['column']}: "
                        f"{check.get('null_rows', 0)} issues found"
                    )

        else:
            logging.warning("✗ Staging table not found - skipping staging checks")
            overall_report['layers']['staging'] = {
                'status': 'SKIPPED',
                'reason': 'Table does not exist'
            }

    except Exception as e:
        logging.error(f"✗ Staging layer check failed: {str(e)}")
        overall_report['layers']['staging'] = {
            'status': 'ERROR',
            'error': str(e)
        }

    # --- LAYER 3: MART CHECKS ---
    try:
        logging.info("\n[MART LAYER] Checking dimensional model...")

        mart_results = {
            'tables': {},
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0
        }

        # Check fact table
        if db.table_exists('mart', 'fact_transactions'):
            fact_count = db.get_row_count('mart', 'fact_transactions')

            # Define fact table checks
            fact_checks = [
                {'type': 'not_null', 'column': 'transaction_id'},
                {'type': 'unique', 'column': 'transaction_id'},
                {'type': 'not_null', 'column': 'customer_key'},
                {'type': 'not_null', 'column': 'product_key'},
                {'type': 'not_null', 'column': 'date_key'},
                {'type': 'range', 'column': 'quantity', 'min_value': 0},
                {'type': 'range', 'column': 'unit_price', 'min_value': 0},
                {'type': 'range', 'column': 'line_total', 'min_value': 0},
                {'type': 'range', 'column': 'data_quality_score', 'min_value': 0, 'max_value': 7},
            ]

            fact_results = checker.run_quality_suite(
                schema='mart',
                table='fact_transactions',
                checks_config=fact_checks
            )

            mart_results['tables']['fact_transactions'] = {
                'row_count': fact_count,
                'quality_results': fact_results
            }

            mart_results['total_checks'] += fact_results['total_checks']
            mart_results['passed_checks'] += fact_results['passed_checks']
            mart_results['failed_checks'] += fact_results['failed_checks']

            logging.info(f"✓ Fact table: {fact_count} rows, "
                        f"{fact_results['passed_checks']}/{fact_results['total_checks']} checks passed")

        # Check dimension tables
        dimension_tables = [
            ('dim_customers', 'customer_id'),
            ('dim_products', 'product_id'),
            ('dim_dates', 'date_day')
        ]

        for table_name, pk_column in dimension_tables:
            if db.table_exists('mart', table_name):
                dim_count = db.get_row_count('mart', table_name)

                # Basic checks for dimensions
                dim_checks = [
                    {'type': 'not_null', 'column': pk_column},
                    {'type': 'unique', 'column': pk_column},
                ]

                dim_results = checker.run_quality_suite(
                    schema='mart',
                    table=table_name,
                    checks_config=dim_checks
                )

                mart_results['tables'][table_name] = {
                    'row_count': dim_count,
                    'quality_results': dim_results
                }

                mart_results['total_checks'] += dim_results['total_checks']
                mart_results['passed_checks'] += dim_results['passed_checks']
                mart_results['failed_checks'] += dim_results['failed_checks']

                logging.info(f"✓ {table_name}: {dim_count} rows, "
                           f"{dim_results['passed_checks']}/{dim_results['total_checks']} checks passed")

        overall_report['layers']['mart'] = mart_results

        # Update summary
        overall_report['summary']['total_checks'] += mart_results['total_checks']
        overall_report['summary']['passed_checks'] += mart_results['passed_checks']
        overall_report['summary']['failed_checks'] += mart_results['failed_checks']

    except Exception as e:
        logging.error(f"✗ Mart layer check failed: {str(e)}")
        overall_report['layers']['mart'] = {
            'status': 'ERROR',
            'error': str(e)
        }

    # --- FINAL REPORT SUMMARY ---
    logging.info("\n" + "=" * 80)
    logging.info("QUALITY REPORT SUMMARY")
    logging.info("=" * 80)

    # Calculate overall pass rate
    if overall_report['summary']['total_checks'] > 0:
        pass_rate = (overall_report['summary']['passed_checks'] /
                    overall_report['summary']['total_checks'] * 100)
    else:
        pass_rate = 0

    logging.info(f"Total Checks Run: {overall_report['summary']['total_checks']}")
    logging.info(f"Passed: {overall_report['summary']['passed_checks']} ✓")
    logging.info(f"Failed: {overall_report['summary']['failed_checks']} ✗")
    logging.info(f"Pass Rate: {pass_rate:.1f}%")
    logging.info("=" * 80)

    # Push comprehensive report to XCom
    context['ti'].xcom_push(key='comprehensive_quality_report', value=overall_report)

    # Also push individual layer results for easier access
    if 'staging' in overall_report['layers']:
        context['ti'].xcom_push(key='staging_quality_results', value=overall_report['layers']['staging'])

    if 'mart' in overall_report['layers']:
        context['ti'].xcom_push(key='mart_quality_results', value=overall_report['layers']['mart'])

    # Determine if pipeline should fail
    critical_failures = overall_report['summary']['failed_checks']

    if critical_failures > 0:
        logging.error(f"✗ Pipeline has {critical_failures} critical quality check failures!")
        raise ValueError(
            f"Data quality validation failed: {critical_failures} checks failed. "
            f"Pass rate: {pass_rate:.1f}%"
        )

    logging.info("All data quality checks passed!")
    return overall_report


# --- DAG DEFINITION ---

with DAG(
    dag_id='customer_transactions_pipeline',
    default_args=DEFAULT_ARGS,
    description='End-to-end pipeline for customer transaction data',
    schedule_interval='@daily',  # Run daily (can be adjusted)
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Don't backfill historical runs
    max_active_runs=1,  # Prevent concurrent runs
    tags=['data-platform', 'transactions', 'dbt', 'postgres'],
    doc_md=__doc__,
) as dag:

    # TASK 1: Validate Source File

    validate_file = PythonOperator(
        task_id='validate_source_file',
        python_callable=validate_source_file,
        doc_md="""
        Validates that the source CSV file exists and is readable.
        Performs basic sanity checks before processing.
        """
    )

    # TASK 2: Extract & Load Raw Data

    extract_load = PythonOperator(
        task_id='extract_load_raw_data',
        python_callable=extract_and_load_raw_data,
        doc_md="""
        Extracts data from CSV and loads to raw.customer_transactions.
        Implements truncate-and-load pattern for idempotency.
        """
    )

    # TASK 3: Validate Raw Data
    validate_raw = PythonOperator(
        task_id='validate_raw_data',
        python_callable=validate_raw_data_loaded,
        doc_md="""
        Validates that raw data was successfully loaded.
        Checks row counts and required columns.
        """
    )
    
    # TASK GROUP: dbt Staging Transformations

    with TaskGroup(group_id='dbt_staging', tooltip='dbt staging layer transformations') as dbt_staging:

        dbt_run_staging = BashOperator(
            task_id='run_staging_models',
            bash_command=run_dbt_command('run --models staging'),
            doc_md="""
            Runs dbt staging models to clean and standardize raw data.
            Handles data type conversions, null values, and format inconsistencies.
            """
        )

        dbt_test_staging = BashOperator(
            task_id='test_staging_models',
            bash_command=run_dbt_command('test --models staging'),
            doc_md="""
            Executes dbt tests on staging models.
            Validates uniqueness, not-null constraints, and accepted values.
            """
        )

        dbt_run_staging >> dbt_test_staging

    # TASK GROUP: dbt Mart Transformations

    with TaskGroup(group_id='dbt_mart', tooltip='dbt mart layer transformations') as dbt_mart:

        dbt_run_dimensions = BashOperator(
            task_id='run_dimension_models',
            bash_command=run_dbt_command('run --models mart.dimensions'),
            doc_md="""
            Builds dimension tables (customers, products, dates).
            Creates business-ready dimensional model.
            """
        )

        dbt_run_facts = BashOperator(
            task_id='run_fact_models',
            bash_command=run_dbt_command('run --models mart.facts'),
            doc_md="""
            Builds fact tables (transactions).
            Includes calculated fields and foreign key references.
            """
        )

        dbt_run_aggregates = BashOperator(
            task_id='run_aggregate_models',
            bash_command=run_dbt_command('run --models mart.aggregates'),
            doc_md="""
            Builds aggregate/summary tables.
            Provides pre-computed metrics for reporting.
            """
        )

        dbt_test_mart = BashOperator(
            task_id='test_mart_models',
            bash_command=run_dbt_command('test --models mart'),
            doc_md="""
            Executes dbt tests on mart models.
            Validates business rules and referential integrity.
            """
        )

        # Dependencies within mart group
        [dbt_run_dimensions, dbt_run_facts] >> dbt_run_aggregates >> dbt_test_mart

    # TASK 4: Generate Data Quality Report

    quality_report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_data_quality_report,
        doc_md="""
        Generates comprehensive data quality report.
        Validates completeness and correctness across all layers.
        """
    )

    # TASK 5: dbt Documentation (Optional)

    dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=run_dbt_command('docs generate'),
        trigger_rule='all_done',  # Run even if upstream tasks fail
        doc_md="""
        Generates dbt documentation.
        Creates data catalog and lineage graphs.
        """
    )

    # TASK DEPENDENCIES

    validate_file >> extract_load >> validate_raw >> dbt_staging
    dbt_staging >> dbt_mart >> quality_report
    quality_report >> dbt_docs
