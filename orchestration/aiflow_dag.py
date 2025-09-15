from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_process_domain_load',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 9, 15),
    catchup=False,
) as dag:

    # Parallel extract tasks
    extract_common_crawl = BashOperator(
        task_id='extract_common_crawl',
        bash_command='python /path/to/raw_sources/common_crawl/extract.py'
    )

    extract_au_abr = BashOperator(
        task_id='extract_au_abr',
        bash_command='python /path/to/raw_sources/au_abr/extract.py'
    )

    # Processing tasks (depend on both extract tasks)
    process_common_crawl = BashOperator(
        task_id='process_common_crawl',
        bash_command='python /path/to/raw_sources/common_crawl/common_crawl_process.py'
    )

    process_au_abr = BashOperator(
        task_id='process_au_abr',
        bash_command='python /path/to/raw_sources/au_abr/abr_bulk_process.py'
    )

    # Domain match task (depends on processing tasks)
    domain_match = BashOperator(
        task_id='domain_match',
        bash_command='python /path/to/processing/domain_match.py'
    )

    # Final load to Postgres (depends on domain match)
    load_postgres = BashOperator(
        task_id='load_postgres',
        bash_command='python /path/to/target/load_postgres.py'
    )

    # Set task dependencies
    [extract_common_crawl, extract_au_abr] >> [process_common_crawl, process_au_abr] >> domain_match >> load_postgres
