from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta

# ---------------------------
# Failure Alert Function
# ---------------------------
def notify_failure(context):
    subject = f"Airbnb Pipeline Failed: {context['task_instance'].task_id}"
    body = f"""
    DAG: {context['dag'].dag_id} <br>
    Task: {context['task_instance'].task_id} <br>
    Execution Time: {context['execution_date']} <br>
    Log: {context['task_instance'].log_url}
    """
    send_email(
        to="your_email@gmail.com",
        subject=subject,
        html_content=body
    )

# ---------------------------
# Default Arguments
# ---------------------------
default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ---------------------------
# DAG Definition
# ---------------------------
with DAG(
    dag_id='airbnb_dbt_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    on_failure_callback=notify_failure
) as dag:

    # ---------------------------
    # BRONZE LAYER (Incremental)
    # ---------------------------
    bronze_bookings = BashOperator(
        task_id='bronze_bookings',
        bash_command='dbt run --models bronze_bookings'
    )

    bronze_hosts = BashOperator(
        task_id='bronze_hosts',
        bash_command='dbt run --models bronze_hosts'
    )

    bronze_listings = BashOperator(
        task_id='bronze_listings',
        bash_command='dbt run --models bronze_listings'
    )

    # ---------------------------
    # SILVER LAYER
    # ---------------------------
    silver_bookings = BashOperator(
        task_id='silver_bookings',
        bash_command='dbt run --models silver_bookings'
    )

    silver_hosts = BashOperator(
        task_id='silver_hosts',
        bash_command='dbt run --models silver_hosts'
    )

    silver_listings = BashOperator(
        task_id='silver_listings',
        bash_command='dbt run --models silver_listings'
    )

    # ---------------------------
    # DATA QUALITY TEST (Silver)
    # ---------------------------
    test_silver = BashOperator(
        task_id='test_silver',
        bash_command='dbt test --models silver'
    )

    # ---------------------------
    # GOLD LAYER - OBT
    # ---------------------------
    obt = BashOperator(
        task_id='create_obt',
        bash_command='dbt run --models obt'
    )

    # ---------------------------
    # GOLD LAYER - FACT
    # ---------------------------
    fact = BashOperator(
        task_id='create_fact',
        bash_command='dbt run --models fact'
    )

    # ---------------------------
    # FINAL DATA TEST
    # ---------------------------
    test_gold = BashOperator(
        task_id='test_gold',
        bash_command='dbt test --models gold'
    )

    # ---------------------------
    # DEPENDENCY FLOW
    # ---------------------------

    # Bronze → Silver
[bronze_bookings, bronze_hosts, bronze_listings] >> [
    silver_bookings, silver_hosts, silver_listings
]

# Silver → Test
[silver_bookings, silver_hosts, silver_listings] >> test_silver

# Gold flow
test_silver >> obt >> fact >> test_gold