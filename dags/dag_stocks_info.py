from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args = {
    "owner":"AgustinaHermelo",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    
}

with DAG(
    dag_id="dag_stocks_info",
    start_date=datetime(2023, 12, 1),
    catchup=False,
    schedule_interval="@daily",
    default_args=default_args,
    description= "Extracción de informacion de acciones de la API de IEX Cloud y cargarlo en Redshift"
) as dag:



    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/create_tables.sql"
    )

    extract_stocks_data_task = BashOperator(
        task_id="extract_and_load_stocks_data",
        bash_command="python scripts/stocks_etl_main.py"
    )

    create_tables_task >> extract_stocks_data_task