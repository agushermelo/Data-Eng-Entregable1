from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.main import load_measurements_data, load_stations_data

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_catchup",
    start_date=datetime(2023, 12, 01),
    catchup=False,
    schedule_interval="@daily",
    default_args=default_args
) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/create_tables.sql",
        hook_params={	
            "options": "-c search_path=agustinahermelo_coderhouse_schema"
        }
    )

    load_stations_data_task = PythonOperator(
        task_id="load_stations_data",
        python_callable=load_stations_data,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini"
        }
    )

    load_measurements_data_task = PythonOperator(
        task_id="load_measurements_data",
        python_callable=load_measurements_data,
        op_kwargs={
            "config_file": "/Coder-Data-Eng/config/config.ini",
            "start": "{{ data_interval_start }}",
            "end": "{{ data_interval_end }}"
        }
    )

    dummy_end_task = DummyOperator(
        task_id="end"
    )

    dummy_start_task >> create_tables_task
    create_tables_task >> load_stations_data_task
    create_tables_task >> load_measurements_data_task
    # dummy_start_task >> load_stations_data_task
    # dummy_start_task >> load_measurements_data_task

    load_stations_data_task >> dummy_end_task
    load_measurements_data_task >> dummy_end_task

    #dummy_start_task >> create_tables_task >> load_measurements_data_task >> dummy_end_task