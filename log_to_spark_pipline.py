from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import pymysql

default_args = {
    'start_date' : datetime(2023, 1, 1)
}

with DAG (
    dag_id = 'log_to_spark_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args = default_args
) as dag:

    spark_to_s3_load = MySqlOperator()

    # Spark에서 정제

    # MySql에 담아주기
    pass