from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)



default_args = {
    'start_date' : datetime(2023, 1, 1)
}

with DAG (
    dag_id = 'log_to_spark_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args = default_args
) as dag:

    # Preprocess
    load_to_s3 = SparkSubmitOperator(
        application = "/home/sungjin/airflow/dags/spark_sql_pipeline.py",
        task_id = "preprocess",
        conn_id = "spark_local"
    )

    # Upload_to_S3
    upload_S3 = PythonOperator(
        task_id = "upload_S3",
        python_callable = upload_to_s3,
        op_kwargs = {
            'filename' : '/home/sungjin/airflow/data/{0}.parquet',
            'key' : 'data/log.parquet',
            'bucket_name' : 'log-to-spark'
        }
    )



    load_to_s3 >> upload_S3