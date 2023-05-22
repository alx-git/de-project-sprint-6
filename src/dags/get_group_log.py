from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable

import pandas as pd

import pendulum

import boto3

import vertica_python

def fetch_s3_file(bucket: str, key: str):

    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/lessons/dags/data/{key}'
    )


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def dag_get_group_log():
    task1_fetch_group_log = PythonOperator(
        task_id='fetch_group_log',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )

    task1_fetch_group_log

sprint6 = dag_get_group_log()