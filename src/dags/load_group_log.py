from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import pandas as pd

import pendulum

import boto3

import vertica_python


def load_file(table_name: str, key: str, columns: str):

    csv_file_path=f'/lessons/dags/data/{key}'
    df = pd.read_csv(csv_file_path)
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
    num_rows = len(df)
    chunk_size = num_rows // 10
    with vertica_python.connect(vertica_conn_id='vertica_connection') as conn:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            df.loc[start: end].to_csv('/lessons/dags/data/chunk.csv', index=False)
            cur=conn.cursor()
            cur.execute(f"COPY KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.{table_name} ({columns}) FROM LOCAL '/lessons/dags/data/chunk.csv' DELIMITER ',' ENCLOSED BY '''' REJECTED DATA AS TABLE KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.{table_name}_rej;")
            conn.commit()
            start += chunk_size + 1
        conn.close()


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def dag_load_group_log():
    task1_load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_file,
        op_kwargs={'table_name': 'group_log', 'key': 'group_log.csv', 'columns': 'group_id, user_id, user_id_from, event, datetime'},
    )

    task1_load_group_log

sprint6 = dag_load_group_log()