from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import psycopg2
import os
import sys

from catboost import CatBoostClassifier


os.environ["HOST_DB"] = '192.168.176.4'
os.environ["HOST_DB_port"] = '5432'
os.environ["DB_name"] = 'airflow'
os.environ["USER_DB"] ='airflow'
os.environ["PASSWORD_DB"] = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def get_data_and_predict(ti, **kwargs):

    conn = psycopg2.connect(
        host=os.environ["HOST_DB"], 
        port = os.environ["HOST_DB_port"], 
        database=os.environ["DB_name"], 
        user=os.environ["USER_DB"], 
        password=os.environ["PASSWORD_DB"]
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM features limit 1")
    query_results = cur.fetchall()

    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])

    conn.close()
    cur.close()
    data = pd.DataFrame(query_results, columns=col_names)

    model = CatBoostClassifier()
    model.load_model(
        'recommendation_models/catboost_base_model'
    )

    predicts = model.predict_proba(data)[:, 1]

    ti.xcom_push(
        key='model_preds', 
        value=predicts
    )
                    

with DAG(
    'catboost_model_predict', 
    description='ml_model_predict', 
    start_date=datetime(2022, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag: 

        get_data_and_predict  = PythonOperator(
            task_id='get_data_and_predict', 
            python_callable=get_data_and_predict
        )

        insert_in_table = PostgresOperator(
            task_id = "insert_in_table",
            postgres_conn_id = "postgres_local",
            sql = [f"""
                INSERT INTO iris_predict VALUES(
                {{ {{ ti.xcom_pull(
                    key='model_preds', 
                    task_ids=['get_data_and_predict'])[0][0] 
                }} }},
                {{ {{ ti.xcom_pull(
                    key='model_preds', 
                    task_ids=['get_data_and_predict'])[0][1] 
                }} }})
            """]
            )

        get_data_and_predict >> insert_in_table

# Добавить в docker-compose

    # database:
    #     image: "postgres" # use latest official postgres version
    #     env_file:
    #         - database.env # configure postgres
    #     ports:
    #         - "5423:5432"

#  database.env
# POSTGRES_USER=unicorn_user
# POSTGRES_PASSWORD=magical_password
# POSTGRES_DB=rainbow_database