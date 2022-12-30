import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, ttest_ind
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


test_model_name = 'new_model'


def load_data_url(ti):
    """
    Option 1. URL way.
    As alternative to local database creation you can simply load ab-test.csv with url.

    Function loads df from url with already calculated Hitrate for recommend. system
    """
    url = 'https://raw.githubusercontent.com/grokhi/startMLapp/main/ab_tests/ab_test.csv'
    df = pd.read_csv(url)

    ti.xcom_push(
        key='df_abtest',
        value=df.to_json()
    )


def load_data_postgresql_db(ti):
    """
    Option 2. Loads df from local postrges database.
    df contains already calculated hitrate metrics for recommendation system.

    Instructions:
    1. Insert ab_test.csv into postgresql database manually (f.e. using Dbeaver)
    2. Create connection on airflow webserver with specs:
        Connection Id : postgres_localhost
        Connection Type : Postgres
        Host : host.docker.internal
        Schema : abtest_db
        Login : airflow
        Password : airflow
        Port : 5432 
    """
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()


    df = hook.get_pandas_df(
        sql = "select * from abtest", 
        parameters = {'conn_id':conn}
    )

    print('DataFrame columns:', df.columns)

    ti.xcom_push(
        key='df_abtest',
        value=df.to_json()
    )

def ab_test(ti):
    """
    Calculates statistics which shows if change in model is statistically significant
    """
    df = pd.read_json(
        ti.xcom_pull(
            key='df_abtest',
            task_ids=f'load_df_{test_model_name}'
        )
    )

    mannw =  mannwhitneyu(
        df[df.exp_group == 'control'].hitrate_new,
        df[df.exp_group == 'test'].hitrate_new,
    )

    print(
       mannw
    )
    if mannw[1] < .05: 
        print('Statistically significant change. Metrics got better!')
    else:
        print('Change is not statistically significant.')



with DAG(
    'abtest_dag',
    description='abtest_for_control_and_test_groups',
    start_date=datetime(2022, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag:
    get_abtest_data = PythonOperator(
        task_id = f'load_df_{test_model_name}',
        python_callable=load_data_postgresql_db,
        op_kwargs={'test_model_name':test_model_name}
    )
    analyze_significance_of_change = PythonOperator(
        task_id='analyze_data',
        python_callable=ab_test,
        op_kwargs={'test_model_name':test_model_name}
    )

    get_abtest_data >> analyze_significance_of_change