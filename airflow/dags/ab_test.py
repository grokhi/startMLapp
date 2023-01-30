import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, ttest_ind
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.pipeline_config import cfgdct


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



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


def load_data_postgresql_db(ti, connection, tablename):
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
    hook = PostgresHook(postgres_conn_id=connection)
    conn = hook.get_conn()


    df = hook.get_pandas_df(
        sql = f"select * from { tablename }", 
        parameters = {'conn_id':conn}
    )

    print('Fetched from table:', tablename )
    print(df.sample(10))

    ti.xcom_push(
        key='df_abtest',
        value=df.to_json()
    )

def ab_test(ti, dataframe):
    """
    Calculates statistics which shows if change in model is statistically significant
    """
    df = pd.read_json(dataframe)

    mean_hitrate = df.groupby('exp_group').hitrate.mean()
    print('Mean Hitrate is:', mean_hitrate)

    mannw =  mannwhitneyu(
        df[df.exp_group == 'control'].hitrate_new,
        df[df.exp_group == 'test'].hitrate_new,
    )

    print(
       mannw
    )
    if mannw[1] < .05: 
        print('Null-hypothesis of metrics equality rejected. It is statistically significant change. Metrics got better!')
    else:
        print('Null-hypothesis of metrics equality is not rejected. Change is not statistically significant.')



with DAG(
    'abtest_dag',
    description='abtest_for_control_and_test_groups',
    start_date=datetime(2023, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag:

    get_abtest_data = PythonOperator(
        task_id = 'fetch_data_from_local_db',
        python_callable=load_data_postgresql_db,
        op_kwargs={
            'connection' : cfgdct['local_conn'],
            'tablename' :  cfgdct['tablename']
        }
    )

    analyze_significance_of_change = PythonOperator(
        task_id='analyze_significance_of_change',
        python_callable=ab_test,
        op_kwargs={
            'dataframe' : "{{ti.xcom_pull(key='df_abtest')}}"
        }
    )

    get_abtest_data >> analyze_significance_of_change