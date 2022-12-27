import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, ttest_ind
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago

# default_args = {
#     'owner': 'airflow',
# }

url = 'https://raw.githubusercontent.com/grokhi/startMLapp/main/ab_tests/ab_test.csv'
test_model_name = 'new_model'

def load_df(ti):
    """
    Loads df from url with already calculated Hitrate for recommend. system
    """
    df = pd.read_csv(url)

    ti.xcom_push(
        key='df_abtest',
        value=df
    )

def ab_test(ti):
    """
    Calculates statistics which shows if change in model is statistically significant
    """
    df = ti.xcom_pull(
        key='df_abtest',
        task_ids=f'load_df_{test_model_name}'
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'abtest_dag',
    start_date=datetime(2022, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag:
    get_abtest_data = PythonOperator(
        task_id = f'load_df_{test_model_name}',
        python_callable=load_df,
        op_kwargs={'test_model_name':test_model_name}
    )
    analyze_signidicance_of_change = PythonOperator(
        task_id = 'analyze_data',
        python_callable=ab_test,
        op_kwargs={'test_model_name':test_model_name}
    )

    get_abtest_data >> analyze_signidicance_of_change