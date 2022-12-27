import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, ttest_ind

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago

# default_args = {
#     'owner': 'airflow',
# }

url = 'https://raw.githubusercontent.com/grokhi/startMLapp/main/ab_tests/tmp_agg.csv'

def load_df(ti):
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    df = pd.read_csv(url)

    ti.xcom_push(
        key='df_abtest',
        value=df
    )

def ab_test(ti):
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    df = ti.xcom_pull(
        key='df_abtest',
        task_ids='load_df'
    )

    ti.xcom_push(
        key='df_abtest',
        value=df
    )


with DAG(
    'abtest_dag',
    start_date=datetime(2022, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    opr_get_covid_data = PythonOperator(
        task_id = 'get_testing_increase_data_{0}'.format(state),
        python_callable=get_testing_increase,
        op_kwargs={'state':state}
    )
    opr_analyze_testing_data = PythonOperator(
        task_id = 'analyze_data',
        python_callable=analyze_testing_increases,
        op_kwargs={'state':state}
    )

    opr_get_covid_data >> opr_analyze_testing_data