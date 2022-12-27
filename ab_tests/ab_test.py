import numpy as np
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago

# default_args = {
#     'owner': 'airflow',
# }

def load_df(ti):
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    res = requests.get(url + '{0}/current.json'.format(state))
    testing_increase = json.loads(res.text)['totalTestResultsIncrease']
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key='testing_increase',
        value=testing_increase
    )

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def taskflow_etl_pandas():
    @task()
    def extract():
        order_data_dict = pd.DataFrame({
            'a': np.random.rand(100000),
            'b': np.random.rand(100000),
        })
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        return {"total_order_value": order_data_dict["a"].sum()}

    @task()
    def load(total_order_value: float):
        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

with DAG(
    'xcom_dag',
    start_date=datetime(2021, 1, 1),
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