"""
DOCU
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

# from airflow.settings import AIRFLOW_HOME

from utils.get_exp_groups import get_exp_groups
from utils.get_features import get_features
from utils.get_hitrate import get_hitrate
from utils.get_abtest_table import get_abtest_table
from utils.get_recommendations import get_recommendations_model
from utils.postgres_ops import fetchfrompostgres, upload2postgres
from utils.pipeline_config import cfg_dict


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'get_abtest_table', 
    description='Get a data prepared for A/B test of recommender models', 
    start_date=datetime(2023, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag:

    dag.doc_md = __doc__

    with TaskGroup(group_id='check_config') as check_config:
        t1 = DummyOperator(task_id='check_local_conn')
        t2 = DummyOperator(task_id='check_external_conn')

        [t1, t2]

    fetchfrompostgres = PythonOperator(
        task_id = 'fetch_data_from_local_db',
        python_callable=fetchfrompostgres,
        op_kwargs={
            'connection' : cfg_dict['local_conn']
        }
    )

    get_exp_groups = PythonOperator(
        task_id = 'get_exp_groups',
        python_callable=get_exp_groups,
        op_kwargs={
            'salt' : cfg_dict['salt'],
            'dataframe' : "{{ ti.xcom_pull(key='views') }}"
        }
    )

    get_features = PythonOperator(
        task_id = 'get_features_from_external_db',
        python_callable=get_features,
        op_kwargs={
            'external_connection' : cfg_dict['external_conn'],
        }
    )

    get_recommendations_base_model  = PythonOperator(
        task_id='get_recommendations_base_model',
        python_callable=get_recommendations_model,
        op_kwargs={
            'dataframe' : "{{ ti.xcom_pull(key='control_id') }}",
            'likes' : "{{ ti.xcom_pull(key='likes') }}",
            'user_features' : "{{ ti.xcom_pull(key='user_features') }}",
            'model_posts_features' : "{{ ti.xcom_pull(key='base_model_posts_features') }}",
            'model_path' : '/opt/airflow/dags/recommendation_models/catboost_base_model',
            'output_key' : 'base_model_recommendations'
        }
    )

    get_recommendations_enhanced_model  = PythonOperator(
        task_id='get_recommendations_enhanced_model',
        python_callable=get_recommendations_model,
        op_kwargs={
            'dataframe' : "{{ ti.xcom_pull(key='test_id') }}",
            'likes' : "{{ ti.xcom_pull(key='likes') }}",
            'user_features' : "{{ ti.xcom_pull(key='user_features') }}",
            'model_posts_features' : "{{ ti.xcom_pull(key='enhanced_model_posts_features') }}",
            'model_path' : '/opt/airflow/dags/recommendation_models/catboost_enhanced_model',
            'output_key' : 'enhanced_model_recommendations'
        }
    )

    get_hitrate =  PythonOperator(
        task_id='get_hitrate',
        python_callable=get_hitrate,
        op_kwargs={
            'likes' : "{{ ti.xcom_pull(key='likes') }}",
            'base_model_recommendations' : "{{ ti.xcom_pull(key='base_model_recommendations') }}",
            'enhanced_model_recommendations' : "{{ ti.xcom_pull(key='enhanced_model_recommendations') }}",
        }       
    )

    get_abtest_table =  PythonOperator(
        task_id='get_abtest_table',
        python_callable=get_abtest_table,
        op_kwargs={
            'dataframe' : "{{ ti.xcom_pull(key='hitrate_df') }}",
        }  
    )

    create_table = PostgresOperator(
        task_id="create_table_local_db",
        postgres_conn_id="postgres_localhost",
        sql= f"""
            DROP TABLE IF EXISTS { cfg_dict['tablename'] };
            CREATE TABLE IF NOT EXISTS { cfg_dict['tablename'] } (
            exp_group varchar(50) NULL,
            bucket int4 NULL,
            hitrate int4 NULL,
            "view" int4 NULL,
            hitrate_new float4 NULL
            );
        """,
    )

    upload2postgres = PythonOperator(
        task_id = 'insert_abtest_table_to_local_db',
        python_callable=upload2postgres,
        op_kwargs={
            'tablename': cfg_dict['tablename'],
            'connection' : cfg_dict['local_conn'],
            'dataframe' : "{{ ti.xcom_pull(key='abtest_df') }}"
        }
    )

    check_config >> \
    fetchfrompostgres >> get_exp_groups >> get_features \
    >> [get_recommendations_base_model, get_recommendations_enhanced_model] \
    >> get_hitrate >> get_abtest_table \
    >> create_table >> upload2postgres

