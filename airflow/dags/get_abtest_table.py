from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.settings import AIRFLOW_HOME

import psycopg2
import os
import sys

from catboost import CatBoostClassifier



CONN_KC = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



def hook_data(ti):
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()

    views = hook.get_pandas_df(
        sql = "select * from views_karpov", 
        parameters = {'conn_id':conn}
    )

    likes = hook.get_pandas_df(
        sql = "select * from likes", 
        parameters = {'conn_id':conn}
    )

    ti.xcom_push(
        key='views',
        value=views.to_json()
    )

    ti.xcom_push(
        key='likes',
        value=likes.to_json()
    )

def get_exp_groups(ti):

    df = pd.read_json(
        ti.xcom_pull(
            key='views',
        )
    ).drop(columns='recommendations')
    
    df['date'] = pd.to_datetime(df.timestamp, unit='s')

    tmp = df.groupby('user_id').exp_group.nunique().reset_index()
    duplicate_users = tmp[tmp.exp_group > 1].user_id.values

    df = df[~np.in1d(df.user_id, duplicate_users)]

    control_id = df[df.exp_group=='control']
    print('Control size:', len(control_id))
    test_id = df[df.exp_group=='test']
    print('Test size:', len(test_id))

    print('Value counts:\n', df.user_id.value_counts(normalize=True))
    
    ti.xcom_push(
        key='control_id',
        value=control_id.to_json()
    )

    ti.xcom_push(
        key='test_id',
        value=test_id.to_json()
    )

def get_features(ti):

    print('loading user features')
    user_features = pd.read_sql(
        """SELECT * FROM public.user_data""",
        con=CONN_KC
    )


    # user_features = pd.read_sql(
    #     """SELECT * FROM public.user_data""",
    #     con=CONN_KC
    # )



    print('loading posts features')
    base_model_posts_features = pd.read_sql("""
        SELECT * 
        FROM public.grokhi_base_model_posts_info_features
    """,
        con=CONN_KC
    )

    # enhanced_model_posts_features = pd.read_sql("""
    #     SELECT * 
    #     FROM public.grokhi_enhanced_model_posts_info_features
    # """,
    #     con=CONN_KC
    # )

    print('droppping_columns features')
    base_model_posts_features = base_model_posts_features.drop(
        ['index', 'text', 'topic'], axis=1
    )

    # enhanced_model_posts_features = enhanced_model_posts_features.drop(
    #     ['index', 'text', 'topic', 'KMeansTextCluster',	'DECTextCluster', 'IDECaugTextCluster'], axis=1
    # )

    ti.xcom_push(
        key='user_features',
        value=user_features.to_json()
    )

    ti.xcom_push(
        key='base_model_posts_features',
        value=base_model_posts_features.to_json()
    )

def get_recommendations(
    model,
    user_id:pd.Series, 
    time:pd.Series, 
    user_features:pd.DataFrame, 
    posts_features:pd.DataFrame,
    likes:pd.DataFrame, 
    limit:int = 5
) -> list:

    user_features = user_features[user_features.user_id==user_id].drop('user_id', axis=1)

    add_user_features = dict(zip(user_features.columns, user_features.values[0]))

    user_posts_features = posts_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')

    user_posts_features['hour'] = time.hour
    user_posts_features['month'] = time.month

    predicts = model.predict_proba(user_posts_features)[:, 1]
    user_posts_features['predicts'] = predicts

    # deleting liked posts
    liked_posts = likes[likes.user_id == user_id].post_id.values

    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]
    recommended_posts = filtered_.sort_values('predicts')[-limit:].index

    return recommended_posts.to_list()

def get_recommendations_base_model(ti):

    # print("$AIRFLOW_HOME=", AIRFLOW_HOME)

    base_model = CatBoostClassifier()
    base_model.load_model(
        '/opt/airflow/dags/recommendation_models/catboost_base_model'
    )

    df = pd.read_json(
        ti.xcom_pull(
            key='control_id',
        )
    )[:1000]

    likes = pd.read_json(
        ti.xcom_pull(
            key='likes',
        )
    )

    user_features = pd.read_json(
        ti.xcom_pull(
            key='user_features',
        )
    )

    base_model_posts_features = pd.read_json(
        ti.xcom_pull(
            key='base_model_posts_features',
        )
    )

    print(df.columns)
    print(df.shape)
    print('getting recommendations...')
    df['recommendations'] = df.apply(
        lambda row: get_recommendations(
            base_model,
            row['user_id'],
            row['date'],
            user_features=user_features,
            posts_features=base_model_posts_features,
            likes=likes
        ), axis=1
    )

    ti.xcom_push(
        key='base_model_recommendations',
        value=df.to_json()
    )
                    
def get_recommendations_enhanced_model(ti):

    print("$AIRFLOW_HOME=", AIRFLOW_HOME)

    enhanced_model = CatBoostClassifier()
    enhanced_model.load_model(
        '/opt/airflow/dags/recommendation_models/catboost_enhanced_model'
    )

    print('read')

    df = pd.read_json(
        ti.xcom_pull(
            key='test_id',
        )
    )[:1000]

    print('df')

    likes = pd.read_json(
        ti.xcom_pull(
            key='likes',
        )
    )

    print('likes')

    user_features = pd.read_json(
        ti.xcom_pull(
            key='user_features',
        )
    )

    print('user_features')
    
    enhanced_model_posts_features = pd.read_json(
        ti.xcom_pull(
            key='enhanced_model_posts_features',
        )
    )

    print('enhanced_model_posts_features')

    print(df.columns)
    print(df.shape)
    print('getting recommendations...')
    df['recommendations'] = df.apply(
        lambda row: get_recommendations(
            enhanced_model,
            row['user_id'],
            row['date'],
            user_features=user_features,
            posts_features=enhanced_model_posts_features,
            likes=likes
        ), axis=1
    )

    ti.xcom_push(
        key='enhanced_model_recommendations',
        value=df.to_json()
    )

def get_hitrate(ti):
    print('Getting hitrate!')

    # control_df = pd.read_json(
    #     ti.xcom_pull(
    #         key='base_model_recommendations',
    #     )
    # )
    
    # test_df = pd.read_json(
    #     ti.xcom_pull(
    #         key='enhanced_model_recommendations',
    #     )
    # )

    # df = pd.concat([control_df, test_df])
    # print(df.shape)
    # print(df.sample(10))

with DAG(
    'get_abtest_table', 
    description='Get hitrate score for ', 
    start_date=datetime(2022, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag:

    hook_data = PythonOperator(
        task_id = 'hook_data',
        python_callable=hook_data,
    )

    get_exp_groups = PythonOperator(
        task_id = 'get_exp_groups',
        python_callable=get_exp_groups,
    )

    get_features = PythonOperator(
        task_id = 'get_features',
        python_callable=get_features,
    )

    get_recommendations_base_model  = PythonOperator(
        task_id='get_recommendations_base_model',
        python_callable=get_recommendations_base_model
    )

    get_recommendations_enhanced_model  = PythonOperator(
        task_id='get_recommendations_enhanced_model',
        python_callable=get_recommendations_enhanced_model
    )

    get_hitrate =  PythonOperator(
        task_id='get_hitrate',
        python_callable=get_hitrate
    )

    # push2postgres_db = PostgresOperator(
    #     task_id = "insert_in_table",
    #     postgres_conn_id = "postgres_local",
    #     sql = [f"""
    #         INSERT INTO iris_predict VALUES(
    #         {{ {{ ti.xcom_pull(
    #             key='model_preds', 
    #             task_ids=['get_data_and_predict'])[0][0] 
    #         }} }},
    #         {{ {{ ti.xcom_pull(
    #             key='model_preds', 
    #             task_ids=['get_data_and_predict'])[0][1] 
    #         }} }})
    #     """]
    # )

    # [get_recommendations_base_model, get_recommendations_enhanced_model] 

    hook_data >> get_exp_groups>>get_features\
    >> get_recommendations_base_model\
    >> get_hitrate
    # >> 
    # push2postgres_db

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