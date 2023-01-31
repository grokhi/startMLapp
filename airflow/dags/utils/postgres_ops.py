import pandas as pd
import numpy as np

from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetchfrompostgres(ti, connection):
    hook = PostgresHook(postgres_conn_id=connection)
    conn = hook.get_conn()

    views = hook.get_pandas_df(
        sql = "select * from views", 
        parameters = {'conn_id':conn}
    )
    views.timestamp = pd.to_datetime(views.timestamp, unit='s')


    likes = hook.get_pandas_df(
        sql = "select * from likes", 
        parameters = {'conn_id':conn}
    )
    likes.timestamp = pd.to_datetime(likes.timestamp, unit='s')

    ti.xcom_push(
        key='views',
        value=views.to_json()
    )

    print(views.sample(10))

    ti.xcom_push(
        key='likes',
        value=likes.to_json()
    )

    print(likes.sample(10))


def upload2postgres(ti, tablename, connection, dataframe):

    hook = PostgresHook(postgres_conn_id=connection)

    df = pd.read_json(dataframe)

    rows = df.values.tolist()
    fields = df.columns.tolist()

    print(df.head())

    hook.insert_rows(
        table=tablename,
        rows=rows,
        target_fields=fields
    )

    print('Done!')
