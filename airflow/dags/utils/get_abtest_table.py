import pandas as pd
import numpy as np
import hashlib
import os

def get_abtest_table(ti, dataframe):

    df = pd.read_json(dataframe)

    print('Bucketing users...')

    df['bucket'] = df['user_id'].apply(
        lambda x: int(hashlib.md5((str(x) + 'bbb').encode()).hexdigest(), 16) % 100
    )
    df['view'] = 1


    print('Forming abtest table...')

    abtest_df = (
        df
        .drop(columns=['timestamp_x', 'user_id' ])
        .groupby(['exp_group', 'bucket'])
        .sum(['hitrate', 'view'])
        .reset_index()
    )
    abtest_df['hitrate_new'] = abtest_df.hitrate / abtest_df.view

    print(abtest_df.sample(10))

    ti.xcom_push(
        key='abtest_df',
        value=abtest_df.to_json()
    )