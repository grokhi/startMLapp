import pandas as pd
import numpy as np
import hashlib
import json

from utils.pipeline_config import cfg_dict
TEST_MODE = cfg_dict['test_mode']
# TEST_MODE = False


def get_group(user:str, salt:str, group_count:int):
    value_str = user + salt
    value_num = int(hashlib.md5(value_str.encode()).hexdigest(), 16)
    return value_num % group_count

def get_exp_groups(ti, salt, dataframe):

    df = pd.read_json(dataframe).drop(columns=['recommendations'])

    print(df.sample(10))

    if TEST_MODE:
        control_id = df[df.exp_group=='control']
        test_id = df[df.exp_group=='test']
    
    else: 
        # generating exp_group with salt
        df.user_id = df.user_id.astype(str)

        df['exp_group'] = df.user_id.apply(
            lambda user: get_group(user, salt, 2)
        )
        df['exp_group'] = df['exp_group'].map({ 0: 'control', 1 : 'test'})

        control_id = df[df.exp_group=='control']
        test_id = df[df.exp_group=='test']

    print('Control size:', len(control_id))
    print('Test size:', len(test_id))

    
    ti.xcom_push(
        key='control_id',
        value=control_id.to_json()
    )

    ti.xcom_push(
        key='test_id',
        value=test_id.to_json()
    )