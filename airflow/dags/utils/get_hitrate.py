import pandas as pd
import numpy as np

from scipy.stats import binomtest
from utils.pipeline_config import cfg_dict

TEST_MODE = cfg_dict['test_mode']
# TEST_MODE = False

def get_hitrate(ti, likes, base_model_recommendations, enhanced_model_recommendations):
    
    likes = pd.read_json(likes)
    control_views = pd.read_json(base_model_recommendations)    
    test_views = pd.read_json(enhanced_model_recommendations)

    print(control_views[:5])

    if TEST_MODE:
        views = pd.read_json(
            ti.xcom_pull(
                key='views',
            )
        ) 
    else:
        views = pd.concat([control_views, test_views])

    
    tmp = (
        views.groupby('user_id')
        .exp_group.nunique().reset_index()
    )

    # deleting duplicated users
    bad_users = tmp[tmp.exp_group > 1].user_id.values

    views = views[~np.in1d(views.user_id, bad_users)]
    likes = likes[~np.in1d(likes.user_id, bad_users)]

    print('Shape:', views.shape)
    print('Columns:', views.columns)

    # test splitting using BinomTest
    vc = views.groupby('user_id').first().exp_group.value_counts()

    print('Value counts:', vc)
    print('Check splitting using BinomTestResult..')

    btest = binomtest(k=vc.iloc[0], n=vc.iloc[0]+vc.iloc[1], p=0.5)
    print(btest)

    if btest.pvalue > .05:
        print(f'Splitting is OK! Null-hypothesis of fair splitting (P=0.5) is not rejected (p-value={btest.pvalue})')
    else:
        print(f'Splitting is bad. Null-hypothesis of fair splitting rejected. Need to check aplitting algorithm! (p-value={btest.pvalue})')

    
    tmp = pd.merge(views, likes, on='user_id', how='outer')
    None if TEST_MODE else tmp.dropna(how='all', inplace=True)

    tmp.post_id = tmp.post_id.fillna(-1).astype(int)

    print('Parsing recommendations...')
    tmp.recommendations = tmp.recommendations.astype(str)

    if TEST_MODE:
        tmp['recommendations'] = tmp.recommendations.apply(
            lambda x: list(map(int, filter(bool, x[1:-1].split(' '))))
        )
    else:
        tmp['recommendations'] = tmp.recommendations.apply(
        lambda x: list(map(int, filter(bool, x[1:-1].split(', '))))
        )

    # tmp['recommendations'] = tmp.recommendations.apply(
    #     lambda x: list(map(int, filter(bool, x[1:-1].split(' '))))
    # )
    
    
    print('Filtering data...')
    tmp.timestamp_x = tmp.timestamp_x.view(int) / 10**9
    tmp.timestamp_y = tmp.timestamp_y.view(int) / 10**9


    # print(tmp.sample(10,random_state=0)[['post_id','recommendations']])
  

    tmp.post_id = tmp.apply(
        lambda row:
        -1
        if
            (row.post_id == -1) | 
            ((row.timestamp_x > row.timestamp_y) |
            (row.timestamp_x + 60 * 60 < row.timestamp_y)) |
            (row.post_id not in row.recommendations)
        else
        row.post_id, axis=1
    )


    print(tmp.sample(10,random_state=0)[['post_id','recommendations']])


    print('Aggregate hitrate values...')

    def agg_hitrate(values):
        values = set(values)
        if -1 in values and len(values) >= 2:
            return 1
        elif -1 not in values:
            return 1
        return 0

    tmp_agg = (
        tmp.groupby(['user_id', 'exp_group', 'timestamp_x'])
        .post_id.agg(agg_hitrate)
    )
    tmp_agg = tmp_agg.reset_index().rename(
        columns={'post_id': 'hitrate'}
    )

    print('Getting Hitrate...')
    hitrate_score = tmp_agg.hitrate.mean()

    print('Average Hitrate is', hitrate_score)
    print(tmp_agg[:5])

    ti.xcom_push(
        key='hitrate_df',
        value=tmp_agg.to_json()
    )