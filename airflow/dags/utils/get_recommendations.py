import pandas as pd
from catboost import CatBoostClassifier

from utils.pipeline_config import cfg_dict

TEST_MODE = cfg_dict['test_mode']
LIMIT_RECS = 1000

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



def get_recommendations_model(
    ti, dataframe, likes, user_features, 
    model_posts_features,
    model_path, output_key
):

    model = CatBoostClassifier()
    model.load_model(model_path)

    if TEST_MODE:
        df = pd.read_json(dataframe)[:LIMIT_RECS]
    else:
        df = pd.read_json(dataframe)

    likes = pd.read_json(likes)
    user_features = pd.read_json(user_features)
    model_posts_features = pd.read_json(model_posts_features)

    print('Columns:', df.columns)
    print('Shape:', df.shape)
    print('getting recommendations...')

    df['recommendations'] = df.apply(
        lambda row: get_recommendations(
            model,
            row['user_id'],
            row['timestamp'],
            user_features=user_features,
            posts_features=model_posts_features,
            likes=likes
        ), axis=1
    )

    print(df.sample(10))

    ti.xcom_push(
        key=output_key,
        value=df.to_json()
    )
