import pandas as pd
from catboost import CatBoostClassifier

from utils.pipeline_config import cfgdct

TEST_MODE = cfgdct['test_mode']
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

def get_recommendations_base_model(ti, dataframe, likes, user_features, base_model_posts_features):

    # print("$AIRFLOW_HOME=", AIRFLOW_HOME)

    base_model = CatBoostClassifier()
    base_model.load_model(
        '/opt/airflow/dags/recommendation_models/catboost_base_model'
    )

    if TEST_MODE:
        df = pd.read_json(dataframe)[:LIMIT_RECS].drop(columns=['recommendations'])       
    else:
        df = pd.read_json(dataframe).drop(columns=['recommendations'])

    likes = pd.read_json(likes)
    user_features = pd.read_json(user_features)
    base_model_posts_features = pd.read_json(base_model_posts_features)

    print('Columns:', df.columns)
    print('Shape:', df.shape)
    print('getting recommendations...')

    df['recommendations'] = df.apply(
        lambda row: get_recommendations(
            base_model,
            row['user_id'],
            row['timestamp'],
            user_features=user_features,
            posts_features=base_model_posts_features,
            likes=likes
        ), axis=1
    )

    print(df.sample(10))

    ti.xcom_push(
        key='base_model_recommendations',
        value=df.to_json()
    )

def get_recommendations_enhanced_model(ti, dataframe, likes, user_features, enhanced_model_posts_features):

    # print("$AIRFLOW_HOME=", AIRFLOW_HOME)

    enhanced_model = CatBoostClassifier()
    enhanced_model.load_model(
        '/opt/airflow/dags/recommendation_models/catboost_enhanced_model'
    )

    if TEST_MODE:
        df = pd.read_json(dataframe)[:LIMIT_RECS].drop(columns=['recommendations'])       
    else:
        df = pd.read_json(dataframe).drop(columns=['recommendations'])

    likes = pd.read_json(likes)
    user_features = pd.read_json(user_features)
    enhanced_model_posts_features = pd.read_json(enhanced_model_posts_features)

    print('Columns:', df.columns)
    print('Shape:', df.shape)
    print('getting recommendations...')

    df['recommendations'] = df.apply(
        lambda row: get_recommendations(
            enhanced_model,
            row['user_id'],
            row['timestamp'],
            user_features=user_features,
            posts_features=enhanced_model_posts_features,
            likes=likes
        ), axis=1
    )

    print(df.sample(10))

    ti.xcom_push(
        key='enhanced_model_recommendations',
        value=df.to_json()
    )
