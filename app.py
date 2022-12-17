import os
from re import I
import pandas as pd
from typing import List
from catboost import CatBoostClassifier
from fastapi import FastAPI
from schema import PostGet, Response 
from datetime import datetime
from sqlalchemy import create_engine
from loguru import logger
import hashlib


# constants
CONN = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"\
        "postgres.lab.karpov.courses:6432/startml"
SALT = 'my_salt'

AB_TESTING = True

DEFAULT_MODEL_PATH = 'recommendation_models/catboost_enhanced_model'
DEFAULT_MODEL_FEATURES_TABLE_NAME = 'grokhi_enhanced_model_posts_info_features'

if AB_TESTING:
    CONTROL_MODEL_PATH = 'recommendation_models/catboost_base_model'
    TEST_MODEL_PATH = DEFAULT_MODEL_PATH

    CONTROL_MODEL_FEATURES_TABLE_NAME =  'grokhi_base_model_posts_info_features',
    TEST_MODEL_FEATURES_TABLE_NAME =   DEFAULT_MODEL_FEATURES_TABLE_NAME

app = FastAPI()

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    
    engine = create_engine(CONN)
    conn = engine.connect().execution_options(stream_results=True)
    
    chunks = []
    
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        if path == CONTROL_MODEL_PATH:
            path = '/workdir/user_input/model_control'
        elif path == TEST_MODEL_PATH:
            path = '/workdir/user_input/model_test'
    return path


def get_exp_group(id: int) -> str:
    value_str = str(id) + SALT
    value_num = int(hashlib.md5(value_str.encode()).hexdigest(), 16)
    percent = value_num % 100
    if percent < 50:
        return "control"
    elif percent < 100:
        return "test"
    return "unknown"


def load_features(ab_testing:bool=False) -> pd.DataFrame:
    logger.info('loading liked posts')
    liked_posts_query = '''
        SELECT distinct post_id, user_id
        FROM public.feed_data
        WHERE action = 'like'
        LIMIT 200000
        '''
    liked_posts = batch_load_sql(liked_posts_query)

    logger.info('loading user features')
    user_features = pd.read_sql('''
        SELECT *
        FROM public.user_data ''',
        con=CONN
    )

    if not ab_testing:
        logger.info('loading posts features')
        posts_features = pd.read_sql(f'''
            SELECT *
            FROM public.{DEFAULT_MODEL_FEATURES_TABLE_NAME} ''',
            con=CONN
        )
        return [liked_posts, user_features, posts_features]

    else:
        logger.info('loading posts features for model_control')
        posts_features_model_control = pd.read_sql(f'''
            SELECT *
            FROM public.grokhi_base_model_posts_info_features''', #CONTROL_MODEL_FEATURES_TABLE_NAME
            con=CONN
        )
        logger.info('loading posts features for model_test')
        posts_features_model_test = pd.read_sql(f'''
            SELECT *
            FROM public.{TEST_MODEL_FEATURES_TABLE_NAME}''',
            con=CONN
        )
        return [liked_posts, user_features, posts_features_model_control, posts_features_model_test]


def load_models(ab_testing:bool=False):
    '''
    load models() -> model
    load models() -> (model_control, model_test) if ab_testing==True
    '''
    if ab_testing:
        loaded_model_control = CatBoostClassifier()
        control_model_path = get_model_path(CONTROL_MODEL_PATH)

        loaded_model_test = CatBoostClassifier()
        test_model_path = get_model_path(TEST_MODEL_PATH)

        return (
            loaded_model_control.load_model(control_model_path), 
            loaded_model_test.load_model(test_model_path)
        )
    else:
        loaded_model = CatBoostClassifier()
        model_path = get_model_path(DEFAULT_MODEL_PATH)

        return loaded_model.load_model(model_path)


logger.info('loading model')
model_s = load_models(AB_TESTING)

logger.info('loading_features')
features = load_features(AB_TESTING)

logger.info('service is up and running')

def get_recommended_feed(id: int, time: datetime, limit: int):
    logger.info(f'user_id: {id}')
    logger.info('reading_features')
    user_features = features[1].loc[features[1].user_id == id]
    user_features = user_features.drop('user_id', axis=1)

    logger.info('droppping_columns')

    if not AB_TESTING:
        posts_features = features[2].drop(['index', 'text', 'topic'], axis=1)
        content = features[2][['post_id', 'text', 'topic' ]]
    else:
        exp_group = get_exp_group(id)
        if exp_group == 'control':
            posts_features = features[2].drop(
                ['index', 'text', 'topic'], axis=1
            )
            content = features[2][['post_id', 'text', 'topic' ]]
        elif exp_group == 'test':
            posts_features = features[3].drop(
                ['index', 'text', 'topic', 'KMeansTextCluster',	'DECTextCluster', 'IDECaugTextCluster'], axis=1
            )
            content = features[3][['post_id', 'text', 'topic' ]]
        else:
            raise ValueError('unknown group')

    logger.info('zipping everything')
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info('assigning everything')
    user_posts_features = posts_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')

    logger.info('add time info')
    user_posts_features['hour'] = time.hour
    user_posts_features['month'] = time.month

    if not AB_TESTING:
        model = model_s[0]
        predicts = model.predict_proba(user_posts_features)[:, 1]
    else:
        logger.info(f'predicting for user_id={id} from {exp_group} group')
        if exp_group == 'control':
            predicts = model_s[0].predict_proba(user_posts_features)[:, 1]
        elif exp_group == 'test':
            predicts = model_s[1].predict_proba(user_posts_features)[:, 1]

    user_posts_features['predicts'] = predicts

    logger.info('deleting liked posts')
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]

    recommended_posts = filtered_.sort_values('predicts')[-limit:].index

    return Response(**{
        'exp_group': get_exp_group(id),
        'recommendations': [
            PostGet(**{
                'id': i,
                'text': content[content.post_id == i].text.values[0],
                'topic': content[content.post_id == i].topic.values[0]
            }) for i in recommended_posts
        ]
    })
    
@app.get("/post/recommendations/", response_model=Response)
def get_recommendations(id: int, time: datetime = datetime.now(), limit: int = 5) -> Response:
    return get_recommended_feed(id, time, limit)
