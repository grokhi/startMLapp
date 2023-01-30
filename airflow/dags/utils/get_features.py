import pandas as pd

def get_features(ti, external_connection):

    print('loading user features')
    user_features = pd.read_sql(
        """SELECT * FROM public.user_data""",
        con=external_connection
    )


    print('loading posts features')
    base_model_posts_features = pd.read_sql("""
        SELECT * 
        FROM public.grokhi_base_model_posts_info_features
    """,
        con=external_connection
    )

    enhanced_model_posts_features = pd.read_sql("""
        SELECT * 
        FROM public.grokhi_enhanced_model_posts_info_features
    """,
        con=external_connection
    )

    print('droppping_columns features')
    base_model_posts_features = base_model_posts_features.drop(
        ['index', 'text', 'topic'], axis=1
    )

    enhanced_model_posts_features = enhanced_model_posts_features.drop(
        ['index', 'text', 'topic', 'KMeansTextCluster',	'DECTextCluster', 'IDECaugTextCluster'], axis=1
    )

    ti.xcom_push(
        key='user_features',
        value=user_features.to_json()
    )

    ti.xcom_push(
        key='base_model_posts_features',
        value=base_model_posts_features.to_json()
    )

    ti.xcom_push(
        key='enhanced_model_posts_features',
        value=enhanced_model_posts_features.to_json()
    )