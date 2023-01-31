cfg_dict = {
    'external_conn' : "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml",
    'local_conn' : "postgres_localhost",
    'tablename' : 'ab_test_{{ ds_nodash }}',
    'salt' : 'abtest_1',
    'test_mode' : True,
}