import functools
import os

import snowflake.connector
import yaml


def sf_conn():
    search_config = load_config()
    ctx = snowflake.connector.connect(user=search_config['snowflake_user'],
                                      password=search_config['snowflake_pass'],
                                      account='vwa67686.us-east-1',
                                      warehouse='COMPUTE_WH',
                                      database='NUBELA_TRANSFORMATION',
                                      schema='SYSADMIN')
    ctx.cursor().execute("USE ROLE SYSADMIN")
    cur = ctx.cursor()
    cur.execute("USE DATABASE XVERIUM")  # MYS3STAGE have the permition to read from  sagemaker-2710/data
    return cur


@functools.lru_cache()
def load_config():
    with open(os.path.join(os.path.dirname(__file__), "mycredentials.yaml")) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def export_data_from_sf(query, condition=False):
    full_query = query.format(*condition) if isinstance(condition, tuple) else query.format(condition)
    cur = sf_conn()
    cur.execute(full_query)
    return cur.fetch_pandas_all()


def list_split(n, lst):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def embed_list_in_query(lst, param):
    lst = [*set(x.replace("'", '\\\'') for x in lst if isinstance(x, str) and x != '')]
    lst_split = list_split(16000, lst)
    query_format = ""
    if len(lst_split) > 0:
        for i in range(0, len(lst_split)):
            query_format += f"{param} IN ('" + "', '".join(lst_split[i]) + "') OR "
        return f'({query_format[:-4]})'
    return f"{param} IN ('')"
