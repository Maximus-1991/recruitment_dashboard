"""
Functions to retrieve update Database and request data from Database.
"""

import time
import requests
import mysql.connector
import pymysql
from sqlalchemy import create_engine

import pandas as pd

from .candidate_data import get_cand_data
from .candidate_dataframe import create_df
from .candidate_dataframe import merge_df
from .candidate_dataframe import transform_df
from .candidate_data import retrieve_activities

def df_to_db(df_write_to_db, db_password, db_name='candidates2', db_user='root:', host='localhost', port=3306,
             schema='recruitment_dashboard'):
    '''
    Function to write DataFrame to MySQL DB

    Args:
        df_write_to_db: DataFrame containing all candidate data
        db_password: password of database
        db_name: Name of the to be created MySQL DB (default is 'candidates2')
        db_user: user name of db (default is 'root:')
        host = 'localhost' if connecting to local DB
        host = '<IP address of DB>' if connecting to a DB hosted externally
        port: port (default is no port specified). Type is string, example: '3306'
        schema: database schema name

    Returns:
        Populated MySQL Database with DataFrame df_write_to_db
    '''
    dialect = 'mysql+pymysql://'
    port_format = ':' + str(port)
    engine = create_engine(dialect + db_user + db_password + '@' + host + port_format + '/' + schema, echo=False)
    df_write_to_db.to_sql(name=db_name, con=engine, if_exists='append', index=False)

def db_to_df(db_password, db_name="candidates2", db_user='root', host='127.0.0.1', port=3306,
             database='recruitment_dashboard'):
    '''
    Function to convert MySQL DB to DataFrame
    Args:
        db_name: Name of the to be created MySQL DB (default is 'candidates2')
        db_user: user name of db (default is 'root')
        db_password: password of database
        host = '127.0.0.1' if connecting to local DB
        host = '<IP address of DB>' if connecting to a DB hosted externally
        port: port (default port is 3306). Argument type is integer
            mysql documentation specifies default port of 3306
            https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        database: database schema name

    Returns:
        Populated MySQL Database
    '''

    conn = mysql.connector.connect(user=db_user, password=db_password, host=host,
                                   database=database, port=port, auth_plugin='mysql_native_password')
    df_from_db = pd.read_sql("SELECT * from " + db_name, conn)
    return df_from_db

def create_db(db_password, api_url, api_headers, key_list, db_name="candidates2", db_user='root',
              host='127.0.0.1', port=3306, database='recruitment_dashboard', start_date=None):
    '''
    Function to convert MySQL DB to DataFrame
    Args:
        db_password: password of database
        db_name: Name of the to be created MySQL DB (default is 'candidates')
        db_user: user name of db (default is 'root')
        api_url: url of the Workable API
        api_headers: headers to connect to the Workable API
        key_list: list of Workable API keys to serve as DataFrame column names
        host = '127.0.0.1' if connecting to local DB
        host = '<IP address of DB>' if connecting to a DB hosted externally
        port: port (default port is 3306). Argument type is integer
            mysql documentation specifies default port of 3306
            https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        database: database schema name

    Returns:
        Creates MySQL Database
    '''

    if start_date is None:
        start_date = ''

    section = api_url + 'candidates'
    first_api_id = requests.get(section + '.json', headers=api_headers).json()['candidates'][0]['id']

    # Create empty df_dict
    df_dict = {}
    for key in key_list:
        df_dict[key] = []
    df_dict, since_id, cand_id_list = get_cand_data(df_dict, key_list, api_url,
                                                    api_headers, cand_id_list=None,
                                                    start_id=first_api_id, start_date=start_date)
    while since_id is not None:
        df_dict, since_id, cand_id_list = get_cand_data(df_dict, key_list, api_url,
                                                        api_headers, cand_id_list=cand_id_list,
                                                        start_id=since_id, start_date=None)
        time.sleep(2.0)
    if cand_id_list:
        df_dict_cand = retrieve_activities(api_url, api_headers, cand_id_list)
        df_cand = create_df(df_dict)
        df_act = create_df(df_dict_cand)
        df_comb = merge_df(df_cand, df_act, how='left', merge_on=['id'])
        df_comb = transform_df(df_comb)
        df_to_db(df_comb, db_password, db_name, db_user + ':', host, port, schema=database)

def retrieve_last_db_entry(db_password, db_name="candidates2", db_user='root', host='127.0.0.1', port=3306,
                           database='recruitment_dashboard'):
    '''
    Function to retrieve candidate ID of the last entry in the MySQL DB

    Args:
        db_name: Name of the to be created MySQL DB (default is 'candidates2')
        db_user: user name of db (default is 'root')
        db_password: password of database
        host = '127.0.0.1' if connecting to local DB
            host = '<IP address of DB>' if connecting to a DB hosted externally
        port: port (default is no port specified). Type is integer, example: 3306
            mysql documentation specifies default port of 3306
            https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        database: database schema name

    Returns:
        last_entry_id: 'id' of last entry in the MySQL DB
    '''
    conn = mysql.connector.connect(user=db_user, password=db_password, host=host,
                                   database=database, port=port, auth_plugin='mysql_native_password')
    cursor = conn.cursor()
    #sql_select_query = """SELECT id FROM candidates2 ORDER BY created_at DESC LIMIT 1"""
    sql_select_query = """SELECT id FROM %s ORDER BY created_at DESC LIMIT 1""" % (db_name)
    cursor.execute(sql_select_query)
    last_entry_id = cursor.fetchall()
    last_entry_id = last_entry_id[0][0]
    conn.close()
    return last_entry_id

def update_db(db_password, api_url, api_headers, key_list, db_name="candidates2", db_user='root',
              host='127.0.0.1', port=3306, database='recruitment_dashboard', start_date=None):
    '''
    Function to convert MySQL DB to DataFrame
    Args:
        db_password: password of database
        db_name: Name of the to be created MySQL DB (default is 'candidates2')
        db_user: user name of db (default is 'root')
        api_url: url of the Workable API
        api_headers: headers to connect to the Workable API
        key_list: list of Workable API keys to serve as DataFrame column names
        host = '127.0.0.1' if connecting to local DB
        host = '<IP address of DB>' if connecting to a DB hosted externally
        port: port (default port is 3306). Argument type is integer
            mysql documentation specifies default port of 3306
            https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        database: database schema name

    Returns:
        Updates MySQL Database with latest Workable entries
    '''
    if start_date is None:
        start_date = ''

    last_db_id = retrieve_last_db_entry(db_password=db_password, db_name=db_name, db_user=db_user,
                                        host=host, port=port, database=database)
    # Create empty df_dict
    df_dict = {}
    for key in key_list:
        df_dict[key] = []

    df_dict, since_id, cand_id_list = get_cand_data(df_dict, keys=key_list, api_url=api_url,
                                                    api_headers=api_headers, cand_id_list=[],
                                                    start_id=last_db_id, start_date=start_date)
    while since_id:
        df_dict, since_id, cand_id_list = get_cand_data(df_dict, keys=key_list,
                                                        api_url=api_url, api_headers=api_headers,
                                                        cand_id_list=cand_id_list,
                                                        start_id=since_id, start_date=start_date)
        time.sleep(2.0)
    #Remove first item in cand_id_list to avoid duplicate of the last_db_id
    if last_db_id in cand_id_list:
        cand_id_list.remove(last_db_id)
    #Remove first item in all the keys in df_dict to avoid duplicate of the last_db_id
        for key in df_dict.keys():
            df_dict[key].pop(0)
    if cand_id_list:
        df_dict_cand = retrieve_activities(api_url, api_headers, cand_id_list)
        df_cand = create_df(df_dict)
        df_act = create_df(df_dict_cand)
        df_comb = merge_df(df_cand, df_act, how='left', merge_on='id')
        df_comb = transform_df(df_comb)
        df_to_db(df_write_to_db=df_comb, db_password=db_password, db_name=db_name, db_user=db_user + ':',
                 host=host, port=port, schema=database)
