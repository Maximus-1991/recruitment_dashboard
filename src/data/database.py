import requests
import time
import mysql.connector
from sqlalchemy import create_engine
import pymysql
import numpy as np
import pandas as pd


from data.candidate_data import get_cand_data
from data.candidate_dataframe import create_df
from data.candidate_dataframe import merge_df
from data.candidate_dataframe import transform_df
from data.candidate_data import retrieve_activities


def df_to_db(df, pw, db_name='candidates', user='root:', host='localhost', port='', schema='recruitment_dashboard'):
    '''
    Function to write DataFrame to MySQL DB
    Inputs:
    df: DataFrame containing all candidate data
    db_name: Name of the to be created MySQL DB (default is 'candidates2')
    user: user name of db (default is 'root')
    pw: password of database
    host = 'localhost' if connecting to local DB
    host = '<IP address of DB>' if connecting to a DB hosted externally
    port: port (default is no port specified). Format is: ':<port>'
    schema: database schema name

    Outputs:
    Populated MySQL Database
    '''
    engine = create_engine('mysql+pymysql://' + user + pw + '@' + host + port + '/' + schema, echo=False)
    df.to_sql(name=db_name, con=engine, if_exists='append', index=False)


def db_to_df(pw, db_name="candidates2", user='root', host='127.0.0.1', port='', database='recruitment_dashboard'):
    '''
    Function to convert MySQL DB to DataFrame
    Inputs:
    db_name: Name of the to be created MySQL DB (default is 'candidates2')
    user: user name of db (default is 'root')
    pw: password of database
    host = '127.0.0.1' if connecting to local DB
    host = '<IP address of DB>' if connecting to a DB hosted externally
    port: port (default is no port specified). Format is: ':<port>'
    database: database schema name

    Outputs:
    Populated MySQL Database
    '''
    conn = mysql.connector.connect(user=user, password=pw, host=host, database=database)
    df = pd.read_sql("SELECT * from " + db_name, conn)
    return df

def create_db(pw, url, headers, key_list, db_name="candidates", user='root', host='127.0.0.1', port='', database='recruitment_dashboard', start_date=''):
    first_api_id = requests.get(url+'candidates'+'.json', headers=headers).json()['candidates'][0]['id']
    # Create empty df_dict
    df_dict={}
    for key in key_list:
        df_dict[key]=[]
    df_dict, since_id, cand_id_list = get_cand_data(df_dict, key_list, url, headers, cand_id_list=[], start_id=first_api_id, start_date=start_date)
    while since_id!=None:
        df_dict, since_id, cand_id_list = get_cand_data(df_dict, key_list, url, headers, cand_id_list=cand_id_list, start_id=since_id, start_date=start_date)
        time.sleep(2.0)
    if cand_id_list!=[]:
        df_dict_cand=retrieve_activities(url,headers,cand_id_list)
        df_cand=create_df(df_dict)
        df_act=create_df(df_dict_cand)
        df_comb=merge_df(df_cand,df_act,how='left', on=['id'])
        df_comb=transform_df(df_comb)
        df_to_db(df_comb,pw,db_name,user+':',host,port,schema=database)

def retrieve_last_db_entry(pw, db_name="candidates", user='root', host='127.0.0.1', port='',
                           database='recruitment_dashboard'):
    '''
    Function to retrieve candidate id of the last entry in the MySQL DB
    Inputs:
    db_name: Name of the to be created MySQL DB (default is 'candidates')
    user: user name of db (default is 'root')
    pw: password of database
    host = '127.0.0.1' if connecting to local DB
    host = '<IP address of DB>' if connecting to a DB hosted externally
    port: port (default is no port specified). Format is: ':<port>'
    database: database schema name

    Outputs:
    last_entry_id: 'id' of last entry in the MySQL DB
    '''
    conn = mysql.connector.connect(user=user, password=pw, host=host, database=database)
    cursor = conn.cursor()
    sql_select_query = """SELECT id FROM %s ORDER BY created_at DESC LIMIT 1""" % (db_name)
    cursor.execute(sql_select_query)
    last_entry_id = cursor.fetchall()
    last_entry_id = last_entry_id[0][0]
    conn.close()
    return last_entry_id

def update_db(pw, url, headers, key_list, db_name="candidates", user='root', host='127.0.0.1', port='', database='recruitment_dashboard', start_date=''):
    last_db_id = retrieve_last_db_entry(pw, db_name, user, host, port, database)
    # Create empty df_dict
    df_dict={}
    for key in key_list:
        df_dict[key]=[]
    df_dict, since_id, cand_id_list = get_cand_data(df_dict, key_list, url, headers, cand_id_list=[], start_id=last_db_id, start_date=start_date)
    while since_id!=None:
        df_dict, since_id, cand_id_list = get_cand_data(df_dict, url, headers, cand_id_list=cand_id_list, start_id=since_id, start_date=start_date)
        time.sleep(2.0)
    #Remove first item in cand_id_list to avoid duplicate of the last_db_id
    if last_db_id in cand_id_list:
        cand_id_list.remove(last_db_id)
    #Remove first item in all the keys in df_dict to avoid duplicate of the last_db_id
        for key in df_dict.keys():
            df_dict[key].pop(0)
    if cand_id_list!=[]:
        df_dict_cand=retrieve_activities(url,headers,cand_id_list)
        df_cand=create_df(df_dict)
        df_act=create_df(df_dict_cand)
        df_comb=merge_df(df_cand,df_act,how='left', on=['id'])
        df_comb=transform_df(df_comb)
        df_to_db(df_comb,pw,db_name,user+':',host,port,schema=database)
        print('done')