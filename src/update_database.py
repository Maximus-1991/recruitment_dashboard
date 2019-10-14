"""
Function to update MySQL Database.
"""
import logging

from .data.candidate_data import KEY_LIST
from .data.workable_db import update_db
from .data.login_details import get_login_details

KEY_LIST = KEY_LIST
DB_USER, DB_PASSWORD, API_AUTH_KEY = get_login_details()

API_HEADERS = {'Authorization': 'Bearer ' + str(API_AUTH_KEY)}
API_URL = 'https://jdriven.workable.com/spi/v3/'

def database_update():
    """
    Function to write DataFrame to MySQL DB
    """

    logging.info('here')

    #update_db(db_password=DB_PASSWORD, api_url=API_URL, api_headers=API_HEADERS, key_list=KEY_LIST,
    #          db_name="candidates2", db_user=DB_USER, host='127.0.0.1', port=3306,
    #          database='recruitment_dashboard', start_date=None)

    update_db(db_password='MjB6KtDfI4pkzKr9', api_url=API_URL, api_headers=API_HEADERS, key_list=KEY_LIST,
              db_name="candidates2", db_user=DB_USER, host='34.90.224.97', port=3306,
              database='recruitment', start_date=None)

    logging.info('success!')
    print("DATABASE UPDATED")
