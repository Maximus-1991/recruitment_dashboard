"""
Function to retrieve Workable API login details
"""

import os
from dotenv import load_dotenv, find_dotenv

def get_login_details():
    '''
    Function to retrieve API login details from .env file

    Args:
        None

    Returns:
        db_user: database username
        db_password: database password
        api_auth_key: API Authentication key to be used in API header
    '''
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)

    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PW")
    api_auth_key = os.environ.get("AUTH_HEADER")

    return db_user, db_password, api_auth_key
