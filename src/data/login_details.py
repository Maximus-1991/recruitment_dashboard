import os
from dotenv import load_dotenv, find_dotenv

def get_login_details():
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)

    user = os.environ.get("DB_USER")
    pw = os.environ.get("DB_PW")
    auth_key = os.environ.get("AUTH_HEADER")

    return user, pw, auth_key