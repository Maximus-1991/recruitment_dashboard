from data.login_details import get_login_details
from data.locate_key import locate_element
import data.candidate_data
from data.candidate_data import last_api_entry, get_cand_data, retrieve_activities
from data.candidate_dataframe import create_df, merge_df, transform_df
from data.database import df_to_db, db_to_df, create_db, update_db, retrieve_last_db_entry

key_list = data.candidate_data.key_list
user, pw, auth_key = get_login_details()

headers = {'Authorization':'Bearer '+str(auth_key)}
url = 'https://jdriven.workable.com/spi/v3/'


update_db(pw=pw, url=url, headers=headers, key_list=key_list, db_name="candidates2", user=user, host='127.0.0.1', port='', database='recruitment_dashboard',start_date='')