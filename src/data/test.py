import candidate_data
from candidate_data import last_api_entry
from login_details import get_login_details

key_list = candidate_data.key_list
user, pw, auth_key = get_login_details()

headers = {'Authorization':'Bearer '+str(auth_key)}
url = 'https://jdriven.workable.com/spi/v3/'

last_id = last_api_entry(url, headers = headers)
print(last_id)