"""
Functions to retrieve candidate data from Workable through Workable API.
"""

import datetime
import time
from copy import deepcopy

import numpy as np
import requests

from .locate_key import locate_element

KEY_LIST = [
    'id',
    'name',
    'firstname',
    'lastname',
    'headline',
    'subdomain',
    'shortcode',
    'title',
    'stage',
    'disqualified',
    'disqualification_reason',
    'hired_at',
    'sourced',
    'profile_url',
    'address',
    'phone',
    'email',
    'domain',
    'created_at',
    'updated_at',
]

def last_api_entry(url, headers):
    '''
    Function to retrieve the last entry in Workable through API

    Inputs:
    url: url of the Workable API
    headers: headers to connect to the API

    Outputs:
    'id' of the last entry
    '''
    section = 'candidates?'
    url_section = url + section
    limit = 'limit=100'
    api_date = datetime.datetime.today().isoformat()
    created_after = '&created_after=' + api_date
    r_last_entry = requests.get(url_section + limit + created_after + '.json', headers=headers)
    while r_last_entry.json()['candidates']:
        api_date = (api_date - datetime.timedelta(days=1))
        r_last_entry = requests.get(url_section + limit + created_after + '.json', headers=headers)
        time.sleep(0.9)
    last_id = r_last_entry.json()['candidates'][-1]['id']
    return last_id


def get_cand_data(df_dict, key_list, url, headers, cand_id_list=None, start_id='', start_date=''):
    if cand_id_list is None:
            cand_id_list = []
    if start_date != '':
        start_after = '&created_after=' + start_date
    else:
        start_after = ''

    if start_id != '':
        start_id = '&since_id=' + start_id
    else:
        start_id = ''
    section = 'candidates?'
    url_section = url + section
    limit = 'limit=100'
    request = requests.get(url_section + limit + start_after + start_id + '.json', headers=headers)
    for cand in request.json()['candidates']:
        cand_id_list.append(cand['id'])
        for k in key_list:
            loc = locate_element(cand, k)
            value = cand
            for i in loc:
                value = value[i]
            df_dict[k].append(value)
    try:
        since_id = request.json()['paging']['next'].split("since_id=", 1)[1]
        return df_dict, since_id, cand_id_list
    except:
        since_id = None
        return df_dict, since_id, cand_id_list


def retrieve_activities(url, headers, cand_id_list):
    '''
    Function to create candidate activity dictionary
    Inputs:

    df_dict: dictionary containing candidate data
    Outputs:

    DataFrame containing the same candidate data as the input
    '''
    # Create DataFrame column labels
    df_dict_cand = {}
    key_list_cand = ['id', 'tags']
    stage_name_list = [
        'Sourced',
        'Applied',
        'Shortlisted',
        'Talentpool',
        'Review',
        'To schedule',
        'Inplannen 1e gesorek',  # not in use anymore --> combine with 'To Schedule' --> delete
        'Inplannen 1e gesprek',  # not in use anymore --> combine with 'To Schedule' --> delete
        'inplannen 2e gesprek',  # not in use anymore --> combine with '1st Interview' --> delete
        '1st Interview',
        '1e gesprek',  # not in use anymore --> combine with '1st Interview' --> delete
        'Interview 1',  # not in use anymore --> combine with '1st Interview' --> delete
        '2nd Interview',
        'Interview 2',  # not in use anymore --> combine with '2nd Interview' --> delete
        'Assessment',  # not in use anymore --> combine with '2nd Interview' --> delete
        '2e gesprek',  # not in use anymore --> combine with '2nd Interview' --> delete
        'Offer',
        'Aanbieding',  # not in use anymore --> combine with 'Offer' --> delete
        'Hired',
        'Aangenomen',  # not in use anymore --> combine with 'Hired' --> delete
        'Test Fase',  # not in use anymore --> delete
        'intern evalueren',  # not in use anymore --> delete
        'Plan 1',  # not in use anymore --> delete
        'Plan 2',  # not in use anymore --> delete
        'Vergaarbak'  # not in use anymore --> delete
    ]

    # Add labels to dictionary
    for key in key_list_cand:
        df_dict_cand[key] = []
    for key in stage_name_list:
        df_dict_cand[key] = []
    df_dict_cand['disqualified_at'] = []

    # Retrieve data through API
    section = 'candidates/'
    url_section = url + section
    act = '/activities'

    for cand_id in cand_id_list:
        r_cand_id = requests.get(url_section + cand_id + '.json', headers=headers)
        time.sleep(1.0)
        for k in key_list_cand:
            loc = locate_element(r_cand_id.json()['candidate'], k)
            value = r_cand_id.json()['candidate']
            for idx in loc:
                value = value[idx]
            df_dict_cand[k].append(value)

        # loop through activities for candidate cand_id
        r_cand_id_act = requests.get(url_section + cand_id + act + '.json', headers=headers)
        r_cand_id_act = r_cand_id_act.json()['activities']
        time.sleep(1.0)
        stages = deepcopy(stage_name_list)
        disqualified = False
        for act in r_cand_id_act:
            if act['action'] == 'disqualified' and disqualified == False:
                df_dict_cand['disqualified_at'].append(act['created_at'])
                disqualified = True
            if act['stage_name'] in stage_name_list:
                if act['stage_name'] not in stages:
                    continue
                else:
                    df_dict_cand[act['stage_name']].append(act['created_at'])
                    stages.remove(act['stage_name'])
        if disqualified == False:
            df_dict_cand['disqualified_at'].append(np.nan)
        for remaining_stage in stages:
            df_dict_cand[remaining_stage].append(np.nan)
        time.sleep(0.5)
    return df_dict_cand
