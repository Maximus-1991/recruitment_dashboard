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
    Function to retrieve the last candidate id of last entry in Workable

    Args:
        url: url of the Workable API
        headers: headers to connect to the API

    Returns:
        candidate id of the last entry in Workable
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


def get_cand_data(df_dict, keys, url, headers, cand_id_list=None, start_id=None, start_date=None):
    '''
    Function to retrieve candidate data and store in dictionary
    Every Workable page contains 100 candidates, as specified by limit

    Args:
        df_dict: dictionary containing candidate data
        keys: all the required keys from the .json() that need to be put in DataFrame
        url: Workable API url
        headers: API Headers (contains Authorization Headers)
        cand_id_list: list of candidate IDs (Default is None)
        start_id: returns results with a candidate ID greater than or equal to the specified ID
        start_date: API request returns results created after the specified timestamp

    Returns:
        DataFrame containing the same candidate data as the input
        since_id: specifies the first candidate ID of the next page in Workable
        cand_id_list: list of candidate IDs that are retrieved from the Workable page
    '''
    if cand_id_list is None:
        cand_id_list = []

    if start_date is not None:
        start_after = '&created_after=' + start_date
    else:
        start_after = ''

    if start_id is not None:
        start_id = '&since_id=' + start_id
    else:
        start_id = ''

    section = url+'candidates?'
    limit = 'limit=100'
    request = requests.get(section + limit + start_after + start_id + '.json', headers=headers)
    for cand in request.json()['candidates']:
        cand_id_list.append(cand['id'])
        for key in keys:
            loc = locate_element(cand, key)
            value = cand
            for idx in loc:
                value = value[idx]
            df_dict[key].append(value)
    try:
        since_id = request.json()['paging']['next'].split("since_id=", 1)[1]
        return df_dict, since_id, cand_id_list
    except KeyError:
        since_id = None
        return df_dict, since_id, cand_id_list


def retrieve_activities(url, headers, cand_id_list):
    '''
    Function to retrieve for a each candidate his/her activity data and store in dictionary

    Args:
        url: Workable API url
        headers: API Headers (contains Authorization Headers)
        cand_id_list: list of candidate IDs

    Returns:
        df_dict_cand: dictionary containing candidate data
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
            if act['action'] == 'disqualified' and not disqualified:
                df_dict_cand['disqualified_at'].append(act['created_at'])
                disqualified = True
            if act['stage_name'] in stage_name_list:
                if act['stage_name'] not in stages:
                    continue
                else:
                    df_dict_cand[act['stage_name']].append(act['created_at'])
                    stages.remove(act['stage_name'])
        if not disqualified:
            df_dict_cand['disqualified_at'].append(np.nan)
        for remaining_stage in stages:
            df_dict_cand[remaining_stage].append(np.nan)
        time.sleep(0.5)
    return df_dict_cand
