import urllib.request
import os.path
import json
import requests
from tqdm import tqdm
import gzip
from data_manager._data_definitions import *
from data_manager._config import *
import datetime as dt
import pandas as pd

def get_p114_filenames_for_date(p114_date):
    """
    Returns a list of p114 filenames generated on a specific date

    Parameters
    ----------
    p114_date : date
        the date for which filenames are to be retrieved

    Returns
    -------
    list
        list of strings containing filenames

    Raises
    ------


    """

    response = requests.get(P114_LIST_URL.format(ELEXON_KEY,
                                                 dt.datetime.strftime(p114_date, "%Y-%m-%d")))
    json_data = json.loads(response.text)

    if len(json_data)>0:
        unrecognised_feeds = [x for x in json_data if x.split('_')[0] not in PROCESSED_FEEDS+IGNORED_FEEDS]
        if len(unrecognised_feeds)>0:
            raise ValueError('Feed type not recognised for files: '+unrecognised_feeds)
        files_to_be_processed = [x for x in json_data if x.split('_')[0] in PROCESSED_FEEDS]
        if len(files_to_be_processed)>0:
            return files_to_be_processed
    return None

def get_p114_file(filename, overwrite=True):
    """
    downloads specified P114 file

    Parameters
    ----------
    filename : string
        the filename to be retrieved
    overwrite : boolean
        if the file already exists, whether to overwrite or keep

    Returns
    -------

    Raises
    ------

    """
    #print(filename)
    if not os.path.isfile(P114_INPUT_DIR + filename) or overwrite:
        remote_url = (P114_DOWNLOAD_URL.format(ELEXON_KEY, filename))
        urllib.request.urlretrieve(remote_url,
                                   P114_INPUT_DIR + filename)


def file_to_message_list(filename):
    """
    Converts locally saved file to list of message dictionaries
    Filters for accepted message types and raises errors for
    unrecognised message types

    Parameters
    ----------
    filename : string
        the filename to be Processed

    Returns
    -------
    message_list: list
        a list of message dictionaries containing key/value pairs
    """

    p114_feed = filename.split('_')[0]
    if p114_feed not in ACCEPTED_MESSAGES and p114_feed not in IGNORED_MESSAGES:
        raise ValueError('P114 item {} not recognised'.format(p114_feed))
    file = gzip.open(P114_INPUT_DIR + filename, 'rb')
    file_content = file.read().decode('utf-8', 'ignore')
    message_list = []
    for row in file_content.split('\n'):
        if len(row)>0:
            message_values = row.split('|')
            message_type = message_values[0]
            if message_type in ACCEPTED_MESSAGES[p114_feed]:
                message_keys = FIELDNAMES[message_type]
                casted_message_values = [FIELD_CASTING_FUNCS[key](value.strip()) for
                                         key, value in zip(message_keys,
                                                           message_values[1:])]
                message_list.append(dict(zip(['message_type']+message_keys, [message_type]+casted_message_values)))
            elif message_type not in IGNORED_MESSAGES[p114_feed]:
                print(row)
                raise ValueError('message type {} not recognised'.format(message_type))
    return message_list


def insert_data(message_list, p114_date):
    """
    Generates and saves Django ORM object from message dictionary
    in order to insert a row of BMUID-level data to DB

    Parameters
    ----------
    message: message dictionary

    Returns
    -------
    None

    Raises
    ------

    """
    # here we rely on message order to be correct in the input files
    # e.g. as ABP datapoints come immediately after the ABV datapoint
    # with which they are associated, we link ABPs to the most recent
    # created ABV object in the loop
    # TODO: add integrity checks e.g. that numbers of and links between
    # each object in a processed file are consistent with this assumption
    df_all = pd.DataFrame({})
    df_gsp = pd.DataFrame({})

    message_list = list(filter(lambda x: x['message_type'] in ['MPD', 'GP9', 'GMP'], message_list))


    if len(message_list) > 0:
        MPD = message_list[0]

        message_list = message_list[1:]

        idx_list = [idx for idx, x in enumerate(message_list) if x['message_type']=='GP9']

        message_list_list = [message_list[idx:idx_list[_id + 1]] if _id < len(idx_list) - 1
                                                                     else message_list[idx:] for _id, idx
                                                                        in enumerate(idx_list)]
        df_MPD = pd.concat([pd.DataFrame(GSP[1:]).assign(gsp_id=GSP[0]['gsp_id']) for GSP in message_list_list])

        df_MPD = df_MPD.drop(columns=['message_type'])

        df_MPD['date'] = p114_date.date()
        df_MPD['gsp_group'] = MPD['gsp_group']
        df_MPD['sr_type'] = MPD['sr_type']
        df_MPD['run_no'] = MPD['run_no']

        df_MPD.to_csv(P114_INPUT_DIR+"/gsp_demand.csv", mode='a+')

def process_p114_date(p114_date):
    """
    Retrieves data for nominated day and processes it

    Parameters
    ----------
    p114_date : date
        the date for which filenames are to be retrieved

    Returns
    -------

    Raises
    ------
    """
    filenames = get_p114_filenames_for_date(p114_date)
    if filenames is not None:
        print('{} relevant files found'.format(len(filenames)))
        for filename in tqdm(filenames):
            get_p114_file(filename, overwrite=True)
            insert_data(file_to_message_list(filename), p114_date)
            os.remove(P114_INPUT_DIR + filename)
    else:
        print('No relevant files found')

def run(*args, **options):
    """downloads P114 data for specific date range,
    expected 2 arguments of form ['yyyy-mm-dd', 'yyyy-mm-dd']"""
    if os.path.isfile(P114_INPUT_DIR+"/gsp_demand.csv"):
        os.remove(P114_INPUT_DIR+"/gsp_demand.csv")
    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])
    date = start_date
    t0 = dt.datetime.now()
    completed_requests = 0
    while date <= end_date:
        if ((dt.datetime.now() - t0).seconds / 60) - (request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))
            process_p114_date(date)
            date += dt.timedelta(days=1)
            completed_requests += 1