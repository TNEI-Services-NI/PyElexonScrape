# -*- coding: utf-8 -*-
"""
- Author: 
- Position: 
- Company: TNEI Services, Glasgow
- E-mail: 
- IDE: PyCharm
- Project Name: gb-data-pull
- Date: 02/08/2021
- Time: 09:36
"""
import datetime as dt
import json
import pandas as pd
import multiprocessing as mp
import os.path
import urllib.request

import pandas as pd
import requests
from tqdm import tqdm

import data_manager._config as cf
import data_manager.P114.util as p114_util
from data_manager._data_definitions import *


def pull_p114_date_files(p114_date: dt.date) -> None:
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
            get_p114_file(filename, overwrite=False)
            p114_util.insert_data(p114_util.file_to_message_list(filename), filename)
            os.remove(cf.P114_INPUT_DIR + filename)
    else:
        print('No relevant files found')


def pull_p114_date_files_parallel(dates, q: mp.Queue, t0) -> None:
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
    # filenames = get_p114_filenames_for_date(p114_date)
    completed_requests = 0
    if dates is not None:
        print('{} relevant files found'.format(len(dates)))
        for date in dates.iterrows():
            if not os.path.exists(cf.P114_INPUT_DIR + date[1]['file'][0]):
                get_p114_file(date[1]['file'][0], overwrite=False)
            if type(q) == list:
                continue
            else:
                q.put({'filename': date[1]['file'][0], 'p114_date': date[1]['date']})
    else:
        print('No relevant files found')


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



    if len(json_data) > 0:
        unrecognised_feeds = [x for x in json_data if x.split('_')[0] not in PROCESSED_FEEDS + IGNORED_FEEDS]
        if len(unrecognised_feeds) > 0:
            raise ValueError('Feed type not recognised for files: ' + unrecognised_feeds)
        files_to_be_processed = [x for x in json_data if x.split('_')[0] in PROCESSED_FEEDS]
        if len(files_to_be_processed) > 0:
            return files_to_be_processed
    return None


def get_p114_file(filename, overwrite=False):
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
    # print(filename)
    if not os.path.isfile(cf.P114_INPUT_DIR + filename) or overwrite:
        remote_url = (cf.P114_DOWNLOAD_URL.format(cf.ELEXON_KEY, filename))
        urllib.request.urlretrieve(remote_url,
                                   cf.P114_INPUT_DIR + filename)


def get_dates(from_='30-04-2010', to_='31-12-2020'):
    all_dates = []
    for p114_date_ in pd.date_range(from_, to_):
        print(p114_date_)
        response = requests.get(cf.P114_LIST_URL.format(cf.ELEXON_KEY,
                                                     dt.datetime.strftime(p114_date_, "%Y-%m-%d")))
        json_data = json.loads(response.text)

        if len(json_data) == 0:
            continue

        dates = [(
            dt.datetime.strptime(d[0], '%Y%m%d').date(),
            dt.datetime.strptime(d[1], '%Y%m%d%H%M%S').date(),
            d[2],
            d[3],
            d[4],
        )
                 for d in [(
                k.split('_')[1],
                k.split('_')[-1].strip('.gz'),
                (k,v),
                k.split('_')[0],
                k.split('_')[3],
            )
                     for k, v in json_data.items()]]

        dates = pd.DataFrame({
            'date': [d[0] for d in dates],
            'settlement': [d[1] for d in dates],
            'file': [d[2] for d in dates],
            'message': [d[3] for d in dates],
            'group': [d[4] for d in dates],
        })
        all_dates.append(dates)
    return pd.concat(all_dates)


def pull_data_parallel(dates, t0, q, status_q):
    print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
    pull_p114_date_files_parallel(dates, q, t0)


def pull_data(date, end_date, t0):
    completed_requests = 0
    while date <= end_date:
        if ((dt.datetime.now() - t0).seconds / 60) - (cf.request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))
            pull_p114_date_files(date)
            date += dt.timedelta(days=1)
            completed_requests += 1
