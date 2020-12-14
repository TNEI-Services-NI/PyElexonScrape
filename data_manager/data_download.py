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
from functools import reduce
import multiprocessing as mp
import time
import numpy as np
import shutil


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

    if len(json_data) > 0:
        unrecognised_feeds = [x for x in json_data if x.split('_')[0] not in PROCESSED_FEEDS + IGNORED_FEEDS]
        if len(unrecognised_feeds) > 0:
            raise ValueError('Feed type not recognised for files: ' + unrecognised_feeds)
        files_to_be_processed = [x for x in json_data if x.split('_')[0] in PROCESSED_FEEDS]
        if len(files_to_be_processed) > 0:
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
    # print(filename)
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

    target_present = reduce(lambda l, r: l and r, [id_ in file_content for id_ in TARGET_MESSAGES])

    if target_present:

        message_list = []
        for row in file_content.split('\n'):
            if len(row) > 0:
                message_values = row.split('|')
                message_type = message_values[0]
                if message_type in ACCEPTED_MESSAGES[p114_feed]:
                    message_keys = FIELDNAMES[message_type]
                    casted_message_values = [FIELD_CASTING_FUNCS[key](value.strip()) for
                                             key, value in zip(message_keys,
                                                               message_values[1:])]
                    message_list.append(
                        dict(zip(['message_type'] + message_keys, [message_type] + casted_message_values)))
                elif message_type not in IGNORED_MESSAGES[p114_feed]:
                    print(row)
                    raise ValueError('message type {} not recognised'.format(message_type))
        message_list = list(filter(lambda x: x['message_type'] in ['MPD', 'GP9', 'GMP'], message_list))
        return message_list
    else:
        return []


def insert_data(message_list, p114_date, pool = []):
    """

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

    if len(message_list) > 0:
        MPD = message_list[0]

        message_list = message_list[1:]

        idx_list = [idx for idx, x in enumerate(message_list) if x['message_type'] == 'GP9']

        message_list_list = [message_list[idx:idx_list[_id + 1]] if _id < len(idx_list) - 1
                             else message_list[idx:] for _id, idx
                             in enumerate(idx_list)]

        # By GSP and YEAR ONLY
        dict_MPD = {m[0]['gsp_id']:pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
                                                                             sr_type=MPD['sr_type'],
                                                                             run_no=MPD['run_no'],
                                                                             group=MPD[
                                                                                 'gsp_group']).pivot_table(
            index=['group', 'sr_type', 'run_no', 'date'], columns=['sp']) for m in message_list_list}

        if type(pool) == list:
            foldername = P114_INPUT_DIR
        elif type(pool) == int:
            foldername = P114_INPUT_DIR + "{}/".format(pool)

        if not os.path.exists(foldername):
            print("generating dir")
            os.makedirs(foldername)

        for gsp, df_MPD in dict_MPD.items():
            filedir = foldername + 'gspdemand-{}-{}.csv'.format(gsp, p114_date.year)

            if not os.path.isfile(filedir):
                df_MPD.to_csv(filedir, mode='a', header=True)
            else:
                df_MPD.to_csv(filedir, mode='a', header=False)

        # By YEAR ONLY
        # df_MPD = pd.concat([pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
        #                                                                               sr_type=MPD['sr_type'],
        #                                                                               run_no=MPD['run_no'],
        #                                                                               gsp=m[0]['gsp_id'],
        #                                                                               group=MPD[
        #                                                                                   'gsp_group']).pivot_table(
        #     index=['group', 'gsp', 'sr_type', 'run_no', 'date'], columns=['sp']) for m in message_list_list])
        #
        # if type(pool) == list:
        #     foldername = P114_INPUT_DIR
        # elif type(pool) == int:
        #     foldername = P114_INPUT_DIR + "{}/".format(pool)
        #
        # if not os.path.exists(foldername):
        #     os.makedirs(foldername)
        #
        # filedir = foldername + 'gspdemand-{}.csv'.format(p114_date.year)
        #
        # if not os.path.isfile(filedir):
        #     df_MPD.to_csv(filedir, mode='a', header=True)
        # else:
        #     df_MPD.to_csv(filedir, mode='a', header=False)

        # By GROUP ONLY
        # list_MPD = [
        #     pd.DataFrame(GSP[1:])
        #         .drop(columns=['message_type'])
        #         .assign(date=p114_date.date(), sr_type=MPD['sr_type'], run_no=MPD['run_no'])
        #         .pivot(index=['date', 'sr_type', 'run_no'], columns='sp')
        #         .reset_index()
        #         .assign(gsp=GSP[0]['gsp_id'])
        #     for GSP in message_list_list]
        #
        # df_MPD = pd.concat(list_MPD)
        #
        # if not os.path.isfile(P114_INPUT_DIR+"gspdemand-{}.csv".format(MPD['gsp_group'])):
        #     df_MPD.to_csv(P114_INPUT_DIR+"gspdemand-{}.csv".format(MPD['gsp_group']),
        #                                   mode='a', header=True, index=False)
        # else:
        #     df_MPD.to_csv(P114_INPUT_DIR+"gspdemand-{}.csv".format(MPD['gsp_group']),
        #                                   mode='a', header=False, index=False)


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
            get_p114_file(filename, overwrite=True)
            insert_data(file_to_message_list(filename), p114_date)
            os.remove(P114_INPUT_DIR + filename)
    else:
        print('No relevant files found')


def pull_p114_date_files_parallel(p114_date: dt.date, q: mp.Queue) -> None:
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
            if type(q) == list:
                continue
            else:
                q.put({'filename': filename, 'p114_date': p114_date})
    else:
        print('No relevant files found')


def combine_data(q: mp.Queue, pool : int):
    time.sleep(10)
    t0 = dt.datetime.now()
    for i in range(20):
        while q.qsize() > 0:
            t1 = dt.datetime.now()
            if (t1-t0).seconds > 10:
                print('Running combine pool {}'.format(pool))
                t0 = dt.datetime.now()
            _file_data = q.get()
            filename = _file_data['filename']
            p114_date = _file_data['p114_date']
            insert_data(file_to_message_list(filename), p114_date, pool)
            # os.remove(P114_INPUT_DIR + filename)
            if not os.path.exists(P114_INPUT_DIR + "/done"):
                os.makedirs(P114_INPUT_DIR + "/done")
            shutil.move(P114_INPUT_DIR + filename, P114_INPUT_DIR + "/done/" + filename)
        print("Pool queue empty {}".format(pool))
        time.sleep(180)


def pull_data_parallel(start_date, end_date, t0, q, status_q):
    completed_requests = 0
    date = start_date
    while (date >= start_date) if reverse else (date <= end_date):
        if ((dt.datetime.now() - t0).seconds / 60) - (request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))
            pull_p114_date_files_parallel(date, q)
            date += dt.timedelta(days=-1 if reverse else 1)
            completed_requests += -1 if reverse else 1
    return 0


def pull_data(date, end_date, t0):
    completed_requests = 0
    while date <= end_date:
        if ((dt.datetime.now() - t0).seconds / 60) - (request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))
            pull_p114_date_files(date)
            date += dt.timedelta(days=1)
            completed_requests += 1


def merge_data():
    pool_folders = list(filter(lambda x: '.gitkeep' not in x, os.listdir(P114_INPUT_DIR)))

    gsp_func = lambda x: [y.split('-')[1] for y in x]
    pool_folders_files = [os.listdir(P114_INPUT_DIR + '/{}'.format(pool_folder)) for pool_folder in pool_folders]

    pool_folder_gsps = [gsp_func(pool_folder_files) for pool_folder_files in pool_folders_files]

    gsps = list(set(reduce(lambda l, r: list(set(l+r)), pool_folder_gsps)))

    dict_gsp_dfs = {gsp: pd.concat([pd.concat(
        [pd.read_csv(P114_INPUT_DIR + '/{}/{}'.format(pool_folder, filename), header=[0, 1], index_col=[0, 1, 2, 3])
         for filename in os.listdir(P114_INPUT_DIR + "/{}".format(pool_folder)) if gsp in filename]) for pool_folder in
           pool_folders]) for gsp in gsps}

    for gsp, df_gsp in dict_gsp_dfs.items():
        df_gsp.to_csv(P114_INPUT_DIR+"/gspdemand-{}.csv".format(gsp))


def run_parallel(*args, **options):
    """downloads P114 data for specific date range,
    expected 2 arguments of form ['yyyy-mm-dd', 'yyyy-mm-dd']"""
    if os.path.isfile(P114_INPUT_DIR + "gsp_demand.csv"):
        os.remove(P114_INPUT_DIR + "gsp_demand.csv")

    if not os.path.exists(P114_INPUT_DIR):
        os.makedirs(P114_INPUT_DIR)

    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])

    start_dates = [dt.datetime(*(start_date + pool * ((end_date - start_date) / pull_pools)).date().timetuple()[:3]) for
                   pool in range(pull_pools)]
    end_dates = [start_date_ + dt.timedelta(days=-1) for start_date_ in start_dates[1:]] + [end_date]

    if reverse:
        temp = end_dates
        end_dates = start_dates
        start_dates = temp
        del temp

    dates = list(zip(start_dates, end_dates))

    t0 = dt.datetime.now()

    q = mp.Queue()
    status_q = mp.Queue()

    if pull_pools == 0:
        files = list(filter(lambda f: '.gz' in f, os.listdir(P114_INPUT_DIR)))

        [q.put(
            {'filename': file, 'p114_date': dt.datetime.strptime(files[0].split('.')[0].split('_')[-1][:-6], '%Y%m%d')})
         for file in files]

    workers = [mp.Process(target=pull_data_parallel, args=(date_[0], date_[1], t0, q, status_q,))
               for date_ in dates] + \
                [mp.Process(target=combine_data, args=(q,pool,)) for pool in range(1,MAX_POOLS-pull_pools+1)]

    # Execute workers
    for p in workers:
        p.start()
    # Add worker to queue and wait until finished
    for p in workers:
        p.join()

    merge_data()


def run(*args, **options):
    """downloads P114 data for specific date range,
    expected 2 arguments of form ['yyyy-mm-dd', 'yyyy-mm-dd']"""
    if os.path.isfile(P114_INPUT_DIR + "gsp_demand.csv"):
        os.remove(P114_INPUT_DIR + "gsp_demand.csv")
    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])
    date = start_date
    t0 = dt.datetime.now()
    completed_requests = 0
    while date <= end_date:
        if ((dt.datetime.now() - t0).seconds / 60) - (request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))
            pull_p114_date_files(date)
            date += dt.timedelta(days=-1 if reverse else 1)
            completed_requests += -1 if reverse else 1
