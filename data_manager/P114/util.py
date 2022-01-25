# -*- coding: utf-8 -*-
"""
- Author: 
- Position: 
- Company: TNEI Services, Glasgow
- E-mail: 
- IDE: PyCharm
- Project Name: gb-data-pull
- Date: 02/08/2021
- Time: 09:37
"""
import datetime
import glob
import datetime as dt
import gzip
import multiprocessing as mp
import os.path
import shutil
import time
from functools import reduce

import pandas as pd

import data_manager._config as cf
from data_manager._data_definitions import *
import io

DIR = os.path.dirname(__file__)

def add_delimiters(fpath, delimiter=','):

    s_data = ''
    max_num_delimiters = 0

    with open(fpath, 'r') as f:
        for line in f:
            s_data += line
            delimiter_count = line.count(delimiter)
            if delimiter_count > max_num_delimiters:
                max_num_delimiters = delimiter_count

    s_delimiters = delimiter * max_num_delimiters + '\n'

    return io.StringIO(s_delimiters + s_data)


def fix_DST_days():
    all_columns_index = pd.MultiIndex.from_arrays(
        [['ei' for x in range(1, 51)] + ['ii' for x in range(1, 51)] + ['vol' for x in range(1, 51)],
         [x for x in range(1, 51)] + [x for x in range(1, 51)] + [x for x in range(1, 51)]], names=[None, 'sp'])

    spring_columns_index = pd.MultiIndex.from_arrays(
        [['ei' for x in range(1, 47)] + ['ii' for x in range(1, 47)] + ['vol' for x in range(1, 47)],
         [x for x in range(1, 47)] + [x for x in range(1, 47)] + [x for x in range(1, 47)]], names=[None, 'sp'])

    autumn_columns_index = pd.MultiIndex.from_arrays(
        [['ei' for x in range(1, 51)] + ['ii' for x in range(1, 51)] + ['vol' for x in range(1, 51)],
         [x for x in range(1, 51)] + [x for x in range(1, 51)] + [x for x in range(1, 51)]], names=[None, 'sp'])

    regular_columns_index = pd.MultiIndex.from_arrays(
        [['ei' for x in range(1, 49)] + ['ii' for x in range(1, 49)] + ['vol' for x in range(1, 49)],
         [x for x in range(1, 49)] + [x for x in range(1, 49)] + [x for x in range(1, 49)]], names=[None, 'sp'])

    pool_folders = list(filter(lambda x: ('.gitkeep' not in x)&('gz' not in x)&('done' not in x)&('fixed_dst' not in x), os.listdir(cf.P114_INPUT_DIR.replace('gz/', ''))))

    gsp_func = lambda x: [y.split('-')[1] for y in x]
    pool_folders_files_gsp = [list(filter(lambda x: 'agggsp' not in x, os.listdir(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool_folder)))) for pool_folder in pool_folders]
    pool_folders_files_agg = [list(filter(lambda x: 'agggsp' in x, os.listdir(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool_folder)))) for pool_folder in pool_folders]

    pool_folder_gsps = [gsp_func(pool_folder_files) for pool_folder_files in pool_folders_files_gsp]
    pool_folder_groups = [gsp_func(pool_folder_files) for pool_folder_files in pool_folders_files_agg]

    gsps = list(set(reduce(lambda l, r: list(set(l+r)), pool_folder_gsps)))
    groups = list(set(reduce(lambda l, r: list(set(l+r)), pool_folder_groups)))

    # df_data = pd.read_csv(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/{}".format(pool_folders[0], pool_folders_files_gsp[0][0]))
    for key, pool_folder_files in {'agg': pool_folders_files_agg, 'gsp': pool_folders_files_gsp}.items():
        if key == 'gsp':
            index_cols = 4
        elif key == 'agg':
            index_cols = 3

        for pool_folder in pool_folders:
            new_path = cf.P114_INPUT_DIR.replace('gz/', 'fixed_dst/') + "{}".format(pool_folder)
            for filename in pool_folder_files[int(pool_folder)]:
                new_file = '/'.join([new_path, filename])

                if os.path.isfile(new_file):
                    pass

                df_data = pd.read_csv(add_delimiters(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/{}".format(pool_folder, filename)))

                df_data_index = df_data.iloc[2:, :index_cols]
                df_data_index.columns = df_data_index.iloc[0, :]
                df_data_index = df_data_index.iloc[1:, :]
                df_data_data = df_data.iloc[:, index_cols:]

                df_data_data.columns = pd.MultiIndex.from_arrays([df_data_data.iloc[0, :].values, df_data_data.iloc[1, :].values])
                df_data_data = df_data_data.iloc[3:, :]

                spring_rows = df_data_data.loc[:, [('vol', '48')]].isna().iloc[:, 0]

                columns_index = df_data_data.columns.to_series().reset_index()[0]
                sp48 = columns_index[columns_index == ('vol', '48')].index[0]

                if df_data_data.shape[1] > 144:
                    autumn_rows = ~df_data_data.iloc[:, [sp48+1]].isna().iloc[:, 0]
                else:
                    autumn_rows = pd.Series(index=df_data_data.index, data=False)

                regular_rows = ~spring_rows & ~autumn_rows
                regular_cols = df_data_data.columns.to_series().apply(lambda x: not pd.isna(x[0]))
                autumn_cols = df_data_data.columns.to_series().apply(lambda x: x==x)

                if spring_rows.any():
                    spring_cols = df_data_data.loc[spring_rows, :].apply(lambda x: ~x.isna(), axis=0).iloc[0,:]
                else:
                    spring_cols = regular_cols

                regular_data = df_data_data.loc[regular_rows, regular_cols]
                autumn_data = df_data_data.loc[autumn_rows, autumn_cols]
                spring_data = df_data_data.loc[spring_rows, spring_cols]

                new_data = pd.DataFrame(index=df_data_data.index, columns=all_columns_index)

                new_data.loc[regular_data.index, regular_columns_index] = regular_data.values
                if len(autumn_data) > 0:
                    new_data.loc[autumn_data.index, autumn_columns_index] = autumn_data.values
                if len(spring_data) > 0:
                    new_data.loc[spring_data.index, spring_columns_index] = spring_data.values

                new_data_ = df_data_index.merge(new_data, left_index=True, right_index=True)

                new_data_ = new_data_.set_index(new_data_.columns[:index_cols].values.tolist())

                new_data_.columns = all_columns_index



                if not os.path.isdir(new_path):
                    os.makedirs(new_path)

                new_data_.to_csv(new_file)

    print()


def merge_data(max_pools=7):
    data_dir = cf.P114_INPUT_DIR.replace('gz/', '')

    pool_folders = list(filter(
        lambda x: ('.gitkeep' not in x) & ('.p' not in x) & ('gz' not in x) & ('csv' not in x) & ('gsps' not in x) & ('done' not in x) & ('fixed_dst' not in x) & (
                    'non_target' not in x), os.listdir(data_dir)))

    pool_folder_files = {x: os.listdir('/'.join([data_dir, x])) for x in pool_folders}

    files = list(set(sum(pool_folder_files.values(), [])))

    missing_dates = set()

    if not os.path.exists(cf.P114_INPUT_DIR.replace('gz/', 'gsps/')):
        os.makedirs(cf.P114_INPUT_DIR.replace('gz/', 'gsps/'))

    for file in files:
        pool_files = sum([[pd.read_pickle('/'.join([data_dir, pool, f_])) for f_ in f if f_ == file] for pool, f in pool_folder_files.items()], [])
        df_data = pd.concat(pool_files)
        df_data = df_data.sort_values(by='date')

        missing_dates = missing_dates.union(set(pd.date_range(df_data['date'].sort_values().iloc[0], df_data['date'].sort_values().iloc[-1]).to_series().apply(lambda x: x.date())) - set(df_data['date']))

        df_data.to_pickle('/'.join([cf.P114_INPUT_DIR.replace('gz/', 'gsps/'), file]))

    missing_dates = pd.DataFrame({'missing': sorted(list(missing_dates))})
    missing_dates.to_csv('missing_dates.csv')


def merge_data_subsplit():
    pool_folders = list(filter(lambda x: ('.gitkeep' not in x)&('gz' not in x)&('done' not in x)&('fixed_dst' not in x)&('non_target' not in x), os.listdir(cf.P114_INPUT_DIR.replace('gz/', ''))))

    gsp_func = lambda x: [y.split('-')[1] for y in x]
    pool_folders_files_gsp = {pool_folder: list(filter(lambda x: ('gspdemand' in x) & ('ABV' not in x),
                                          os.listdir(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool_folder))))
                              for pool_folder in pool_folders}
    pool_folders_files_agg = {pool_folder: list(filter(lambda x: ('agggsp' in x) & ('ABV' not in x),
                                          os.listdir(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool_folder))))
                              for pool_folder in pool_folders}
    pool_folders_files_bmu = {pool_folder: list(filter(lambda x: ('bmu' in x) & ('ABV' not in x),
                                          os.listdir(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool_folder))))
                              for pool_folder in pool_folders}

    pool_folder_gsps = {k: gsp_func(pool_folder_files) for k, pool_folder_files in pool_folders_files_gsp.items()}
    pool_folder_groups = {k: gsp_func(pool_folder_files) for k, pool_folder_files in pool_folders_files_agg.items()}
    pool_folder_bmus = {k: gsp_func(pool_folder_files) for k, pool_folder_files in pool_folders_files_bmu.items()}

    gsps = list(set(reduce(lambda l, r: list(set(l+r)), pool_folder_gsps.values())))
    groups = list(set(reduce(lambda l, r: list(set(l+r)), pool_folder_groups.values())))
    bmus = list(set(reduce(lambda l, r: list(set(l+r)), pool_folder_bmus.values())))

    dict_gsp_dfs = {gsp: pd.concat([
        pd.concat([
            pd.read_csv(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/{}".format(pool, filename), header=[0, 1], index_col=[0, 1, 2, 3])
         for filename in pool_files]) for pool, pool_files in pool_folders_files_gsp.items()])
        for gsp in gsps}

    for gsp, df_gsp in dict_gsp_dfs.items():
        df_gsp.to_csv(cf.P114_INPUT_DIR.replace('gz/', '')+"/gspdemand-{}.csv".format(gsp))

    dict_gsp_dfs = None
    del dict_gsp_dfs

    dict_gspagg_dfs = {group: pd.concat([
        pd.concat([
            pd.read_csv(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/{}".format(pool, filename), header=[0, 1],
                        index_col=[0, 1, 2, 3])
            for filename in pool_files]) for pool, pool_files in pool_folders_files_agg.items()])
        for group in groups}
    for group, df_group in dict_gspagg_dfs.items():
        df_group.to_csv(cf.P114_INPUT_DIR.replace('gz/', '')+"/agggspdemand-{}.csv".format(group))

    dict_gspagg_dfs = None
    del dict_gspagg_dfs

    dict_bmu_dfs = {bmu: pd.concat([
        pd.concat([
            pd.read_csv(cf.P114_INPUT_DIR.replace('gz/', '') + "{}/{}".format(pool, filename), header=[0, 1],
                        index_col=[0, 1, 2, 3])
            for filename in pool_files]) for pool, pool_files in pool_folders_files_bmu.items()])
        for bmu in bmus}
    for bmu, df_bmu in dict_bmu_dfs.items():
        df_bmu.to_csv(cf.P114_INPUT_DIR.replace('gz/', '')+"/bmu-{}.csv".format(bmu))

    dict_bmu_dfs = None
    del dict_bmu_dfs


def combine_data(q: mp.Queue, pool: int):
    time.sleep(1)
    t0 = dt.datetime.now()
    dict_data = None
    print('Running combine pool {}'.format(pool))
    move_files = []
    count = 0
    if type(pool) == list:
        foldername = cf.P114_INPUT_DIR.replace('gz/', '')
    elif type(pool) == int:
        foldername = cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool)
    if not os.path.exists(foldername):
        print("generating dir")
        os.makedirs(foldername)
    for i in range(20):
        while q.qsize() > 0:
            count += 1
            t1 = dt.datetime.now()
            _file_data = q.get()
            filename = _file_data['filename']
            p114_date = _file_data['p114_date']
            dict_data = insert_data(file_to_message_list(filename), filename, dict_data, pool)
            # os.remove(cf.P114_INPUT_DIR + filename)
            if not os.path.exists(cf.P114_INPUT_DIR.replace('/gz/', "/done/")):
                os.makedirs(cf.P114_INPUT_DIR.replace('/gz/', "/done/"))
            move_files.append((cf.P114_INPUT_DIR + filename, cf.P114_INPUT_DIR.replace('/gz/', "/done/") + filename))

            if (count > 100 or q.qsize() == 0) and dict_data is not None:
                for k, v in dict_data.items():
                    if len(v) > 0:
                        if k == 'MPD':
                            for gsp, df in v.reset_index(level=0).groupby('level_0'):
                                filedir = foldername + f'gspdemand-{gsp}.p'
                                df = df.iloc[:, 1:]
                                if os.path.isfile(filedir):
                                    df = pd.concat([pd.read_pickle(filedir), df])
                                df.to_pickle(filedir)
                        else:
                            filedir = foldername + f'{k}.csv'
                            if os.path.isfile(filedir):
                                v.to_csv(filedir, header=False, mode='a')
                            else:
                                v.to_csv(filedir)
                dict_data = None
                count = 0

                for files in move_files:
                    shutil.move(files[0], files[1])

                move_files = []

        print("Pool queue empty {}".format(pool))
        # time.sleep(180)



def insert_data(message_list, filename, dict_data = None, pool = [], target=None, return_result=True):
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

    columns = pd.MultiIndex.from_arrays([['ei' for x in range(1,51)]+['ii' for x in range(1,51)]+['vol' for x in range(1,51)], [x for x in range(1,51)]+[x for x in range(1,51)]+[x for x in range(1,51)]], names=[None, 'sp'])
    if dict_data is None:
        dict_data = {'MPD': pd.DataFrame(columns=columns), 'AGV': pd.DataFrame(columns=columns), 'ABV': pd.DataFrame(columns=columns)}
    p114_date = datetime.datetime.strptime(filename.split('_')[1], '%Y%m%d')

    if len(message_list) > 0:
        if message_list[0]['message_type'] =='MPD' and (target is None or target == 'MPD'):
            MPD = message_list[0]

            message_list = message_list[1:]

            idx_list = [idx for idx, x in enumerate(message_list) if x['message_type'] == 'GP9']

            message_list_list = [message_list[idx:idx_list[_id + 1]] if _id < len(idx_list) - 1
                                 else message_list[idx:] for _id, idx
                                 in enumerate(idx_list)]

            # By GSP and YEAR ONLY
            # dict_MPD = {m[0]['gsp_id']:pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
            #                                                                      sr_type=MPD['sr_type'],
            #                                                                      run_no=MPD['run_no'],
            #                                                                      group=MPD[
            #                                                                          'gsp_group']).pivot_table(
            #     index=['group', 'sr_type', 'run_no', 'date'], columns=['sp']) for m in message_list_list}
            list_MPD = {m[0]['gsp_id']:pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
                                                                                 sr_type=MPD['sr_type'],
                                                                                 run_no=MPD['run_no'],
                                                                                 group=MPD['gsp_group'],
                                                                                 gsp_id=m[0]['gsp_id'],
                                                                                                 ).pivot_table(
                index=['group', 'sr_type', 'run_no', 'date', 'gsp_id'], columns=['sp']) for m in message_list_list}

            if type(pool) == list:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '')
            elif type(pool) == int:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool)

            if not os.path.exists(foldername):
                print("generating dir")
                os.makedirs(foldername)

            if not os.path.exists(foldername):
                print("generating dir")
                os.makedirs(foldername)

            df_MPD = pd.concat(list_MPD).reset_index()

            if return_result:
                dict_data['MPD'] = pd.concat([df_MPD, dict_data['MPD']])
                return dict_data

            # for gsp, df_MPD in dict_MPD.items():
            #     filedir = foldername + 'gspdemand-{}-{}.csv'.format(gsp, p114_date.year)
            #
            #     if not os.path.isfile(filedir):
            #         df_MPD.to_csv(filedir, mode='a', header=True)
            #     else:
            #         df_MPD.to_csv(filedir, mode='a', header=False)
        elif message_list[0]['message_type'] =='AGV' and (target is None or target == 'AGV'):
            idx_list = [idx for idx, x in enumerate(message_list) if x['message_type'] == 'AGV']

            message_list_list = [message_list[idx:idx_list[_id + 1]] if _id < len(idx_list) - 1
                                 else message_list[idx:] for _id, idx
                                 in enumerate(idx_list)]

            # By GSP and YEAR ONLY
            list_AGV = [pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
                                                                                                  sr_type=m[0][
                                                                                                      'sr_type'],
                                                                                                  run_no=m[0]['run_no'],
                                                                                                  gsp_group=m[0]['gsp_group'],
                                                                                                     ).pivot_table(
                index=['sr_type', 'run_no', 'date', 'gsp_group'], columns=['sp']) for m in message_list_list]

            if type(pool) == list:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '')
            elif type(pool) == int:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool)

            if not os.path.exists(foldername):
                print("generating dir")
                os.makedirs(foldername)

            if type(pool) == list:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '')
            elif type(pool) == int:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool)

            if not os.path.exists(foldername):
                print("generating dir")
                os.makedirs(foldername)

            df_AGV = pd.concat(list_AGV)

            if return_result:
                dict_data['AGV'] = pd.concat([df_AGV, dict_data['AGV']])
                return dict_data

            # for gsp_group, df_AGV in dict_AGV.items():
            #     filedir = foldername + 'agggspdemand-{}-{}.csv'.format(gsp_group, p114_date.year)
            #
            #     df_AGV_ = pd.DataFrame(index=df_AGV.index, columns=columns)
            #     df_AGV_.loc[:, df_AGV.columns] = df_AGV.loc[:, :]
            #
            #     if not os.path.isfile(filedir):
            #         df_AGV_.to_csv(filedir, mode='a', header=True)
            #     else:
            #         df_AGV_.to_csv(filedir, mode='a', header=False)
        elif message_list[0]['message_type'] =='ABV' and (target is None or target == 'ABV'):
            idx_list = [idx for idx, x in enumerate(message_list) if x['message_type'] == 'ABV']

            message_list_list = [message_list[idx:idx_list[_id + 1]] if _id < len(idx_list) - 1
                                 else message_list[idx:] for _id, idx
                                 in enumerate(idx_list)]

            # By GSP and YEAR ONLY
            # dict_AGV = {m[0]['bmu_id']: pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
            #                                                                                       sr_type=m[0][
            #                                                                                           'sr_type'],
            #                                                                                       run_no=m[0]['run_no'],
            #                                                                                       bmu_id=m[0]['bmu_id'],
            #                                                                                          ).pivot_table(
            #     index=['sr_type', 'run_no', 'date', 'bmu_id'], columns=['sp']) for m in message_list_list}

            list_ABV = [pd.DataFrame(m[1:]).drop(columns=['message_type']).assign(date=p114_date.date(),
                                                                                                  sr_type=m[0][
                                                                                                      'sr_type'],
                                                                                                  run_no=m[0]['run_no'],
                                                                                                  bmu_id=m[0]['bmu_id'],
                                                                                                  ).pivot_table(
                index=['sr_type', 'run_no', 'date', 'bmu_id'], columns=['sp']) for m in message_list_list]

            if type(pool) == list:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '')
            elif type(pool) == int:
                foldername = cf.P114_INPUT_DIR.replace('gz/', '') + "{}/".format(pool)

            if not os.path.exists(foldername):
                print("generating dir")
                os.makedirs(foldername)

            df_ABV = pd.concat(list_ABV)

            if return_result:
                dict_data['ABV'] = pd.concat([df_ABV, dict_data['ABV']])
                return dict_data

            # for bmu_id, df_ABV in dict_ABV.items():
            #     filedir = foldername + 'bmuagg-{}-{}.csv'.format(bmu_id, p114_date.year)
            #
            #     df_ABV_ = pd.DataFrame(index=df_ABV.index, columns=columns)
            #     df_ABV_.loc[:, df_ABV.columns] = df_ABV.loc[:, :]
            #
            #     if not os.path.isfile(filedir):
            #         df_ABV_.to_csv(filedir, mode='a', header=True)
            #     else:
            #         df_ABV_.to_csv(filedir, mode='a', header=False)


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
        #     foldername = cf.P114_INPUT_DIR
        # elif type(pool) == int:
        #     foldername = cf.P114_INPUT_DIR + "{}/".format(pool)
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
        # if not os.path.isfile(cf.P114_INPUT_DIR+"gspdemand-{}.csv".format(MPD['gsp_group'])):
        #     df_MPD.to_csv(cf.P114_INPUT_DIR+"gspdemand-{}.csv".format(MPD['gsp_group']),
        #                                   mode='a', header=True, index=False)
        # else:
        #     df_MPD.to_csv(cf.P114_INPUT_DIR+"gspdemand-{}.csv".format(MPD['gsp_group']),
        #                                   mode='a', header=False, index=False)





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
    try:
        file = gzip.open(cf.P114_INPUT_DIR + filename, 'rb')
    except:
        print(filename)
        return []
    file_content = file.read().decode('utf-8', 'ignore')

    target_present = reduce(lambda l, r: l or r, [id_ in file_content for id_ in cf.TARGET_MESSAGES])

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
        message_list = list(filter(lambda x: x['message_type'] in cf.TARGET_MESSAGES, message_list))
        return message_list
    else:
        return []


if __name__ == '__main__':
    # import glob
    # gzs = glob.glob(cf.P114_INPUT_DIR.replace('gz/', 'done/*'))
    # src = cf.P114_INPUT_DIR.replace('gz/', 'done/')
    # for file in gzs:
    #     shutil.move(file, file.replace('/done', '/gz'))

    merge_data()
    # df = pd.read_csv(cf.P114_INPUT_DIR.replace('gz/', 'MPD.csv'), header=[0, 1, 2])
    # # df.columns = [df.iloc[0, idx] if any(['Unnamed' in x_ for x_ in x]) else '_'.join(x) for idx, x in enumerate(df.columns)]
    # df = df.iloc[:, 1:].copy()
    #
    # cols = [tuple(x_ if 'Unnamed' not in x_ else '' for x_ in x) for x in df.columns]
    # cols[0] = ('', 'sp', 'group')
    #
    # df.columns = pd.MultiIndex.from_tuples(cols)
    #
    # folder = cf.P114_INPUT_DIR.replace('gz/', 'gsps/')
    #
    # if not os.path.exists(folder):
    #     os.makedirs(folder)
    #
    # dict_data = {gsp: df.loc[df.iloc[:, 4] == gsp].drop(columns=df.columns[4]) for gsp in df.iloc[:, 4].unique()}
    #
    # for gsp, df in dict_data.items():
    #     df.to_csv(folder + f'gspdemand-{gsp}.csv', index=False)


