
import os
import pandas as pd
from data_manager._config import *
from functools import reduce
from data_manager._data_definitions import *
import csv

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np

def merge_data():
    P114_OUTPUT_DIR = P114_INPUT_DIR.replace('/gz', '')

    pool_folders = list(filter(lambda x: ('.gitkeep' not in x)&(x!='done')&(x!='gz')&('.csv' not in x), os.listdir(P114_OUTPUT_DIR)))

    gsp_func = lambda x: [y.split('-')[1] for y in x if 'agg' not in y]
    group_func = lambda x: [y.split('-')[1] for y in x if 'agg' in y]
    pool_folders_files = [os.listdir(P114_OUTPUT_DIR.replace('/gz','') + '/{}'.format(pool_folder)) for pool_folder in pool_folders]

    pool_folder_gsps = [gsp_func(pool_folder_files) for pool_folder_files in pool_folders_files]
    pool_folder_groups = [group_func(pool_folder_files) for pool_folder_files in pool_folders_files]

    gsps = list(set(reduce(lambda l, r: list(set(l + r)), pool_folder_gsps)))
    groups = list(set(reduce(lambda l, r: list(set(l + r)), pool_folder_groups)))

    for gsp in gsps:
        list_gsp_files = []
        for pool_folder in pool_folders:
            list_pool_gsp_files = []
            for filename in list(
                    filter(lambda x: 'gspdemand' in x, os.listdir(P114_OUTPUT_DIR + "/{}".format(pool_folder)))):
                if gsp in filename:
                    try:
                        list_pool_gsp_files.append(pd.read_csv(P114_OUTPUT_DIR + '/{}/{}'.format(pool_folder, filename),
                                                               header=[0, 1], index_col=[0, 1, 2, 3],
                                                               error_bad_lines=False,
                                                               ))
                    except Exception as e:
                        with open(P114_OUTPUT_DIR + '/{}/{}'.format(pool_folder, filename)) as csv_file:
                            csv_reader = csv.reader(csv_file, delimiter=',')
                            line_count = 0
                            for row in csv_reader:
                                if line_count == 0:
                                    print(f'Column names are {", ".join(row)}')
                                    line_count += 1
                                else:
                                    if len(row) == 142:
                                        # print('length = {}'.format(len(row)))
                                        print(row)
                                    line_count += 1

                else:
                    continue
            if len(list_pool_gsp_files) > 0:
                pool_gsp = pd.concat(list_pool_gsp_files)
                list_gsp_files.append(pool_gsp)
        if len(list_gsp_files) > 0:
            df_gsp = pd.concat(list_gsp_files)

        df_gsp = df_gsp.reset_index()

        df_gsp.loc[:, ('sr_type', '')] = df_gsp.loc[:, ('sr_type', '')].map(
            {v: idx for idx, v in enumerate(list(reversed(sr_type_map)))})

        df_gsp = df_gsp.sort_values(by=[('date', ''), ('sr_type', ''), ('run_no', '')])
        df_gsp = df_gsp.drop_duplicates(subset=[('date', '')], keep='last')

        df_gsp.loc[:, ('sr_type', '')] = df_gsp.loc[:, ('sr_type', '')].map(
            {idx: v for idx, v in enumerate(list(reversed(sr_type_map)))})

        df_gsp.loc[~(df_gsp.isna().T.any())].to_csv(P114_OUTPUT_DIR + "/gspdemand-{}.csv".format(gsp))

    for group in groups:
        list_group_files = []
        for pool_folder in pool_folders:
            list_pool_group_files = []
            for filename in list(
                    filter(lambda x: 'agg' in x, os.listdir(P114_OUTPUT_DIR + "/{}".format(pool_folder)))):
                if group in filename:
                    list_pool_group_files.append(pd.read_csv(P114_OUTPUT_DIR + '/{}/{}'.format(pool_folder, filename),
                                                           header=[0, 1], index_col=[0, 1, 2, 3],
                                                           error_bad_lines=False))
                else:
                    continue
            if len(list_pool_group_files) > 0:
                pool_group = pd.concat(list_pool_group_files)
                list_group_files.append(pool_group)
        if len(list_group_files) > 0:
            df_group = pd.concat(list_group_files)

        df_group = df_group.reset_index()

        df_group.loc[:, ('sr_type', '')] = df_group.loc[:, ('sr_type', '')].map(
            {v: idx for idx, v in enumerate(list(reversed(sr_type_map)))})

        df_group = df_group.sort_values(by=[('date', ''), ('sr_type', ''), ('run_no', '')])
        df_group = df_group.drop_duplicates(subset=[('date', '')], keep='last')

        df_group.loc[:, ('sr_type', '')] = df_group.loc[:, ('sr_type', '')].map(
            {idx: v for idx, v in enumerate(list(reversed(sr_type_map)))})

        df_group.loc[~(df_group.isna().T.any())].to_csv(P114_OUTPUT_DIR + "/gspdemand-{}.csv".format(group))


if __name__ == '__main__':
    merge_data()