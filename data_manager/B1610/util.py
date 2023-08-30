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


def merge_data():
    date_settlement = list(filter(lambda x: ('.gitkeep' not in x), os.listdir(cf.B1610_INPUT_DIR)))

    data = [pd.read_csv('/'.join([cf.B1610_INPUT_DIR, f])) for f in date_settlement]

    data = [file.reset_index() for file in data if len(file) > 0]

    for idf, file in enumerate(data):
        data[idf].columns = data[idf].iloc[0, :]

    data = [file.iloc[1:,:] for file in data]

    data = pd.concat(data)

    if os.path.isfile('/'.join([cf.B1610_PROCESSED_DIR, 'gsp_generation.csv'])):
        data.to_csv('/'.join([cf.B1610_PROCESSED_DIR, 'gsp_generation.csv']), index=False, header=False, mode='a')
    else:
        data.to_csv('/'.join([cf.B1610_PROCESSED_DIR, 'gsp_generation.csv']), index=False)








