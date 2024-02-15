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
import sp2ts
import tqdm
import pandas as pd

import data_manager._config as cf
from data_manager._data_definitions import *


def handle_settlement_periods(settlement_data):
    """
    Handle settlement periods in the given settlement_data DataFrame.

    This function performs the following tasks:
    - Converts 'date' column to datetime and extracts only the date part.
    - Calculates Unix timestamps from 'date' and 'period' columns using sp2ts function.
    - Converts Unix timestamps to datetime to get the corresponding settlement end datetime.

    Parameters:
        settlement_data (pd.DataFrame): DataFrame containing settlement data.
            Must have columns "date" and "period".

    Returns:
        pd.DataFrame: DataFrame with added 'settlement_end' column.
    """
    settlement_data = settlement_data.copy()

    settlement_data['settlement_end'] = (pd.to_datetime(settlement_data['date']) +
                                         pd.to_timedelta((settlement_data['period'].astype(int, errors='ignore')), unit='m') * 30)

    settlement_data['settlement_end'] = pd.to_datetime(settlement_data['settlement_end'], utc=True)

    settlement_data['unix'] = (settlement_data['settlement_end'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

    return settlement_data

def merge_data():
    date_settlement = list(filter(lambda x: ('.gitkeep' not in x), os.listdir(cf.B1610_INPUT_DIR)))

    data = [pd.read_csv('/'.join([cf.B1610_INPUT_DIR, f])) for f in date_settlement]

    data = [file.reset_index() for file in data if len(file) > 0]

    for idf, file in enumerate(data):
        data[idf].columns = data[idf].iloc[0, :]

    data = [file.iloc[1:,:] for file in data]

    data = pd.concat(data)

    # Rename columns for clarity
    bmu_data = data.rename(columns={
        'Settlement Date': 'date',
        'SP': 'period',
    })

    # Handle settlement periods
    bmu_data = handle_settlement_periods(bmu_data)

    # Set multi-index based on columns and then reset index
    bmu_data = bmu_data.set_index(['date', 'period', 'settlement_end', 'unix']).reset_index()

    bmu_data.to_feather('/'.join([cf.B1610_PROCESSED_DIR, 'bmu_generation.feather']))








