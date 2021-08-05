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
import datetime
import datetime as dt
import json
import multiprocessing as mp
import os.path
import urllib.request

import requests
from tqdm import tqdm

import data_manager._config as cf
import data_manager.B1610.util as B1610_util
from data_manager._data_definitions import *


def get_B1610_data(date_string, filename, settlement_period, overwrite=True):
    """
    downloads specified B1610 file

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

    if not os.path.isfile(cf.B1610_INPUT_DIR + filename) or overwrite:
        remote_url = (cf.B1610_DOWNLOAD_URL.format(cf.ELEXON_KEY, date_string, settlement_period))
        result = urllib.request.urlretrieve(remote_url,
                                   cf.B1610_INPUT_DIR + filename)
