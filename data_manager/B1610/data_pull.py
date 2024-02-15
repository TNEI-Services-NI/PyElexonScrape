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
import os.path
import urllib.request

import data_manager._config as cf


def get_B1610_data(date_string, filename, settlement_period, overwrite=True):
    """
    Downloads B1610 data for a specific date and settlement period.

    Parameters:
        date_string (str): The date for which the data is being downloaded in the format 'yyyy-mm-dd'.
        filename (str): The name of the file to be saved.
        settlement_period (str): The settlement period for which the data is being downloaded.
        overwrite (bool): Indicates whether to overwrite an existing file with the same name.
                          Default is True.

    Returns:
        None
    """
    # Check if the file does not exist or 'overwrite' is set to True
    if not os.path.isfile(cf.B1610_INPUT_DIR + filename) or overwrite:
        # Construct the remote URL for downloading
        remote_url = cf.B1610_DOWNLOAD_URL.format(cf.ELEXON_KEY, date_string, settlement_period)

        # Download the data and save to the specified location
        result = urllib.request.urlretrieve(remote_url, cf.B1610_INPUT_DIR + filename)
