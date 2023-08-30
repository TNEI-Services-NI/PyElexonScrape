import datetime
import datetime as dt
import multiprocessing as mp
import os.path
import time

import numpy as np
import pandas as pd

import data_manager.B1610.data_pull as B1610_data_pull
import data_manager.B1610.util as B1610_util
import data_manager.P114.data_pull as P114_data_pull
import data_manager.P114.util as p114_util
import data_manager._config as cf
import data_manager.util as dm_util


def run_demand_parallel(*args, **options):
    """
    Downloads P114 demand data in parallel for a specific date range.

    This function is responsible for downloading P114 demand data for a specified date range.
    It operates in parallel using multiprocessing. The function creates multiple worker processes
    to pull data, combine it, and merge the final results.

    Parameters:
        *args: Additional arguments (not used in this function).
        **options (dict):
            'missing' (bool): Indicates whether data for missing dates should be downloaded.
            'date' (list of str): Specifies the date range for which data should be downloaded.
                                 Format: ['yyyy-mm-dd', 'yyyy-mm-dd']

    Returns:
        None
    """
    if os.path.isfile(cf.P114_INPUT_DIR + "gsp_demand.csv"):
        os.remove(cf.P114_INPUT_DIR + "gsp_demand.csv")
    if not os.path.exists(cf.P114_INPUT_DIR):
        os.makedirs(cf.P114_INPUT_DIR)

    # Check if 'missing' option is provided
    missing = options.get('missing', False)

    if missing:
        # Read missing dates from file if 'missing' is True
        dates = pd.read_csv('/'.join([p114_util.DIR, 'missing_dates.csv']), index_col=0)
        start_date = pd.to_datetime(dates.iloc[0, 0])
        end_date = pd.to_datetime(dates.iloc[-1, 0])
    else:
        # Parse start and end dates from options
        start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
        end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])

    # Calculate start and end dates for each pool
    start_dates = [dt.datetime(*(start_date + pool * ((end_date - start_date) / cf.pull_pools)).date().timetuple()[:3])
                   for pool in range(cf.pull_pools)]
    end_dates = [start_date_ + dt.timedelta(days=-1) for start_date_ in start_dates[1:]] + [end_date]

    # Reverse start and end dates if needed
    if cf.reverse:
        temp = end_dates
        end_dates = start_dates
        start_dates = temp
        del temp

    t0 = dt.datetime.now()

    # Create multiprocessing Queues for communication between processes
    q = mp.Queue()
    status_q = mp.Queue()

    # Create directories if not exists
    if not os.path.exists(cf.P114_INPUT_DIR.replace('/gz/', "/done/")):
        os.makedirs(cf.P114_INPUT_DIR.replace('/gz/', "/done/"))

    # Read or generate settlement dates
    if os.path.exists('settlement_dates.p'):
        settlement_dates = dm_util.read_fill_and_write_settlement_dates(options)
    else:
        settlement_dates = dm_util.read_and_write_settlement_dates()

    # Filter settlement dates
    settlement_dates = dm_util.filter_settlement_dates(start_date, end_date, settlement_dates, cf.PROCESSED_FEEDS)

    process_stored_files_only = cf.pull_pools == 0

    if process_stored_files_only:
        # Create a list of files to merge
        file_list = dm_util.create_file_merge_list(start_date, end_date, settlement_dates, cf.P114_INPUT_DIR)

        # Put file paths into the queue for merging
        [q.put(f) for f in file_list]

        print('\tDone')
    else:
        # Create worker processes for data pulling
        workers = [mp.Process(target=P114_data_pull.pull_data_parallel, args=(date_, t0, q, status_q,))
                   for date_ in np.array_split(settlement_dates, cf.pull_pools)]

        # Start worker processes
        for p in workers:
            p.start()
        # Wait for worker processes to finish
        for p in workers:
            p.join()

    workers = [mp.Process(target=p114_util.combine_data, args=(q, pool,)) for pool in range(0, cf.MAX_POOLS)]

    # Start worker processes for data combining
    for p in workers:
        p.start()
    # Wait for worker processes to finish
    for p in workers:
        p.join()

    # Merge data from the combined files
    p114_util.merge_data(cf.MAX_POOLS)



def run_demand(*args, **options):
    """
    Downloads P114 demand data for a specific date range.

    Parameters:
        *args: Additional arguments (not used in this function).
        **options (dict):
            'date' (list of str): Specifies the date range for which data should be downloaded.
                                 Format: ['yyyy-mm-dd', 'yyyy-mm-dd']

    This function downloads P114 demand data for a specified date range. It removes any existing
    'gsp_demand.csv' file, creates the necessary directory structure if it doesn't exist,
    and then iterates over the date range to download the data files.

    Args:
        *args: Not used.
        **options: A dictionary containing the 'date' key specifying the date range.

    Returns:
        None
    """
    if os.path.isfile(cf.P114_INPUT_DIR + "gsp_demand.csv"):
        os.remove(cf.P114_INPUT_DIR + "gsp_demand.csv")
    if not os.path.isdir(cf.P114_INPUT_DIR):
        os.makedirs(cf.P114_INPUT_DIR)

    # Parse start and end dates from options
    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])
    date = start_date
    t0 = dt.datetime.now()
    completed_requests = 0

    # Iterate over the date range
    while date <= end_date:
        # Check if it's time to make another request based on request_interval_mins
        if ((dt.datetime.now() - t0).seconds / 60) - (cf.request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))

            # Download data for the current date
            P114_data_pull.pull_p114_date_files(date)

            # Move to the next date
            date += dt.timedelta(days=-1 if cf.reverse else 1)
            completed_requests += -1 if cf.reverse else 1


def run_generation(*args, **options):
    """
    Downloads B1610 generation data for a specific date range.

    This function downloads B1610 generation data for a specified date range and settlement periods.
    It iterates through each date and settlement period, downloading the corresponding data files.

    Parameters:
        *args: Additional arguments (not used in this function).
        **options (dict):
            'date' (list of str): Specifies the date range for which data should be downloaded.
                                 Format: ['yyyy-mm-dd', 'yyyy-mm-dd']

    Returns:
        None
    """
    if os.path.isfile(cf.B1610_INPUT_DIR + "gsp_generation.csv"):
        os.remove(cf.B1610_INPUT_DIR + "gsp_generation.csv")
    if not os.path.isdir(cf.B1610_INPUT_DIR):
        os.makedirs(cf.B1610_INPUT_DIR)

    # Parse start and end dates from options
    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])

    date = start_date
    t0 = dt.datetime.now()
    completed_requests = 0

    # Iterate through each date in the specified range
    while date <= end_date:
        print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
        print("downloading data for " + '{:%Y-%m-%d}'.format(date))

        # Convert date to string format
        date_string = datetime.datetime.strftime(date, '%Y-%m-%d')

        # Iterate through each settlement period (1 to 50)
        for settlement_period in range(1, 50 + 1):
            filename = '_'.join([date_string, str(settlement_period)]) + '.csv'

            # Check if the file exists and if overwrite is not set
            if os.path.isfile(cf.B1610_INPUT_DIR + filename) and not 'overwrite' in options.keys():
                continue

            # Calculate the wait time for rate limiting
            wait = ((dt.datetime.now() - t0).seconds) - (cf.request_interval_secs * completed_requests + 1)
            time.sleep(-1 * wait) if wait < 0 else None

            # Check if it's time to make a request based on rate limiting
            if ((dt.datetime.now() - t0).seconds) - (cf.request_interval_secs * completed_requests) > 0:
                # Call the function to download B1610 data
                B1610_data_pull.get_B1610_data(date_string, filename, settlement_period)
                completed_requests += -1 if cf.reverse else 1

        # Move to the next date
        date += dt.timedelta(days=-1 if cf.reverse else 1)

    # Merge the downloaded data files
    B1610_util.merge_data()


def run(*args, **options):
    """
    Possible Keys for the 'options' Dictionary:

    - 'missing' (bool):
      - Description: Indicates whether data for missing dates should be downloaded.
      - Usage: Used in the 'run_demand_parallel' function.

    - 'date' (list of str):
      - Description: Specifies the date range for which data should be downloaded.
      - Format: ['yyyy-mm-dd', 'yyyy-mm-dd']
      - Usage: Used in the 'run_demand_parallel', 'run_demand', and 'run_generation' functions.

    - 'mode' (str):
      - Description: Specifies the execution mode of the script.
      - Values: 'parallel' for parallel execution, any other value for non-parallel execution.
      - Usage: Used in the 'run' function to determine the mode of execution.

    - 'type' (str):
      - Description: Specifies the type of data to be processed (e.g., 'demand' or 'generation').
      - Values: 'demand' or 'generation'
      - Usage: Used in the 'run' function to determine which data processing function to call.

    - 'overwrite' (bool):
      - Description: Determines whether existing files should be overwritten during data download.
      - Usage: Used in the 'run_generation' function to control file overwrite behavior.

    Note: These keys are used within the provided code snippet to control the behavior of different functions. Depending on the context and other parts of the code, additional keys may be used elsewhere.
    """

    if options['mode'] == 'parallel':
        if options['type'] == 'demand':
            run_demand_parallel(**options)
        elif options['type'] == 'generation':
            run_generation(*args, **options)  # todo (MM) parallel pull of generation
    else:
        if options['type'] == 'demand':
            run_demand(*args, **options)
        elif options['type'] == 'generation':
            run_generation(*args, **options)

