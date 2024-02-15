
import glob
import os

import pandas as pd

import data_manager.P114.data_pull as P114_data_pull


def read_fill_and_write_settlement_dates(options):
    """
    Reads, fills, and writes settlement dates information based on provided options.

    This function reads the existing settlement dates DataFrame from 'settlement_dates.p',
    calculates a date sequence based on the range of dates, and checks if the existing dates
    cover the entire sequence. If not, it retrieves additional dates from P114_data_pull
    and appends them to the DataFrame. The DataFrame is then sorted and de-duplicated before
    being written back to 'settlement_dates.p'.

    Parameters:
        options (dict): Dictionary containing options for data retrieval.

    Returns:
        pd.DataFrame: Updated DataFrame containing settlement dates information.
    """
    # Read the existing settlement dates DataFrame
    all_dates = pd.read_feather('settlement_dates.f')

    date_sequence = pd.date_range(min(options['date']), max(options['date']))

    # Check if existing dates cover the entire date sequence
    if date_sequence.isin(all_dates['date']).all():
        pass  # No action needed if all dates are present
    else:
        # Retrieve missing dates and update the DataFrame
        missing_dates = date_sequence.to_series().loc[~date_sequence.to_series().isin(all_dates['date'])].dt.date
        all_dates_ = P114_data_pull.get_dates(dates=missing_dates)
        all_dates_.loc[:, 'date'] = pd.to_datetime(all_dates_.loc[:, 'date'])
        all_dates_.loc[:, 'settlement'] = pd.to_datetime(all_dates_.loc[:, 'settlement'])
        all_dates = pd.concat([all_dates, all_dates_])
        all_dates = all_dates.sort_values(by=['group', 'date', 'settlement', 'message'])\
            .drop_duplicates(subset=['group', 'date', 'settlement', 'message'])
        latest = all_dates.groupby(['date', 'message', 'group'])['settlement'].idxmax()
        all_dates = all_dates.loc[latest, :]
        all_dates.to_feather('settlement_dates.f')

    return all_dates


def read_and_write_settlement_dates():
    """
    Reads and writes settlement dates information to/from a pickle file.

    This function retrieves settlement dates information using P114_data_pull.get_dates(),
    converts date columns to datetime format, sorts the DataFrame by specified columns,
    and then writes the DataFrame to a pickle file named 'settlement_dates.p'.

    Returns:
        pd.DataFrame: DataFrame containing settlement dates information.
    """
    # Retrieve settlement dates information
    all_dates = P114_data_pull.get_dates()

    # Convert 'date' and 'settlement' columns to datetime format
    all_dates.loc[:, 'date'] = all_dates.loc[:, 'date'].apply(pd.to_datetime)
    all_dates.loc[:, 'settlement'] = all_dates.loc[:, 'settlement'].apply(pd.to_datetime)

    # Sort the DataFrame by 'group', 'date', and 'settlement'
    all_dates = all_dates.sort_values(by=['group', 'date', 'settlement'])

    # Write the DataFrame to a pickle file
    all_dates.to_pickle('settlement_dates.p')

    return all_dates


def filter_settlement_dates(start_date, end_date, settlement_dates, PROCESSED_FEEDS):
    """
    Filters settlement dates based on date range and processed feeds.

    Parameters:
        start_date (datetime.datetime): Start date of the date range.
        end_date (datetime.datetime): End date of the date range.
        settlement_dates (pd.DataFrame): DataFrame containing settlement dates information.
        PROCESSED_FEEDS (list): List of processed feed codes to consider.

    Returns:
        pd.DataFrame: Filtered settlement dates DataFrame.
    """
    # Filter settlement dates based on the specified date range
    settlement_dates = settlement_dates.loc[settlement_dates['date'].isin(pd.date_range(start_date, end_date)), :]

    # Filter settlement dates based on the list of processed feed codes
    settlement_dates = settlement_dates.loc[settlement_dates['message'].isin(PROCESSED_FEEDS), :]

    # Drop duplicates, keeping the last entry for each unique group and date
    return settlement_dates.sort_values(by=['group', 'date', 'message', 'settlement']).drop_duplicates(subset=['group', 'date', 'message'], keep='last')


def create_file_merge_list(start_date, end_date, settlement_dates, P114_INPUT_DIR):
    print('Adding files to queue for combine process')
    paths = pd.Series(glob.glob(f'{P114_INPUT_DIR}/*.gz'))
    # paths = list(filter(lambda f: '.gz' in f, os.listdir(cf.P114_INPUT_DIR)))
    file_dates = pd.to_datetime(paths.apply(os.path.basename).str.split('_', expand=True)[1])
    mask = (file_dates >= start_date) & (file_dates <= end_date)
    mask = mask & paths.str.contains('C0301')
    file_dates = pd.DataFrame({'date': file_dates, 'path': paths, 'file': paths.apply(os.path.basename)})[mask]

    target_files = set(settlement_dates['file'].apply(lambda x: x[0]).tolist())
    stored_files = file_dates['file']

    stored_target_files = set(stored_files).intersection(target_files)

    return [
        {'filename': file_date[1]['file'][0], 'p114_date': file_date[1]['date'].date()}
        for file_date in settlement_dates.loc[
            settlement_dates['file'].apply(lambda x: x[0]).isin(stored_target_files), ['date', 'file']].iterrows()]
