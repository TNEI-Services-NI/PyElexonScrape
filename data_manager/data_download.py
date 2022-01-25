import datetime as dt
import multiprocessing as mp
import os.path
import shutil
import datetime
import time
import pandas as pd

import data_manager._config as cf

import data_manager.P114.data_pull as P114_data_pull
import data_manager.P114.util as p114_util

import data_manager.B1610.data_pull as B1610_data_pull
import data_manager.B1610.util as B1610_util


def run_demand_parallel(*args, **options):
    """downloads P114 data for specific date range,
    expected 2 arguments of form ['yyyy-mm-dd', 'yyyy-mm-dd']"""
    if os.path.isfile(cf.P114_INPUT_DIR + "gsp_demand.csv"):
        os.remove(cf.P114_INPUT_DIR + "gsp_demand.csv")

    if not os.path.exists(cf.P114_INPUT_DIR):
        os.makedirs(cf.P114_INPUT_DIR)

    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])

    # dates = pd.read_csv('/'.join([p114_util.DIR, 'missing_dates.csv']), index_col=0)
    # start_date = pd.to_datetime(dates.iloc[0, 0])
    # end_date = pd.to_datetime(dates.iloc[-1, 0])

    start_dates = [dt.datetime(*(start_date + pool * ((end_date - start_date) / cf.pull_pools)).date().timetuple()[:3])
                   for
                   pool in range(cf.pull_pools)]
    end_dates = [start_date_ + dt.timedelta(days=-1) for start_date_ in start_dates[1:]] + [end_date]

    if cf.reverse:
        temp = end_dates
        end_dates = start_dates
        start_dates = temp
        del temp

    dates = list(zip(start_dates, end_dates))

    t0 = dt.datetime.now()

    q = mp.Queue()
    status_q = mp.Queue()
    if not os.path.exists(cf.P114_INPUT_DIR.replace('/gz/', "/done/")):
        os.makedirs(cf.P114_INPUT_DIR.replace('/gz/', "/done/"))

    # cf.P114_INPUT_DIR = cf.P114_INPUT_DIR.replace('gz', 'non_target')
    if cf.pull_pools == 0:
        print('Adding files to queue for combine process')
        files = list(filter(lambda f: '.gz' in f, os.listdir(cf.P114_INPUT_DIR)))
        file_dates = [dt.datetime.strptime(filename.split('_')[1], '%Y%m%d') for filename in files]
        file_dates = list(zip(files, file_dates))
        file_dates = list(filter(lambda x: (x[1] >= start_date) & (x[1] <= end_date), file_dates))
        # file_dates = list(filter(lambda x: ('C0291' in x[0]) | ('C0301' in x[0]) | ('C0421' in x[0]), file_dates))
        file_dates = list(filter(lambda x: ('C0301' in x[0]), file_dates))
        file_dates = list(sorted(file_dates))
        target_files = list(map(lambda x: x[0], file_dates))
        non_target_files = list(set(files) - set(target_files))
        for non_target_file in non_target_files:
            if not os.path.exists(cf.P114_INPUT_DIR.replace('/gz/', "/non_target/")):
                os.makedirs(cf.P114_INPUT_DIR.replace('/gz/', "/non_target/"))
            shutil.move(cf.P114_INPUT_DIR + '/{}'.format(non_target_file),
                        cf.P114_INPUT_DIR.replace('/gz/', "/non_target/") + '{}'.format(non_target_file))

        [q.put(
            {'filename': file_date[0], 'p114_date': file_date[1]})
            for file_date in file_dates]
        print('\tDone')

    # workers = [mp.Process(target=P114_data_pull.pull_data_parallel, args=(date_[0], date_[1], t0, q, status_q,))
    #            for date_ in dates]
    #
    # # Execute workers
    # for p in workers:
    #     p.start()
    # # Add worker to queue and wait until finished
    # for p in workers:
    #     p.join()

    workers = [mp.Process(target=p114_util.combine_data, args=(q, pool,)) for pool in
               range(0, cf.MAX_POOLS)]

    # Execute workers
    for p in workers:
        p.start()
    # Add worker to queue and wait until finished
    for p in workers:
        p.join()
    #
    # p114_util.merge_data(cf.MAX_POOLS)


def run_demand(*args, **options):
    """downloads P114 data for specific date range,
    expected 2 arguments of form ['yyyy-mm-dd', 'yyyy-mm-dd']"""
    if os.path.isfile(cf.P114_INPUT_DIR + "gsp_demand.csv"):
        os.remove(cf.P114_INPUT_DIR + "gsp_demand.csv")
    if not os.path.isdir(cf.P114_INPUT_DIR):
        os.makedirs(cf.P114_INPUT_DIR)
    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])
    date = start_date
    t0 = dt.datetime.now()
    completed_requests = 0
    while date <= end_date:
        if ((dt.datetime.now() - t0).seconds / 60) - (cf.request_interval_mins * completed_requests) > 0:
            print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
            print("downloading data for " + '{:%Y-%m-%d}'.format(date))
            P114_data_pull.pull_p114_date_files(date)
            date += dt.timedelta(days=-1 if cf.reverse else 1)
            completed_requests += -1 if cf.reverse else 1


def run_generation(*args, **options):
    """downloads P114 data for specific date range,
    expected 2 arguments of form ['yyyy-mm-dd', 'yyyy-mm-dd']"""
    if os.path.isfile(cf.B1610_INPUT_DIR + "gsp_generation.csv"):
        os.remove(cf.B1610_INPUT_DIR + "gsp_generation.csv")
    if not os.path.isdir(cf.B1610_INPUT_DIR):
        os.makedirs(cf.B1610_INPUT_DIR)
    start_date = dt.datetime(*[int(x) for x in options['date'][0].split('-')[:3]])
    end_date = dt.datetime(*[int(x) for x in options['date'][1].split('-')[:3]])
    date = start_date
    t0 = dt.datetime.now()
    completed_requests = 0
    while date <= end_date:
        print('{:%Y-%m-%d %H:%M:%S}'.format(dt.datetime.now()))
        print("downloading data for " + '{:%Y-%m-%d}'.format(date))
        date_string = datetime.datetime.strftime(date, '%Y-%m-%d')
        for settlement_period in range(1, 50 + 1):
            filename = '_'.join([date_string, str(settlement_period)]) + '.csv'
            if os.path.isfile(cf.B1610_INPUT_DIR + filename) and not 'overwrite' in options.keys():
                continue
            wait = ((dt.datetime.now() - t0).seconds) - (cf.request_interval_secs * completed_requests + 1)
            time.sleep(-1*wait) if wait < 0 else None
            if ((dt.datetime.now() - t0).seconds) - (cf.request_interval_secs * completed_requests) > 0:
                B1610_data_pull.get_B1610_data(date_string, filename, settlement_period)
                completed_requests += -1 if cf.reverse else 1
        date += dt.timedelta(days=-1 if cf.reverse else 1)

    B1610_util.merge_data()


def run(*args, **options):
    if 'mode' in options.keys():
        if options['mode'] == 'parallel':
            if 'type' in options.keys():
                if options['type'] == 'demand':
                    run_demand_parallel(**options)
                elif options['type'] == 'generation':
                    run_generation(*args, **options)
        else:
            if 'type' in options.keys():
                if options['type'] == 'demand':
                    run_demand(*args, **options)
                elif options['type'] == 'generation':
                    run_generation(*args, **options)

    else:
        if 'type' in options.keys():
            if options['type'] == 'demand':
                run_demand(*args, **options)
            elif options['type'] == 'generation':
                run_generation(*args, **options)


if __name__ == '__main__':
    import pandas as pd
    data = pd.read_csv(cf.P114_INPUT_DIR.replace('gz/', 'MPD.csv'), header=[0,1,2])
    data.loc[:, data.columns[4]] = data.loc[:, data.columns[4]].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    print()



    n_uncompleted = len(os.listdir(cf.P114_INPUT_DIR))
    n_completed = len(os.listdir(cf.P114_INPUT_DIR.replace('gz/', 'done/')))
    print(n_completed / (n_completed + n_uncompleted))


