"""Main point of entry"""

from data_manager.data_download import run_parallel, run

if __name__ == '__main__':
    # run(date=['2015-01-01', '2020-12-01'])
    # run_parallel(date=['2010-04-30', '2020-12-01'])
    run_parallel(date=['2020-11-30', '2020-12-01'])
    # run_parallel(date=['2010-01-01', '2014-12-31'])