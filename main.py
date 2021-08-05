"""Main point of entry"""

from data_manager.data_download import run

if __name__ == '__main__':
    # run(date=['2015-01-01', '2020-12-01'])
    # run(date=['2014-03-29', '2014-03-31'], type='demand', mode='parallel')
    # run(date=['2014-01-01', '2019-12-31'], type='demand')
    run(date=['2014-01-01', '2019-12-31'], type='demand', mode='parallel')
    # run_parallel(date=['2020-11-30', '2020-12-01'])
    # run_parallel(date=['2010-01-01', '2014-12-31'])