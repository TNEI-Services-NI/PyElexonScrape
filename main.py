"""Main point of entry"""

from data_manager.data_download import run

if __name__ == '__main__':
    run(date=['2014-01-01', '2017-12-31'], type='generation', mode='parallel')
    run(date=['2020-01-01', '2021-08-09'], type='generation', mode='parallel')
    run(date=['2020-01-01', '2021-08-09'], type='demand', mode='parallel')
