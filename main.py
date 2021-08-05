"""Main point of entry"""

from data_manager.data_download import run

if __name__ == '__main__':
    run(date=['2014-01-01', '2019-12-31'], type='demand', mode='parallel')
