"""Main point of entry"""

from data_manager.data_download import run

if __name__ == '__main__':
    # run(date=['2014-01-01', '2017-12-31'], type='generation', mode='parallel')
    # run(date=['2015-03-09', '2017-12-31'], type='generation', mode='parallel')
    # run(date=['2015-07-30', '2017-12-31'], type='generation', mode='parallel')
    # run(date=['2017-07-10', '2017-12-31'], type='generation', mode='parallel')
    # run(date=['2018-01-01', '2019-12-31'], type='generation', mode='parallel')
    run(date=['2010-01-01', '2014-12-31'], type='demand', mode='parallel')
    # run(date=['2010-01-01', '2014-12-31'], type='demand', mode='parallel')
