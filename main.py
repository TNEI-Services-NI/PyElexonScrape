"""Main point of entry"""

from data_manager.data_download import run

if __name__ == '__main__':
    run(date=['2015-01-01', '2020-12-01'])