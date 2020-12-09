"""Main point of entry"""

from data_manager.data_download import run

if __name__ == '__main__':
    run(date=['2012-01-09', '2020-12-01'])