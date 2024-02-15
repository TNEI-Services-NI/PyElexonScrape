import os
import json

DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# P114 downloads
P114_LIST_URL = r'https://downloads.elexonportal.co.uk/p114/list?key={}&date={}'
P114_DOWNLOAD_URL = r'https://downloads.elexonportal.co.uk/p114/download?key={}&filename={}'

B1610_LIST_URL = r'https://downloads.elexonportal.co.uk/B1610/download?key={}&filename={}'
B1610_DOWNLOAD_URL = r'https://api.bmreports.com/BMRS/B1610/V2?APIKey={}&SettlementDate={}&Period={}&ServiceType=csv'

P114_INPUT_DIR = BASE_DIR+r"/data_manager/data_/P114/gz/"
B1610_INPUT_DIR = BASE_DIR+r"/data_manager/data_/B1610/csv/"

B1610_PROCESSED_DIR = BASE_DIR+r"/data_manager/data_/B1610/"

ELEXON_KEY = json.load(open(f'{DIR}/config.json'))["ELEXON_KEY"]

# TARGET_MESSAGES = ['AGV', 'AGP', 'MPD', 'GP9', 'GMP', 'ABV', 'ABP']
TARGET_MESSAGES = ['MPD', 'GP9', 'GMP']

PROCESSED_FEEDS = ['C0301']

MAX_POOLS = 1

request_interval_mins = 0
request_interval_secs = 5

pull_pools = 1

reverse = False

