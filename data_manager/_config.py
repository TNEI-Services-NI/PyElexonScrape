import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# P114 downloads
P114_LIST_URL = r'https://downloads.elexonportal.co.uk/p114/list?key={}&date={}&filter=C0301'
P114_DOWNLOAD_URL = r'https://downloads.elexonportal.co.uk/p114/download?key={}&filename={}'

P114_INPUT_DIR = BASE_DIR + '/data_manager/data/'

ELEXON_KEY = 'nvxyve1ubai87gz'

request_interval_mins = 0