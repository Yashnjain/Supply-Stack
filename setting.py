from bu_config import get_config
from bu_snowflake import get_connection
import os 



ENV = 'PROD'



MAIN_CONFIG = get_config('SUPPLY_STACK','EVOID_SOX_NOX_NUCF_FORWARDS')

if ENV == 'PROD':
    conn = get_connection(role=f"OWNER_{MAIN_CONFIG['DATABASE']}",database=MAIN_CONFIG['DATABASE'],schema=MAIN_CONFIG['TABLE_SCHEMA'])
    GECKODRIVER_EXE_PATH = f"{os.getcwd()}\\geckodriver.exe"
    FIREFOX_PATH = r"C:\\Program Files\\Mozilla Firefox\\Firefox.exe"
    CONFIG = MAIN_CONFIG
else:
    CONFIG = {'DATABASE': 'POWERDB_DEV', 
                'TABLE_SCHEMA': 'PMACRO', 
                'USERNAME':MAIN_CONFIG['USERNAME'],
                'PASSWORD':MAIN_CONFIG['PASSWORD'],
                'PROJECT_NAME':'SUPPLY_STACK',
                'TABLE_NAME': 'EVOID_SOX_NOX_NUCF_FORWARDS_22_09_2022',
                'SOURCE_URL': 'https://evoid.evomarkets.com/user/login;https://evoid.evomarkets.com/users/2259/daily_market_summary;https://evoid.evomarkets.com/market/11/3234055;https://evoid.evomarkets.com/market/11/3234056',
                'EMAIL_LIST':'pakhi.laad@biourja.com',
                'IT_OWNER':'Pakhi Laad'
                
                }
    conn = get_connection(role=f"OWNER_{CONFIG['DATABASE']}",database=CONFIG['DATABASE'],schema=CONFIG['TABLE_SCHEMA'])
    GECKODRIVER_EXE_PATH = f"{os.getcwd()}\\geckodriver.exe"
    # FIREFOX_PATH = r"C:\\Users\\{}\\AppData\\Local\\Mozilla Firefox\\firefox.exe".format(os.getlogin())
    # GECKODRIVER_EXE_PATH = r"C:\Users\Pakhi.laad\Documents\WebScrapping\geckodriver.exe"
    FIREFOX_PATH = r"C:\\Program Files\\Mozilla Firefox\\Firefox.exe" 

