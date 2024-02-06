from datetime import date, datetime, timedelta
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import logging
import bu_alerts
import os


def download_data(url_list,files_location,executable_path):
    try:
        print("Inside download_data function")
        logger.info("Inside download_data function")
        options = Options()
        logger.info("Options object created")
        mime_types = ['application/pdf', 'text/plain', 'application/vnd.ms-excel',
            'text/csv', 'application/csv', 'text/comma-separated-values',
            'application/download', 'application/octet-stream', 'binary/octet-stream',
            'application/binary', 'application/x-unknown','attachment/csv',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir", files_location)
        options.set_preference("browser.helperApps.neverAsk.saveToDisk",",".join(mime_types))
        options.set_preference("browser.helperApps.neverAsk.openFile", """application/pdf,
            application/octet-stream, application/x-winzip, application/x-pdf, application/x-gzip""")
        options.set_preference("pdfjs.disabled", True)
        logger.info("Executable path for geckodriver passed successfully")

        browser = webdriver.Firefox(options=options, executable_path=executable_path)
        browser.set_page_load_timeout(9)
        for url in url_list:
            try:
                logger.info("Website opened and downloading pdf")
                browser.get(url)
                
            except TimeoutException as e:
                pass
            
    except Exception as e:
        logger.exception(e)
        raise e
    finally:
        try:      
            browser.quit()
            logger.info("Browser quit successfully")
        except Exception as e:
            logger.exception(e)
            logger.exception("No browser is opened to quit")


def download_hist(files_location,executable_path):
    url = "https://www.aar.org/wp-content/uploads/{}/{}/{}-railtraffic.pdf"

    dt = '2018-09-26'
    main_date = datetime.strptime(dt,'%Y-%m-%d')
    req_year = main_date.year
    req_month = f"{main_date.month:02d}"
    req_date = str(main_date.date())
    url_list = []
    while main_date <= datetime.today():
        req_year = main_date.year
        req_month = f"{main_date.month:02d}"
        req_date = str(main_date.date())
        print(url.format(req_year,req_month,req_date))
        url_list.append(url.format(req_year,req_month,req_date))

        main_date += timedelta(days=7)
        
    download_data(url_list,files_location,executable_path)

    return len(url_list)

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logfilename = bu_alerts.add_file_logging(logger,process_name= 'aar_hist_data')

    files_location = os.getcwd() + '\\download_rail'
    executable_path = os.getcwd() + "\\geckodriver\\geckodriver.exe"
    files = download_hist(files_location,executable_path)

    print(f"{files} number of files downloaded successfully")
