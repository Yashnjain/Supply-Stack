from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.common.by import By
import time
from datetime import datetime
import numpy as np
import pandas as pd
import bu_snowflake
import os
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
import bu_alerts
from setting import GECKODRIVER_EXE_PATH, FIREFOX_PATH, CONFIG,ENV



def nationwide_so2() :    
    product = soup.find('div', {'class': 'view-content'})
    logger.info('Inside first table and fetched product')
    string = product.find('h3').text[2:].split(" ")[:-8]
    product_title = "SO2"
    t_date = product.find('h3').text.split(" ")[5:-2]
    trade_date = ' '.join(t_date)
    logger.info('Inside first table and trade date')
    rows = soup.find('tr',{'id' : 'tw-3697823'})
    data =[]
    cols = {
        0: "Term",
        1: "Bid_Price",
        2: "n/c",
        3: "Offer_Price",
        4: "n/c",
        5: "Actions"
    }
    temp = dict()
    logger.info('Temp dict created')
    for index, col in enumerate(rows.findAll(lambda tag: tag.name=='td')):
        temp[cols[index]] = np.nan if  col.text.strip() in ['', None] else col.text.strip()
    del temp['n/c']
    del temp['Actions']
    data.append(temp)

    df = pd.DataFrame(data)
    logger.info('Dataframe created of the data')
    df['PRODUCT'] = product_title
    df['TRADE_DATE'] = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    df['FLOW_DATE'] = df['Term'] + '-01-01'
    df.rename(columns = {'Term':'TERM','Bid_Price':'BID_PRICE', 'Offer_Price' :'OFFER_PRICE'}, inplace = True)
    new_col =["PRODUCT","TRADE_DATE","TERM","FLOW_DATE","BID_PRICE","OFFER_PRICE"]
    df = df[new_col]
    logger.info('Columns renamed')
    df = df.replace('[$]', '',regex=True)
    df['BID_PRICE'] = df['BID_PRICE'].astype(float)
    df['OFFER_PRICE'] = df['OFFER_PRICE'].astype(float)
    df['INSERT_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info('First dataframe completed and insert date inserted')
    return df



def csapr_nox():
    product = soup.find('div', {'class': 'view-content'})
    logger.info('Inside Second table and fetched product')
 
    for index, title in enumerate(product.findAll('h3')):
        if index == 1:
            string = title.text[2:].split(" ")[:-9]
            t_date = title.text.split(" ")[7:-2]
            break
        else:
            pass
    
    product_title = "CSAPR Annual NOx"
    trade_date = ' '.join(t_date)
    logger.info('Trade Date inserted')
    rows = soup.find('tr',{'id' : 'tw-3195435'})
    data =[]
    cols = {
        0: "Term",
        1: "Bid_Price",
        2: "n/c",
        3: "Offer_Price",
        4: "n/c",
        5: "Actions"
    }
    temp = dict()
    logger.info('Temporary dict created')
    for index, col in enumerate(rows.findAll(lambda tag: tag.name=='td')):
        temp[cols[index]] = np.nan if  col.text.strip() in ['', None] else col.text.strip()
    del temp['n/c']
    del temp['Actions']
    data.append(temp)

    df = pd.DataFrame(data)
    logger.info('Dataframe created of data')
    df['PRODUCT'] = product_title
    df['TRADE_DATE'] = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    df['FLOW_DATE'] = df['Term'] + '-01-01'
    df.rename(columns = {'Term':'TERM','Bid_Price':'BID_PRICE', 'Offer_Price' :'OFFER_PRICE'}, inplace = True)
    new_col =["PRODUCT","TRADE_DATE","TERM","FLOW_DATE","BID_PRICE","OFFER_PRICE"]
    df = df[new_col]
    logger.info('New columns renamed and inserted')
    df = df.replace('[$]', '',regex=True)
    logger.info('$ sign removed')
    df['BID_PRICE'] = df['BID_PRICE'].astype(float)
    df['OFFER_PRICE'] = df['OFFER_PRICE'].astype(float)
    df['INSERT_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info('Second dataframe completed and insert date inserted')
    return df



def csapr_so2_group1():
    product = soup.find('div', {'class': 'view-content'})
    logger.info('Inside Third table and fetched product')
    for index, title in enumerate(product.findAll('h3')):
        if index == 2:
            string = title.text[2:].split(" ")[:-8]
            t_date = title.text.split(" ")[7:-2]
            break
        else:
            pass

    product_title = "CSAPR SO2 Group 1"
    trade_date = ' '.join(t_date)
    logger.info('Trade date inserted')
    rows = soup.find('tr',{'id' : 'tw-3767677'})
    data =[]
    cols = {
        0: "Term",
        1: "Bid_Price",
        2: "n/c",
        3: "Offer_Price",
        4: "n/c",
        5: "Actions"
    }
    temp = dict()
    logger.info('Temporary dict created')
    for index, col in enumerate(rows.findAll(lambda tag: tag.name=='td')):
        temp[cols[index]] = np.nan if  col.text.strip() in ['', None] else col.text.strip()
    del temp['n/c']
    del temp['Actions']
    data.append(temp)

    df = pd.DataFrame(data)
    logger.info('Data inserted to Dataframe')
    df['PRODUCT'] = product_title
    df['TRADE_DATE'] = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    df['FLOW_DATE'] = df['Term'] + '-01-01'
    df.rename(columns = {'Term':'TERM','Bid_Price':'BID_PRICE', 'Offer_Price' :'OFFER_PRICE'}, inplace = True)
    new_col =["PRODUCT","TRADE_DATE","TERM","FLOW_DATE","BID_PRICE","OFFER_PRICE"]
    df = df[new_col]
    logger.info('New columns inserted')
    df = df.replace('[$]', '',regex=True)
    logger.info('$ sign removed')
    df['BID_PRICE'] = df['BID_PRICE'].astype(float)
    df['OFFER_PRICE'] = df['OFFER_PRICE'].astype(float)
    df['INSERT_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info('Third dataframe completed and insert date inserted')
    return df
    


def csapr_so2_group2():
    product = soup.find('div', {'class': 'view-content'})
    logger.info('Inside Fourth table and fetched product')
    for index, title in enumerate(product.findAll('h3')):
        if index == 3:
            string = title.text[2:].split(" ")[:-8]
            t_date = title.text.split(" ")[7:-2]
            break
        else:
            pass
    
    product_title = "CSAPR SO2 Group 2"
    trade_date = ' '.join(t_date)
    logger.info('Trade date inserted')
    rows = soup.find('tr',{'id' : 'tw-3767678'})
    data =[]
    cols = {
        0: "Term",
        1: "Bid_Price",
        2: "n/c",
        3: "Offer_Price",
        4: "n/c",
        5: "Actions"
    }
    temp = dict()
    for index, col in enumerate(rows.findAll(lambda tag: tag.name=='td')):
        temp[cols[index]] = np.nan if  col.text.strip() in ['', None] else col.text.strip()
    del temp['n/c']
    del temp['Actions']
    data.append(temp)

    df = pd.DataFrame(data)
    logger.info('Data inserted to Dataframe')
    df['PRODUCT'] = product_title
    df['TRADE_DATE'] = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    df['FLOW_DATE'] = df['Term'] + '-01-01'
    df.rename(columns = {'Term':'TERM','Bid_Price':'BID_PRICE', 'Offer_Price' :'OFFER_PRICE'}, inplace = True)
    new_col =["PRODUCT","TRADE_DATE","TERM","FLOW_DATE","BID_PRICE","OFFER_PRICE"]
    df = df[new_col]
    logger.info('New columns inserted')
    df = df.replace('[$]', '',regex=True)
    logger.info('$ sign removed')
    df['BID_PRICE'] = df['BID_PRICE'].astype(float)
    df['OFFER_PRICE'] = df['OFFER_PRICE'].astype(float)
    df['INSERT_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info('Fourth dataframe completed and insert date inserted')
    return df



def csapr_NOx_group2():
    url1 = source_url.split(';')[2]
    driver.get(url1)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    product = soup.find('div', {'class': 'view-content'})
    logger.info('Inside fifth table and fetched product')
    dat =  soup.find('form', {'id': 'calendar_date_form'})
    date =  dat.find('input', {'id': 'calendar_date'})
    trade_date = date.get_attribute_list('value')[0]
    trade_date = datetime.strptime(trade_date,"%d-%m-%Y")

    product_title = "CSAPR Seasonal NOx Group 2"
    # trade_date = ' '.join(t_date)
    logger.info('Trade date inserted')
    data =[]
    cols = {
        0: "Term",
        1: "Bid_Price",
        2: "n/c",
        3: "Offer_Price",
        4: "n/c",
        5: "Actions"
    }
    temp = dict()
    logger.info('Temporary dict created')
    for index, col in enumerate(product.findAll(lambda tag: tag.name=='td')):
        temp[cols[index]] = np.nan if  col.text.strip() in ['', None] else col.text.strip()
    del temp['n/c']
    del temp['Actions']
    data.append(temp)

    df = pd.DataFrame(data)
    logger.info('Data inserted to Dataframe')
    df['PRODUCT'] = product_title
    df['TRADE_DATE'] = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    df['FLOW_DATE'] = df['Term'] + '-01-01'
    df.rename(columns = {'Term':'TERM','Bid_Price':'BID_PRICE', 'Offer_Price' :'OFFER_PRICE'}, inplace = True)
    new_col =["PRODUCT","TRADE_DATE","TERM","FLOW_DATE","BID_PRICE","OFFER_PRICE"]
    df = df[new_col]
    logger.info('New columns inserted')
    df = df.replace('[$]', '',regex=True)
    logger.info('$ sign removed')
    df = df.replace('[,]', '',regex=True)
    logger.info(', sign removed')
    df['BID_PRICE'] = df['BID_PRICE'].astype(float)
    df['OFFER_PRICE'] = df['OFFER_PRICE'].astype(float)
    df['INSERT_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info('Fifth dataframe completed and insert date inserted')
    return df


def csapr_NOx_group3():
    url2 = source_url.split(';')[3]
    driver.get(url2)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    product = soup.find('div', {'class': 'view-content'})
    logger.info('Inside Last table and fetched product')
    dat =  soup.find('form', {'id': 'calendar_date_form'})
    date =  dat.find('input', {'id': 'calendar_date'})
    trade_date = date.get_attribute_list('value')[0]
    trade_date = datetime.strptime(trade_date,"%d-%m-%Y")

    product_title = "CSAPR Seasonal NOx Group 3"
    # trade_date = ' '.join(t_date)
    logger.info('Trade date inserted')
    data =[]
    cols = {
        0: "Term",
        1: "Bid_Price",
        2: "n/c",
        3: "Offer_Price",
        4: "n/c",
        5: "Actions"
    }
    temp = dict()
    logger.info('Temporary dict created')
    for index, col in enumerate(product.findAll(lambda tag: tag.name=='td')):
        temp[cols[index]] = np.nan if  col.text.strip() in ['', None] else col.text.strip()
    del temp['n/c']
    del temp['Actions']
    data.append(temp)

    df = pd.DataFrame(data)
    logger.info('Data inserted to Dataframe')
    df['PRODUCT'] = product_title
    df['TRADE_DATE'] = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    df['FLOW_DATE'] = df['Term'] + '-01-01'
    df.rename(columns = {'Term':'TERM','Bid_Price':'BID_PRICE', 'Offer_Price' :'OFFER_PRICE'}, inplace = True)
    new_col =["PRODUCT","TRADE_DATE","TERM","FLOW_DATE","BID_PRICE","OFFER_PRICE"]
    df = df[new_col]
    logger.info('New columns inserted')
    df = df.replace('[$]', '',regex=True)
    logger.info('$ sign removed')
    df = df.replace('[,]', '',regex=True)
    logger.info(', sign removed')
    df['BID_PRICE'] = df['BID_PRICE'].astype(float)
    df['OFFER_PRICE'] = df['OFFER_PRICE'].astype(float)
    df['INSERT_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info('Last dataframe completed and insert date inserted')
    return df

        
if __name__ == "__main__":
    try:
        print(datetime.now())
        log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'emission_evo_log.txt'
        
        logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] - %(message)s',
        filename=log_file_location)

        job_id=np.random.randint(1000000,9999999)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Loading the webdriver
        
        binary = FirefoxBinary(FIREFOX_PATH)
        driver = webdriver.Firefox(firefox_binary=binary, executable_path=GECKODRIVER_EXE_PATH)
        logger.info('Driver loaded')
        pd.options.display.float_format = '{:,.2f}'.format


        # login credentials
        username=(CONFIG['USERNAME'])
        password=(CONFIG['PASSWORD'])
        processname = (CONFIG['PROJECT_NAME'])
        databasename = (CONFIG['DATABASE']) 
        schemaname = (CONFIG['TABLE_SCHEMA'])
        tablename = (CONFIG['TABLE_NAME'])
        source_url = CONFIG['SOURCE_URL']
        url = source_url.split(';')[1]
        process_owner=CONFIG['IT_OWNER']

        driver.get(url)
        login = driver.find_element(By.XPATH ,"/html/body/div[1]/div[2]/div[2]/ul/li/a" )
        login.click()
        driver.find_element(By.XPATH,'//*[@id="edit-name"]').send_keys(username)
        driver.find_element(By.XPATH,'//*[@id="edit-pass"]').send_keys(password)
        entry_page = driver.find_element(By.XPATH,'//*[@id="edit-submit"]').click()
        time.sleep(10)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        rows =0
        
        df = nationwide_so2()
        logger.info('First dataframe received')
        df1 = csapr_nox()
        logger.info('Second dataframe received')
        df2 = csapr_so2_group1()
        logger.info('Third dataframe received')
        df3 = csapr_so2_group2()
        logger.info('fourth dataframe received')
        df4 = csapr_NOx_group2()
        logger.info('fifth dataframe received')
        df5 = csapr_NOx_group3()
        logger.info('Last dataframe received')
        final_df = pd.concat([df,df1,df2,df3,df4,df5])
        logger.info('Final dataframe created')
        if ENV == 'PROD':
                engine = bu_snowflake.get_engine(warehouse='BUPOWER_INDIA_WH',
                                        role=f"OWNER_{CONFIG['DATABASE']}",
                                        schema=CONFIG['TABLE_SCHEMA'],
                                        database=CONFIG['DATABASE'])
        else:
                engine = bu_snowflake.get_engine(
                                        username="pakhi.laad@biourja.com",
                                        password="23@Pakhi",
                                        warehouse='BUPOWER_INDIA_WH',
                                        role="OWNER_POWERDB_DEV",
                                        schema="PMACRO",
                                        database="POWERDB_DEV")
        cs = engine.connect()
        logger.info('Connection created with Snowflake')
        try:
            cs.execute(f"Delete FROM {CONFIG['TABLE_NAME']} WHERE TRADE_DATE ='{final_df.iloc[0]['TRADE_DATE']}'")
            logger.info('Duplicates Removed')
            final_df.to_sql(f"{CONFIG['TABLE_NAME']}",cs,index = False,method = pd_writer,if_exists='append')
            logger.info('New data inserted to snowflake')
            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
            logging.info('Execution Done')
            engine.dispose()
            logging.info(' Engine Disposed ---- end')
            bu_alerts.send_mail(
                    receiver_email = CONFIG['EMAIL_LIST'],
                    mail_subject ='JOB SUCCESS - {}'.format(tablename),
                    mail_body = '{} completed successfully, Attached logs'.format(tablename),
                    attachment_location = log_file_location
                )
        except Exception as e:
            print(f"error occurred : {e}")
            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            bu_alerts.bulog(process_name=processname,database=databasename,status='Failed',table_name=tablename, row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner) 
            bu_alerts.send_mail(
                receiver_email = CONFIG['EMAIL_LIST'],
                mail_subject = 'JOB FAILED - {}'.format(tablename),
                mail_body='{} failed during execution, Attached logs'.format(tablename),
                attachment_location = log_file_location
            )
            logger.info(f'Error : {e}')
    except Exception as e:
        print(f"error occurred : {e}")
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=processname,database=databasename,status='Failed',table_name=tablename, row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner) 
        bu_alerts.send_mail(
                receiver_email = CONFIG['EMAIL_LIST'],
                mail_subject = 'JOB FAILED - {}'.format(tablename),
                mail_body='{} failed during execution, Attached logs'.format(tablename),
                attachment_location = log_file_location
            )
        logger.info(f'Error : {e}')
    finally:
        print(datetime.now())
        logger.info('Process completed')
        