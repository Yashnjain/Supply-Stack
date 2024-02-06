from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import time,sys
from datetime import date,datetime
import logging
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
import os
from bu_config import get_config
import bu_alerts
import numpy as np
import pandas as pd
import bu_snowflake
from  bu_snowflake import get_connection,get_engine
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import functools

def get_last_modified_date(selected_file_name):
    logger.info("Inside get_last_modified_date function")

    """
    Retrieves a list of the column headers of the table.

    Args:
        database : the database in question
        schemename : the schema in question
        tablename : the table in question

    Returns:
        cols_in_db : the list of the column headers of the table
        or
        [] : if error arises when retrieving columns or table doesn't exist
    """
    logging.info("Inside get_table_columns function")
    sql = '''
    select max(LAST_MODIFIED) from {} where FILE_NAME='{}'
    '''.format(database_name+"."+schema_name+"."+table_name,selected_file_name)


    try:
        logging.info(f"connecting with snowflake Inside get_last_modified_date function for {table_name}")
        # Connect to Snowflake
        cs = get_connection(role=f'OWNER_{database_name}',database=database_name,schema=schema_name).cursor()
        cs.execute("USE WAREHOUSE QUANT_WH")
        cs.execute(sql)
        logging.info(f"connecting with snowflake Succesfull Inside get_last_modified_date function for {table_name}")
        df = pd.DataFrame.from_records(iter(cs))
        cs.close()
        if str(df[0][0])!='None':
            send_value=str(df[0][0]).split(' ')[0]
        else:
            send_value='None'        
        logging.info(f"max LAST_modified date fetched")
        return send_value
    except Exception as e:
        print(f"Exception caught {e} during execution inside get_last_modified_date function")
        logging.exception(f'Exception caught during execution in get_last_modified_date function : {e}')
        raise e
  
def remove_existing_files(files_location):
    """_summary_

    Args:
        files_location (_type_): _description_

    Raises:
        e: _description_
    """           
    logger.info("Inside remove_existing_files function")
    try:
        files = os.listdir(files_location)
        if len(files) > 0:
            for file in files:
                os.remove(files_location + "\\" + file)
            logger.info("Existing files removed successfully")
        else:
            print("No existing files available to reomve")
        print("Pause")
    except Exception as e:
        logger.info(e)
        raise e


def login_and_download(): 
    logger.info("Inside login_and_download function") 
    '''This function downloads log in to the website'''
    try:
        no_of_rows=0
        logger.info('Accesing website')
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.dir', files_location)
        profile.set_preference('browser.download.useDownloadDir', True)
        profile.set_preference('browser.download.viewableInternally.enabledTypes', "")
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk','Portable Document Format (PDF), application/pdf')
        profile.set_preference('pdfjs.disabled', True)
        logger.info('Adding firefox profile')
        driver=webdriver.Firefox(executable_path='geckodriver.exe',firefox_profile=profile)#GeckoDriverManager().install(),firefox_profile=profile)
        logger.info("Getting URL") 
        driver.get(source_url)
        time.sleep(45)      
        logger.info("selecting EMISSION- HOURLY")
        emissions=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/div[1]/div/select/option[5]'))).click()
        time.sleep(15)      
        hourly=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/div[1]/div/select[2]/option[2]'))).click()
        time.sleep(15) 
        logger.info("Accessing search bar and clearing it")
        search_key=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="bulk-data-files-table-search"]')))
        search_key.clear()
        time.sleep(5)      
        # file_selected=f'emissions-hourly-{today_year}'
                # file_selected=f'emissions-hourly-{today_year}'
        years_to_be_checked=['2022','2023']
        file_selected=f'emissions-hourly-{today_year}'
        search_key.send_keys(file_selected)
        search_button=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'.usa-search__submit-text')))
        search_button.click()
        # select_all=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/div[2]/\
        #                                                                                                 div[2]/div/div[2]/div/div[1]/div/div/div[1]/div/div[1]/input')))
        # select_all.click()
        time.sleep(5)
        files_check_=driver.find_elements(By.XPATH,'/html/body/div/div/div/main/div/div[2]/div[2]/div/div[2]/div/div/div/div/div/span')
        if files_check_[0].text!='There are no records to display':
            logger.info(f"files present for year: {today_year}")
            years_to_be_checked.append(today_year)

        else:
            print(f'No values for year:{today_year} and it showed {files_check_[0].text}')    
            logger.info(f'No values for year:{today_year} and it showed {files_check_[0].text}')   
        for year_selected in years_to_be_checked:
            file_selected=f'emissions-hourly-{year_selected}'

            search_key.clear()
            # file_selected=f'emissions-hourly-2022'      #uncomment this line for testing
            search_key.send_keys(file_selected)
            logger.info("Accessing search button and clicking it")
            # search_button=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'.usa-search__submit-text')))
            search_button.click()
            time.sleep(10)   
            select_all=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/div[2]/\
                                                                                                        div[2]/div/div[2]/div/div[1]/div/div/div[1]/div/div[1]/input')))
            select_all.click()
            time.sleep(5)
            logger.info("Accessing total count of selected files")
            total_select_count=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/\
                                                div/div/div/main/div/div[2]/div[2]/div/div[1]/div[1]'))).text
            time.sleep(2)
            select_all.click()
            total_files=int(total_select_count.split(':')[1].strip())
            time.sleep(5)
            scroll_down=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/\
                                                    main/div/div[2]/div[2]/div/div[2]/div/div[2]')))
            driver.execute_script("arguments[0].scrollIntoView();", scroll_down)


            next_button=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/div[2]/\
                                            div[2]/div/div[2]/div/div[2]/nav/div[1]/select')))
            driver.execute_script("arguments[0].scrollIntoView();", next_button)
            next_button.click()
            time.sleep(5)
            WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/\
                                        div[2]/div[2]/div/div[2]/div/div[2]/nav/div[1]/select/option[4]'))).click()
            time.sleep(5)
            logger.info("Getting total elements on page")
            page_elements=driver.find_elements(By.XPATH,'/html/body/div/div/div/main/div/div[2]/div[2]/div/div[2]/div/div[1]/div/div/div[2]')[0]
            text_page_elements=page_elements.text
            time.sleep(5)
            file_name_need=[]
            for i in text_page_elements.split('\n'):
                if file_selected in i:
                    file_name_need.append(i)
            for key in file_name_need[0:total_files+1]:
                logger.info(f"-------------Process started for FILE_NAME: {key}-----------------------")
                search_key.clear()
                search_key.send_keys(key)
                time.sleep(3)
                search_button.click()
                time.sleep(3)
                logger.info("Getting modified_date")
                date_modified=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/div/div/div/main/div/div[2]/div[2]/div/div[2]/\
                                                                                                            div/div[1]/div/div/div[2]/div[1]/div[3]/div'))).text
                selected_file_name=key.split('.')[0]
                logger.info("Checking the max maodified date of the file from sf.")
                max_last_modified_date = get_last_modified_date(selected_file_name)
                if (max_last_modified_date== 'None') or (date_modified>max_last_modified_date):
                    logger.info(f"{selected_file_name} file values are not there in sf.")
                    logger.info(f"--------DOWNLOADING & UPLOADING process Statrts for :{selected_file_name} ------------------------")
                    try:
                        time.sleep(10)
                        select_all.click()
                    except Exception as e:
                        logger.info(f"Error in Downloading {selected_file_name} file")
                        logger.info(f"ERROR:{e}")
                        raise e
                    logger.info(f"Downloading {selected_file_name} file")
                    download_button=WebDriverWait(driver, 150, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'.flex-end')))
                    time.sleep(5)
                    if download_button.is_enabled():
                        download_button.click()
                    else:
                        download_button=WebDriverWait(driver, 150, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/\
                                                                        main/div/div[2]/div[2]/div/div[1]/button')))
                        download_button.click()
                    time.sleep(5) 
                    counter=0
                    while True:
                        if len((os.listdir(files_location)))==1:
                            break
                        else:
                            time.sleep(10)
                            counter+=1
                    downloaded_files= os.listdir(files_location)
                    downloaded_file_name=downloaded_files[0].split('.')[0]
                    logger.info(f"Creating df for the file:{selected_file_name}")
                    df=pd.read_csv(files_location+'\\'+downloaded_files[0])
                    df1=df[['State','Facility Name','Facility ID','Unit ID','Date','Hour','Operating Time','Gross Load (MW)','Steam Load (1000 lb/hr)',
                            'SO2 Mass (lbs)','SO2 Mass Measure Indicator','SO2 Rate (lbs/mmBtu)','SO2 Rate Measure Indicator','NOx Rate (lbs/mmBtu)',
                            'NOx Rate Measure Indicator','NOx Mass (lbs)','NOx Mass Measure Indicator','CO2 Mass (short tons)','CO2 Mass Measure Indicator',
                            'CO2 Rate (short tons/mmBtu)','CO2 Rate Measure Indicator','Heat Input (mmBtu)']]
                    renamed_columns=["STATE","PLANT_NAME","EIA_PLANTID","EIA_GENERATORID","FLOW_DATE","FLOW_HOUR","OPERATING_HOUR","GROSS_LOAD_MW",\
                                "STEAM_LOAD_1000LB_PER_HR","SO2_MASS_LBS","SO2_MASS_MEASURE_FLG","SO2_RATE_LBS_PER_MMBTU","SO2_RATE_MEASURE_FLG",\
                                    "NOX_RATE_LBS_PER_MMBTU","NOX_RATE_MEASURE_FLG","NOX_MASS_LBS","NOX_MASS_MEASURE_FLG",\
                                        "CO2_MASS_TONS","CO2_MASS_MEASURE_FLG","CO2_RATE_TONS_PER_MMBTU","CO2_RATE_MEASURE_FLG",\
                                            "HEAT_INPUT_MMBTU"]
                    df1.columns=renamed_columns
                    print(df1)
                    df1['EIA_GENERATORID']=df1['EIA_GENERATORID'].astype(str)
                    df1['INSERT_DATE']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df1['UPDATE_DATE']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df1.insert(loc=0,column='FILE_NAME',value=downloaded_file_name)
                    df1.insert(loc=1,column='LAST_MODIFIED',value=date_modified)
                    logger.info(f"dis selecting the file {selected_file_name}  in webpage")
                    select_all.click()
                    os.remove(files_location+'\\'+downloaded_files[0])
                    logger.info(f"Uploading file: {selected_file_name}  into sf")
                    no_of_rows+=upload_to_sf(df1,date_modified,downloaded_file_name) 
                    logger.info(f"------------------------Uploading file: {selected_file_name}  completed------------------------------ ")
                    print(f"------------------------Uploading file: {selected_file_name}  completed------------------------------ ")
                else:
                    logger.info(f"Data Already got inserted for file:{selected_file_name} and LAST_UPDATED on : {max_last_modified_date} and NO NEW DATA FOUND")
        return no_of_rows      
    except Exception as e:
        print(f"Exception caught {e} during execution inside login_and_download function")
        logging.exception(f'Exception caught during execution in login_and_download function : {e}')
        raise e
    finally:
        driver.quit()
    #     file_selected=f'emissions-hourly-2023'      #uncomment this line for testing
    #     search_key.send_keys(file_selected)
    #     logger.info("Accessing search button and clicking it")
    #     search_button=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'.usa-search__submit-text')))
    #     search_button.click()
    #     time.sleep(10)     
    #     logger.info("Accessing select_all button and clicking it") 
    #     select_all=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/main/div/div[2]/\
    #                                                                                                 div[2]/div/div[2]/div/div[1]/div/div/div[1]/div/div[1]/input')))
    #     select_all.click()
    #     time.sleep(5)
    #     logger.info("Accessing total count of selected files")
    #     total_select_count=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/\
    #                                         div/div/div/main/div/div[2]/div[2]/div/div[1]/div[1]'))).text
    #     time.sleep(2)
    #     select_all.click()
    #     total_files=int(total_select_count.split(':')[1].strip())
    #     time.sleep(5)
    #     logger.info("Getting total elements on page")
    #     page_elements=driver.find_elements(By.XPATH,'/html/body/div/div/div/main/div/div[2]/div[2]/div/div[2]/div/div[1]/div/div/div[2]')[0]
    #     text_page_elements=page_elements.text
    #     time.sleep(5)
    #     file_name_need=[]
    #     for i in text_page_elements.split('\n'):
    #         if file_selected in i:
    #             file_name_need.append(i)
    #     for key in file_name_need[0:total_files+1]:
    #         search_key.clear()
    #         search_key.send_keys(key)
    #         time.sleep(3)
    #         search_button.click()
    #         time.sleep(3)
    #         logger.info("Getting modified_date")
    #         date_modified=WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/div/div/div/main/div/div[2]/div[2]/div/div[2]/\
    #                                                                                                     div/div[1]/div/div/div[2]/div[1]/div[3]/div'))).text
    #         selected_file_name=key.split('.')[0]
    #         logger.info("Checking the max maodified date of the file from sf.")
    #         max_last_modified_date = get_last_modified_date(selected_file_name)
    #         if (max_last_modified_date== 'None') or (date_modified>max_last_modified_date):
    #             logger.info(f"{selected_file_name} file values are not there in sf.")
    #             try:
    #                 time.sleep(10)
    #                 select_all.click()
    #             except Exception as e:
    #                 logger.info(f"Error in Downloading {selected_file_name} file")
    #                 logger.info(f"ERROR:{e}")
    #                 raise e
    #             logger.info(f"Downloading {selected_file_name} file")
    #             download_button=WebDriverWait(driver, 150, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'.flex-end')))
    #             time.sleep(5)
    #             if download_button.is_enabled():
    #                 download_button.click()
    #             else:
    #                 download_button=WebDriverWait(driver, 150, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,'/html/body/div/div/div/\
    #                                                                 main/div/div[2]/div[2]/div/div[1]/button')))
    #                 download_button.click()
    #             time.sleep(5) 
    #             counter=0
    #             while True:
    #                 if len((os.listdir(files_location)))==1:
    #                     break
    #                 else:
    #                     time.sleep(10)
    #                     counter+=1
    #             downloaded_files= os.listdir(files_location)
    #             downloaded_file_name=downloaded_files[0].split('.')[0]
    #             logger.info(f"Creating df for the file:{selected_file_name}")
    #             df=pd.read_csv(files_location+'\\'+downloaded_files[0])
    #             df1=df[['State','Facility Name','Facility ID','Unit ID','Date','Hour','Operating Time','Gross Load (MW)','Steam Load (1000 lb/hr)',
    #                     'SO2 Mass (lbs)','SO2 Mass Measure Indicator','SO2 Rate (lbs/mmBtu)','SO2 Rate Measure Indicator','NOx Rate (lbs/mmBtu)',
    #                     'NOx Rate Measure Indicator','NOx Mass (lbs)','NOx Mass Measure Indicator','CO2 Mass (short tons)','CO2 Mass Measure Indicator',
    #                     'CO2 Rate (short tons/mmBtu)','CO2 Rate Measure Indicator','Heat Input (mmBtu)']]
    #             renamed_columns=["STATE","PLANT_NAME","EIA_PLANTID","EIA_GENERATORID","FLOW_DATE","FLOW_HOUR","OPERATING_HOUR","GROSS_LOAD_MW",\
    #                         "STEAM_LOAD_1000LB_PER_HR","SO2_MASS_LBS","SO2_MASS_MEASURE_FLG","SO2_RATE_LBS_PER_MMBTU","SO2_RATE_MEASURE_FLG",\
    #                             "NOX_RATE_LBS_PER_MMBTU","NOX_RATE_MEASURE_FLG","NOX_MASS_LBS","NOX_MASS_MEASURE_FLG",\
    #                                 "CO2_MASS_TONS","CO2_MASS_MEASURE_FLG","CO2_RATE_TONS_PER_MMBTU","CO2_RATE_MEASURE_FLG",\
    #                                     "HEAT_INPUT_MMBTU"]
    #             df1.columns=renamed_columns
    #             print(df1)
    #             df1['EIA_GENERATORID']=df1['EIA_GENERATORID'].astype(str)
    #             df1['INSERT_DATE']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             df1['UPDATE_DATE']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             df1.insert(loc=0,column='FILE_NAME',value=downloaded_file_name)
    #             df1.insert(loc=1,column='LAST_MODIFIED',value=date_modified)
    #             logger.info(f"dis selecting the file {selected_file_name}  in webpage")
    #             select_all.click()
    #             os.remove(files_location+'\\'+downloaded_files[0])
    #             logger.info(f"Uploading file: {selected_file_name}  into sf")
    #             no_of_rows+=upload_to_sf(df1,date_modified,downloaded_file_name) 
    #             logger.info(f"------------------------Uploading file: {selected_file_name}  completed------------------------------ ")
    #             print('Done')
    #         else:
    #             logger.info(f"Data got inserted for file:{selected_file_name} already and NO NEW DATA FOUND")
    #     return no_of_rows      
    # except Exception as e:
    #     print(f"Exception caught {e} during execution inside login_and_download function")
    #     logging.exception(f'Exception caught during execution in login_and_download function : {e}')
    #     raise e
    # finally:
    #     driver.quit()
            
def upload_to_sf(df,date_modified,downloaded_file_name):
    logging.info("Inside upload_to_sf function")
    try:
        engine = bu_snowflake.get_engine(
            warehouse="ITPYTHON_WH",
            role=f"OWNER_{database_name}",
            schema=schema_name,
            database=database_name
        )
        conn = engine.connect()
        temp_query = f"""create or replace temporary table {database_name}.PTEMP.EPA_EMISSION_BY_UNIT_TEMP (
                        FILE_NAME VARCHAR(256) NOT NULL,
                        LAST_MODIFIED TIMESTAMP_NTZ(9),
                        STATE VARCHAR(10),
                        PLANT_NAME VARCHAR(100),
                        EIA_PLANTID NUMBER(18,0) NOT NULL,
                        EIA_GENERATORID VARCHAR(20) NOT NULL,
                        FLOW_DATE DATE NOT NULL,
                        FLOW_HOUR NUMBER(18,0) NOT NULL,
                        OPERATING_HOUR NUMBER(18,6),
                        GROSS_LOAD_MW NUMBER(18,6),
                        STEAM_LOAD_1000LB_PER_HR NUMBER(18,6),
                        SO2_MASS_LBS NUMBER(18,6),
                        SO2_MASS_MEASURE_FLG VARCHAR(50),
                        SO2_RATE_LBS_PER_MMBTU NUMBER(18,6),
                        SO2_RATE_MEASURE_FLG VARCHAR(50),
                        NOX_RATE_LBS_PER_MMBTU NUMBER(18,6),
                        NOX_RATE_MEASURE_FLG VARCHAR(50),
                        NOX_MASS_LBS NUMBER(18,6),
                        NOX_MASS_MEASURE_FLG VARCHAR(50),
                        CO2_MASS_TONS NUMBER(18,6),
                        CO2_MASS_MEASURE_FLG VARCHAR(50),
                        CO2_RATE_TONS_PER_MMBTU NUMBER(18,6),
                        CO2_RATE_MEASURE_FLG VARCHAR(50),
                        HEAT_INPUT_MMBTU NUMBER(18,6),
                        EPA_PLANTID NUMBER(18,0),
                        EPA_UNITID NUMBER(18,0),
                        INSERT_DATE TIMESTAMP_NTZ(9),
                        UPDATE_DATE TIMESTAMP_NTZ(9),
                        primary key (FILE_NAME, EIA_PLANTID, EIA_GENERATORID, FLOW_DATE, FLOW_HOUR)
                    )"""
        res = conn.execute(temp_query).fetchone()
        logging.info(res)
        logging.info("Temporary table created")
        df.to_sql("EPA_EMISSION_BY_UNIT_TEMP",
            con=conn,
            index=False,
            if_exists='append',
            schema = 'PTEMP',
            chunksize=200000,
            method=functools.partial(pd_writer, quote_identifiers=True)
            )
        logging.info("Data is inserted into temporary table successfully")
        res2 = conn.execute(f'''merge into {database_name}.{schema_name}.EPA_EMISSION_BY_UNIT t using {database_name}.PTEMP.EPA_EMISSION_BY_UNIT_TEMP s 
                        on 
                        t.FILE_NAME = s.FILE_NAME
                        and t.EIA_PLANTID = s.EIA_PLANTID
                        and t.EIA_GENERATORID = s.EIA_GENERATORID
                        and t.FLOW_DATE = s.FLOW_DATE
                        and t.FLOW_HOUR = s.FLOW_HOUR        
                        when matched then update
                        set 
                        t.FILE_NAME = s.FILE_NAME,
                            t.LAST_MODIFIED = s.LAST_MODIFIED,
                            t.STATE = s.STATE,
                            t.PLANT_NAME = s.PLANT_NAME,
                            t.EIA_PLANTID = s.EIA_PLANTID,
                            t.EIA_GENERATORID = s.EIA_GENERATORID,
                            t.FLOW_DATE = s.FLOW_DATE,
                            t.FLOW_HOUR = s.FLOW_HOUR,
                            t.OPERATING_HOUR = s.OPERATING_HOUR,
                            t.GROSS_LOAD_MW = s.GROSS_LOAD_MW,
                            t.STEAM_LOAD_1000LB_PER_HR = s.STEAM_LOAD_1000LB_PER_HR,
                            t.SO2_MASS_LBS = s.SO2_MASS_LBS,
                            t.SO2_MASS_MEASURE_FLG = s.SO2_MASS_MEASURE_FLG,
                            t.SO2_RATE_LBS_PER_MMBTU = s.SO2_RATE_LBS_PER_MMBTU,
                            t.SO2_RATE_MEASURE_FLG = s.SO2_RATE_MEASURE_FLG,
                            t.NOX_RATE_LBS_PER_MMBTU = s.NOX_RATE_LBS_PER_MMBTU,
                            t.NOX_RATE_MEASURE_FLG = s.NOX_RATE_MEASURE_FLG,
                            t.NOX_MASS_LBS = s.NOX_MASS_LBS,
                            t.NOX_MASS_MEASURE_FLG = s.NOX_MASS_MEASURE_FLG,
                            t.CO2_MASS_TONS = s.CO2_MASS_TONS,
                            t.CO2_MASS_MEASURE_FLG = s.CO2_MASS_MEASURE_FLG,
                            t.CO2_RATE_TONS_PER_MMBTU = s.CO2_RATE_TONS_PER_MMBTU,
                            t.CO2_RATE_MEASURE_FLG = s.CO2_RATE_MEASURE_FLG,
                            t.HEAT_INPUT_MMBTU = s.HEAT_INPUT_MMBTU,
                            t.EPA_PLANTID = s.EPA_PLANTID,
                            t.EPA_UNITID = s.EPA_UNITID,
                            t.INSERT_DATE = s.INSERT_DATE,
                            t.UPDATE_DATE = s.UPDATE_DATE

                            when not matched then
                            insert 
                            (
                                FILE_NAME,
                                LAST_MODIFIED,
                                STATE,
                                PLANT_NAME,
                                EIA_PLANTID,
                                EIA_GENERATORID,
                                FLOW_DATE,
                                FLOW_HOUR,
                                OPERATING_HOUR,
                                GROSS_LOAD_MW,
                                STEAM_LOAD_1000LB_PER_HR,
                                SO2_MASS_LBS,
                                SO2_MASS_MEASURE_FLG,
                                SO2_RATE_LBS_PER_MMBTU,
                                SO2_RATE_MEASURE_FLG,
                                NOX_RATE_LBS_PER_MMBTU,
                                NOX_RATE_MEASURE_FLG,
                                NOX_MASS_LBS,
                                NOX_MASS_MEASURE_FLG,
                                CO2_MASS_TONS,
                                CO2_MASS_MEASURE_FLG,
                                CO2_RATE_TONS_PER_MMBTU,
                                CO2_RATE_MEASURE_FLG,
                                HEAT_INPUT_MMBTU,
                                EPA_PLANTID,
                                EPA_UNITID,
                                INSERT_DATE,
                                UPDATE_DATE
                            )
                        values (
                                s.FILE_NAME,
                                s.LAST_MODIFIED,
                                s.STATE,
                                s.PLANT_NAME,
                                s.EIA_PLANTID,
                                s.EIA_GENERATORID,
                                s.FLOW_DATE,
                                s.FLOW_HOUR,
                                s.OPERATING_HOUR,
                                s.GROSS_LOAD_MW,
                                s.STEAM_LOAD_1000LB_PER_HR,
                                s.SO2_MASS_LBS,
                                s.SO2_MASS_MEASURE_FLG,
                                s.SO2_RATE_LBS_PER_MMBTU,
                                s.SO2_RATE_MEASURE_FLG,
                                s.NOX_RATE_LBS_PER_MMBTU,
                                s.NOX_RATE_MEASURE_FLG,
                                s.NOX_MASS_LBS,
                                s.NOX_MASS_MEASURE_FLG,
                                s.CO2_MASS_TONS,
                                s.CO2_MASS_MEASURE_FLG,
                                s.CO2_RATE_TONS_PER_MMBTU,
                                s.CO2_RATE_MEASURE_FLG,
                                s.HEAT_INPUT_MMBTU,
                                s.EPA_PLANTID,
                                s.EPA_UNITID,
                                s.INSERT_DATE,
                                s.UPDATE_DATE
                            )''').fetchone()
        logging.info(res2)
        logging.info("Uploaded data into POWERDB main table using merge query from temporary table")
        logging.info(f"Uploaded to {database_name}.{schema_name}.{table_name}")
        sql = '''
        select count(*) from {} where FILE_NAME='{}' and LAST_MODIFIED='{}'
        '''.format(database_name+"."+schema_name+"."+table_name,downloaded_file_name,date_modified)
        res3 = conn.execute(sql)
        df2= pd.DataFrame.from_records(iter(res3))
        return df2[0][0]
    except Exception as e:
        print(f"Exception caught {e} during execution inside upload_to_sf function")
        logging.exception(f'Exception caught during execution in upload_to_sf function : {e}')
        raise e
    finally:
        engine.dispose()
        conn.close()
if __name__ == "__main__": 
    try:
        logging.info("Execution Started")
        time_start=time.time()
        today_date=date.today()
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        # log progress --
        logfile = os.getcwd() +"\\logs\\"+'EPA_EMISSION_BY_UNIT_'+str(today_date)+'.txt'
        logging.basicConfig(level=logging.INFO,filename=logfile,filemode='w',format='[line :- %(lineno)d] %(asctime)s [%(levelname)s] - %(message)s ')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        job_id=np.random.randint(1000000,9999999)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        no_of_rows=0
        today_year=date.today().year

        directories_created=["downloadepa","Logs"]
        for directory in directories_created:
            path3 = os.path.join(os.getcwd(),directory)  
            try:
                os.makedirs(path3, exist_ok = True)
                print("Directory '%s' created successfully" % directory)
            except OSError as error:
                print("Directory '%s' can not be created" % directory)       
        files_location=os.getcwd() + "\\downloadepa"


        credential_dict = get_config('SUPPLY_STACK','EPA_EMISSION_BY_UNIT')
        job_name=credential_dict['TABLE_NAME']
        processname = credential_dict['PROJECT_NAME']
        source_url=credential_dict['SOURCE_URL']
        table_name = credential_dict['TABLE_NAME']
        # table_name='EPA_EMISSION_BY_UNIT'
        database_name = credential_dict['DATABASE']
        # database_name='POWERDB_DEV'
        schema_name = credential_dict['TABLE_SCHEMA']
        # schema_name='PMACRO'
        process_owner = credential_dict['IT_OWNER']
        receiver_email = credential_dict['EMAIL_LIST'] 
        # receiver_email = 'enoch.benjamin@biourja.com'
        bu_alerts.bulog(process_name=processname,database=database_name,status='Started',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        remove_existing_files(files_location)
        no_of_rows=login_and_download()
        bu_alerts.bulog(process_name=processname,database=database_name,status='Completed',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)  
        if no_of_rows>0:
            mail_subject_line=f'{no_of_rows} rows got inserted'
        else:
            mail_subject_line=f'NO NEW DATA FOUND'   
        bu_alerts.send_mail(receiver_email = receiver_email,
                                mail_subject =f'JOB SUCCESS - {job_name} and {mail_subject_line}',
                                mail_body = f'{job_name} completed successfully, Attached Logs',
                                attachment_location = logfile)
        time_end=time.time()
        logging.info(f'It takes {time_start-time_end} seconds to run')   
    except Exception as e:
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= 'EPA_EMISSION_BY_UNIT',database='POWERDB_DEV',status='Failed',table_name='',
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner='Enoch Benjamin')
        logging.exception(str(e))
        bu_alerts.send_mail(receiver_email = receiver_email,
                            mail_subject =f'JOB FAILED -{job_name}',
                            mail_body = f'{job_name} failed in __main__, Attached logs',
                            attachment_location = logfile)
        sys.exit(1)