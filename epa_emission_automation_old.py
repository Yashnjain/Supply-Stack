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
from zipfile import ZipFile
import pandas as pd
import bu_snowflake
from snowflake.connector.pandas_tools import write_pandas
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def num_to_col_letters(num):
    try:
        letters = ''
        while num:
            mod = (num - 1) % 26
            letters += chr(mod + 65)
            num = (num - 1) // 26
        return ''.join(reversed(letters))
    except Exception as e:
        logger.info(e)
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

def download_wait(directory, nfiles = None):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 90:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True
        for fname in files:
            print(fname)
            if fname.endswith('.crdownload'):
                dl_wait = True
            elif fname.endswith('.tmp'):
                dl_wait = True
        seconds += 1
    return seconds            

def latest_download_file(num_file,path):
    os.chdir(path)
    while True:
        files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
        #wait for file to be finish download
        if len(files) < num_file:
            time.sleep(1)
            print('waiting for download to be initiated')
        else:
            newest = files[-1]
            if ".crdownload" in newest:
                time.sleep(1)
                print('waiting for download to complete')
            elif ".part" in newest:
                time.sleep(1)
                print('waiting for download to complete')
            elif ".tmp" in newest:
                time.sleep(1)
                print('waiting for download to complete')
            else:
                return newest

def login_and_download(conn,dict3,no_of_rows):  
    '''This function downloads log in to the website'''
    try:
        logger.info('Accesing website')
        years = list(range(current_yr,2013,-1))
        years.sort()
        #fecthing vmax last modified date
        fetch_query=f'''select max(LAST_MODIFIED) from {Database}.{SCHEMA}.{table_name}'''
        cur = conn.cursor()
        cur.execute(fetch_query)
        data=cur.fetchall()
        for year in years:
            # year='2022'                #change here to run for a particular year
            driver.get(f"{source_url}{year}")
            time.sleep(5)           
            check_date=format(str(data[0][0]))
            df_list = pd.read_html(f"{source_url}{year}")
            required_df=df_list[0][["Name","Last modified"]]
            required_df.dropna(how='any',inplace=True)
            required_df.reset_index(inplace=True, drop=True)
            required_df.rename(columns = {'Name':'FILE_NAME', 'Last modified':'LAST_MODIFIED'}, inplace = True)
            filtered_df=required_df.loc[required_df["LAST_MODIFIED"]>check_date]
            filtered_df.reset_index(inplace=True, drop=True) 
            dict1=dict(zip(list(filtered_df['FILE_NAME'][0:]),list(filtered_df['LAST_MODIFIED'][0:]))) 
            # dict2={"2022tx02.zip":"2022-06-22 19:07","2022tx03.zip":"2022-06-22 19:07"}   #change here for manually inserting the files
            if len(dict1)>0:
                for key, value in dict1.items():
                    try:
                        print(key)
                        time.sleep(1)
                        if os.path.exists(files_location +'\\'+ key):
                                os.remove(files_location +'\\'+ key)
                        num_files = len(os.listdir(files_location))
                        WebDriverWait(driver, 90, poll_frequency=2).until(EC.element_to_be_clickable((By.LINK_TEXT, f"{key}"))).click()
                        download_wait(files_location)
                        newest = latest_download_file(num_files,files_location)
                        try:
                            os.rename(files_location +'\\'+ newest,files_location +'\\'+ key)
                        except:
                            os.remove(files_location +'\\'+ key)
                            os.rename(files_location +'\\'+ newest,files_location +'\\'+ key)
                        # unzip the downloaded zip file and get all the files
                        time.sleep(5)
                        retry=0
                        while retry < 10:
                            try:
                                with ZipFile(files_location +'\\'+ key, 'r') as zipObj:
                                    # Extract all the contents of zip file in specified directory
                                    zipObj.extractall(extracted_directory)
                                break
                            except Exception as e:
                                time.sleep(5)
                                retry+=1
                                if retry ==10:
                                    raise e        
                        #iterating over files in the directory    
                        files = os.listdir(extracted_directory)
                        for file in files :
                            #creating dataframe for the file dwonloaded
                            final_df= pd.read_csv (f"{extracted_directory}\\{file}")
                            final_df.columns = ["STATE","PLANT_NAME","EIA_PLANTID","EIA_GENERATORID","FLOW_DATE","FLOW_HOUR","OPERATING_HOUR","GROSS_LOAD_MW",\
                            "STEAM_LOAD_1000LB_PER_HR","SO2_MASS_LBS","SO2_MASS_MEASURE_FLG","SO2_RATE_LBS_PER_MMBTU","SO2_RATE_MEASURE_FLG",\
                                "NOX_RATE_LBS_PER_MMBTU","NOX_RATE_MEASURE_FLG","NOX_MASS_LBS","NOX_MASS_MEASURE_FLG",\
                                    "CO2_MASS_TONS","CO2_MASS_MEASURE_FLG","CO2_RATE_TONS_PER_MMBTU","CO2_RATE_MEASURE_FLG",\
                                        "HEAT_INPUT_MMBTU","EPA_PLANTID","EPA_UNITID"]
                            final_df.insert(0,'LAST_MODIFIED',value) 
                            final_df.insert(0,'FILE_NAME',key)
                            final_df['INSERT_DATE'] = str(datetime.now())
                            final_df['UPDATE_DATE'] = str(datetime.now())
                            final_df["EIA_GENERATORID"] = final_df["EIA_GENERATORID"].astype(str)             
                            final_df["FLOW_DATE"] = final_df["FLOW_DATE"].astype('datetime64[ns]').astype(str)
                            # final_df["INSERTDATE"]=pd.to_datetime(final_df["INSERTDATE"])
                            no_of_rows=snowflake_dump(final_df,conn,no_of_rows) 
                            #removing the file inserted previously 
                            remove_existing_files(extracted_directory) 
                            logger.info(key)
                    except Exception as e:
                        dict3[key] = value
                        logger.info(key,e) 
                        print(key,e)
                        raise e
                        
                    # finally:
                    #     try:
                    #         return dict3
                    #     except:
                    #         pass    
            else:
                logger.info(f"no file found for {year}")
                continue
            return dict3,no_of_rows
        return dict3,no_of_rows

    except Exception as e:
        delete_query=f'''delete from {Database}.{SCHEMA}.{table_name} where INSERT_DATE>=current_date()'''
        cur = conn.cursor()
        cur.execute(delete_query)
        raise e


def snowflake_dump(final_df,conn,no_of_rows):        
    try:
        #deleting and inserting file into snowflake
        logger.info("query to delete data")
        delete_query = f"delete from {Database}.{SCHEMA}.{table_name} where FILE_NAME = '{final_df['FILE_NAME'][0]}'"      
        cur = conn.cursor()
        cur.execute(delete_query)
        logger.info("inserting data")
        success, nchunks, nrows, _ = write_pandas(conn, final_df, table_name)
        no_of_rows+=nrows
        return no_of_rows 
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        raise(e)


def main():
    try:
        no_of_rows=0
        dict3={}
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=processname,database=Database,status='Started',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        remove_existing_files(files_location)
        dict3,no_of_rows=login_and_download(conn,dict3,no_of_rows)
        print("done") 
        locations_list.append(logfile)
        bu_alerts.bulog(process_name=processname,database=Database,status='Completed',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)  
        if no_of_rows>0:
            bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB SUCCESS - {job_name} and {no_of_rows} rows updated',mail_body = f'{job_name} completed successfully, Attached Logs',attachment_location = logfile)
        else:
            bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB SUCCESS - {job_name} data inserted previously and NO NEW DATA FOUND',mail_body = f'{job_name} completed successfully, Attached Logs',attachment_location = logfile)
    except Exception as e:
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=Database,status='Failed',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        logger.exception(str(e))
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB FAILED -{job_name}',mail_body = f'{job_name} failed, Attached logs',multiple_attachment_list = logfile)
        try:
            driver.quit()    
        except:
            pass
        sys.exit(1)   

if __name__ == "__main__": 
    try:
        # logger.info("Execution Started")
        time_start=time.time()
        #Global VARIABLES
        locations_list=[]
        body = ''
        dict3={}
        today_date=date.today()
        # log progress --
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        # log progress --
        logfile = os.getcwd() +"\\logs\\"+'EPA_'+str(today_date)+'.txt'
        logging.basicConfig(level=logging.INFO,filename=logfile,filemode='w',format='[line :- %(lineno)d] %(asctime)s [%(levelname)s] - %(message)s ')

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # logger = logger.getLogger()
        # logger.setLevel(logger.INFO)
        directories_created=["downloadepa","Logs","extracted_zipfiles"]
        for directory in directories_created:
            path3 = os.path.join(os.getcwd(),directory)  
            try:
                os.makedirs(path3, exist_ok = True)
                print("Directory '%s' created successfully" % directory)
            except OSError as error:
                print("Directory '%s' can not be created" % directory)       
        files_location=os.getcwd() + "\\downloadepa"
        filesToUpload = os.listdir(os.getcwd() + "\\downloadepa")
        extracted_directory=os.getcwd() + "\\extracted_zipfiles"
        logger.info('setting paTH TO download')
        path = os.getcwd() + '\\downloadepa'
        logger.info('SETTING PROFILE SETTINGS FOR FIREFOX')
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.dir', path)
        profile.set_preference('browser.download.useDownloadDir', True)
        profile.set_preference('browser.download.viewableInternally.enabledTypes', "")
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk','Portable Document Format (PDF), application/pdf')
        profile.set_preference('pdfjs.disabled', True)
        logger.info('Adding firefox profile')
        driver=webdriver.Firefox(executable_path=GeckoDriverManager().install(),firefox_profile=profile)
        credential_dict = get_config('SUPPLY_STACK','EPA_EMISSION_BY_UNIT')
        receiver_email = credential_dict['EMAIL_LIST']
        # receiver_email='yashn.jain@biourja.com'  
        job_name=credential_dict['TABLE_NAME']
        job_id=np.random.randint(1000000,9999999)
        processname = credential_dict['PROJECT_NAME']
        process_owner = credential_dict['IT_OWNER']
        source_url=credential_dict['SOURCE_URL']
        current_yr=today_date.year
        current_month=today_date.strftime("%m")
        #snowflake variables
        Database = credential_dict['DATABASE']
        # Database = "POWERDB_DEV"
        SCHEMA = credential_dict['TABLE_SCHEMA']
        table_name = credential_dict['TABLE_NAME']
        conn=bu_snowflake.get_connection(
            database  = Database,
            schema=SCHEMA,
            role =f"OWNER_{Database}"
        )
        main()
        conn.close()
        try:
            driver.quit()       
        except:
            pass
        time_end=time.time()
        logger.info(f'It takes {time_start-time_end} seconds to run')
        sys.exit(0)
    except Exception as e:
        logger.exception(str(e))
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB FAILED -{job_name}',mail_body = f'{job_name} failed in __main__, Attached logs',attachment_location = logfile)
        try:
            driver.quit()       
        except:
            pass
        sys.exit(1)