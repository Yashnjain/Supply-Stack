from ast import ExtSlice
import os
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as Datetime
import bu_alerts
import bu_snowflake
import bu_config
import logging
from snowflake.connector.pandas_tools import pd_writer
import functools
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from functools import reduce
import time



def remove_existing_files(files_location):
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

def get_df_from_db(current_year):
    try:
        logger.info("Inside get_db_df function")
        engine = bu_snowflake.get_engine(
                        database=databasename,
                        role=f"OWNER_{databasename}",    
                        schema= schemaname                           
                    )

        query = f"select * from {databasename}.{schemaname}.{tablename} where FLOW_MONTH >= '{current_year}'"
        db_df = pd.read_sql(query,con=engine)
        logger.info("Dataframe created from database for current year")
        return db_df
    except Exception as e:
        logger.exception("Exception while getting  dataframe from snowflake")
        logger.exception(e)
        raise e
    finally:
        try:            
            engine.dispose()
            logger.info("Engine object disposed successfully and connection object closed")
        except Exception as e:
            logger.exception(e)
            raise e

def compare_df(df,db_df,update_date_on):
    try:
        logger.info("Inside compare_df function")
        columns = ['FLOW_MONTH', 'CAPP_CSX_OTC', 'ILLINOIS_BASIN_BARGE_PHY',
        'NAPP_PITT_8_PHY', 'SPRB_8800_OTC']

        df = df[columns]
        db_df = db_df[columns]

        db_df.drop_duplicates(inplace = True)
        db_df.reset_index(inplace = True, drop = True)
        logger.info("Dropped the duplicate values from db_df")

        # This is a dataframe with ones and zeros only to color the dataframe whcich used for reference in df_comapre
        df_mask = db_df.compare(df,keep_shape=True).notnull().astype(int)
        df_mask.columns = df_mask.columns.set_levels(['OLD_VALUE','NEW_VALUE'],level = 1)
        logger.info("df_mask created")

        # This is the dataframe created by comparing the two dataframes and color is applied in the cells where tha values are different
        df_compare = db_df.compare(df,keep_shape=True, keep_equal=True)
        df_compare.columns = df_compare.columns.set_levels(['OLD_VALUE','NEW_VALUE'],level = 1)
        logger.info("df_compare created")

        change_list = (df == db_df).all().values
        check_val = reduce(lambda x,y: x and y,change_list)

        if check_val == True:
            body = f"No Changes found in the Data for {update_date_on}"
        else:
            body = f"Changes found in the Data for {update_date_on} and the changes are hilighted"

        def apply_color(x):
            #This function applies color in the df_mask and on the basis of that we apply color in df_compare

            colors = {1: 'lightblue', 0: 'white'}
            return df_mask.applymap(lambda val: 'background-color: {}'.format(colors.get(val,'')))

        styler = df_compare.style.apply(apply_color, axis=None)
        styler.set_properties(**{'border': '1px solid purple'})
        styler.hide_index()
        styler.set_table_styles([{'selector' : '',
                            'props' : [('border',
                                        '3px solid purple')]}])

        logger.info("Styles applied in the compare dataframe")
        bu_alerts.send_mail(
                receiver_email = business_email,
                mail_subject = f'Data comparison for {tablename}',
                mail_body= f"""<html>
                    <b>{body}<b>
                    <br><br>
                    {styler.render()}
                    </html>""",
                attachment_location = logfilename
                )

        logger.info("Mail sent showing the difference in data")        
        print("Pause")
    except Exception as e:
        logger.info(e)
        raise e
        

def get_and_upload_df(files_location,update_date_on):
    logger.info("Inside get_df function")
    rows = 0
    try:
        df = pd.read_csv(files_location + "\\ForecastPrices.csv")
        logger.info("File read and first dataframe created")
        df.columns = ["FLOW_MONTH","CAPP_CSX_OTC","ILLINOIS_BASIN_BARGE_PHY","NAPP_PITT_8_PHY","SPRB_8800_OTC"]
        logger.info("Columns renamed in dataframe")
        df["FCST_DATE"] = update_date_on
        logger.info("FCST_DATE column value updated")
        current_year = datetime.now().date().replace(month=1,day=1)
        logger.info("Current year object created")
        df['FLOW_MONTH'] = df['FLOW_MONTH'].apply(lambda x:datetime.strptime(x,"%b-%y").date())
        df = df[df['FLOW_MONTH'] >= current_year].reset_index().drop(columns = ['index'])
        logger.info("FLOW_MONTH formated and the dataframe for current year created")
        df['FCST_DATE'] = df['FCST_DATE'].map(str)
        df['FLOW_MONTH'] = df['FLOW_MONTH'].map(str)
        df["INSERT_DATE"] = str(datetime.now())
        df["UPDATE_DATE"] = str(datetime.now())
        logger.info("Final dataframe created and INSERT_DATE and UPDATE_DATE added")

        logger.info("Calling get_df_from_db function")
        db_df = get_df_from_db(current_year)
        logger.info("Get_df_from_db function executed successfully")
        db_df.columns = db_df.columns.str.upper()
        db_df['FCST_DATE'] = db_df['FCST_DATE'].map(str)
        db_df['FLOW_MONTH'] = db_df['FLOW_MONTH'].map(str)
        logger.info("Converted the FCST_DATE and FLOW_MONTH to string type from date for db_df")

        upload_flag = False
        if len(df) != len(db_df):
            # since the lengths are not equal due to the change in year concatenating data from db to db_df for new year
            db_df = pd.concat([db_df,df[df['FLOW_MONTH'].apply(lambda x: str(current_year.year) not in x)]],ignore_index = True)
            upload_flag = True

        if db_df['FCST_DATE'][0] != update_date_on or upload_flag: 
            logger.info("Calling compare df function")
            compare_df(df,db_df,update_date_on)
            logger.info("Compare df function completed")
            
            logger.info("Upload to sf function calling")
            rows = upload_in_sf(tablename, df, current_year)
        else:
            logger.info("No new values found as the updated date is the same")       
            bu_alerts.send_mail(
                receiver_email = it_email,
                mail_subject = f'JOB Data not found - {tablename}. Data must have been already updated',
                mail_body = f"The Data must have been uploaded for {update_date_on}. Check the data if updated.",
                attachment_location = logfilename)

        return rows
    except Exception as e:
        print(e)
        logger.exception(e)



def download_data(url,files_location,executable_path,username,password):
    try:
        this_year = str(datetime.today().year)
        next_year = str(datetime.today().year + 1)
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
        browser.get(url)
        time.sleep(5)
        logger.info("Browser get successfully")
        browser.maximize_window()
        print(browser.title)
        logger.info("Username and password put successfully")
        username_path = browser.find_element(By.ID,"input27")
        username_path.send_keys(username)
        logger.info("Username selected")
        time.sleep(5) 
        browser.find_element(By.XPATH,'/html/body/div[1]/div/div[1]/div[3]/main/div[2]/div/div/div[2]/form/div[2]/input').click()
        time.sleep(5)
        browser.find_element(By.XPATH,'/html/body/div[1]/div/div[1]/div[3]/main/div[2]/div/div/div[2]/form/div[2]/div/div[2]/div[2]/div[2]/a').click()
        time.sleep(5)
        
        password_path = browser.find_element(By.ID,"input84")
        password_path.send_keys(password)
        logger.info("Password selected")

        submit_button = WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/div[1]/div/div[1]/div[3]/main/div[2]/div/div/div[2]/form/div[2]/input")))
        submit_button.click()
        logger.info("Clicked on Submit button")
        time.sleep(5)
        logger.info("Signed in successfully")
        print("Signed in successfully")
        coockies_path = WebDriverWait(browser, 35).until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
        coockies_path.click()
        logger.info("Coockies selected")
        time.sleep(5)

        browser.switch_to.frame("portalframe")
        browser.switch_to.frame("TopNavBar")
        coal_xpath = WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/div[1]/div/ul/li[2]/a")))
        coal_xpath.click()
        time.sleep(5)
        logger.info("Clicked on prices x-path")
        browser.switch_to.default_content()
        browser.switch_to.frame("portalframe")
        browser.switch_to.frame("Body")
        browser.switch_to.frame("Toolbar")

        select_from = WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/form/div/table/tbody/tr/td[2]/table/tbody/tr/td[3]/table/tbody/tr/td/div[2]/select")))
        from_options = Select(select_from)
        from_options.select_by_visible_text(this_year)
        logger.info("From value selected")
        time.sleep(3)

        select_to = WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/form/div/table/tbody/tr/td[2]/table/tbody/tr/td[5]/table/tbody/tr/td/div[2]/select")))
        to_options = Select(select_to)
        last_year = to_options.options[0].text
        to_options.select_by_visible_text(last_year)
        logger.info("To value selected")
        time.sleep(3)

        select_product = WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/form/div/table/tbody/tr/td[2]/table/tbody/tr/td[8]/table/tbody/tr/td[1]/div[2]/select")))
        product_options = Select(select_product)
        product_options.select_by_visible_text("Coal")
        logger.info("Product value selected")
        time.sleep(3)

        select_price_group =  WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/form/div/table/tbody/tr/td[2]/table/tbody/tr/td[9]/table/tbody/tr/td[1]/div[2]/select")))
        price_group_options = Select(select_price_group)
        price_group_options.select_by_visible_text("United States")
        logger.info("Price group value selected")
        time.sleep(10)

        browser.switch_to.default_content()
        browser.switch_to.frame("portalframe")
        browser.switch_to.frame("Body")
        browser.switch_to.frame("Body")  # There are two body iframes. One insie another
        select_format = WebDriverWait(browser, 35).until(EC.presence_of_element_located((By.ID, "ReportViewerControl_ctl01_ctl05_ctl00")))
        format_options = Select(select_format)
        format_options.select_by_visible_text("Excel (CSV) file")
        logger.info("Format option selected to download the data")

        download_button = WebDriverWait(browser, 35).until(EC.presence_of_element_located((By.ID, "ReportViewerControl_ctl01_ctl05_ctl01")))
        download_button.click()
        time.sleep(10)
        logger.info("Data downloaded successfully")

        browser.switch_to.frame("ReportFrameReportViewerControl") 
        time.sleep(5)
        browser.switch_to.frame("report")
        time.sleep(5)
        updated_date_on_xpath = WebDriverWait(browser, 35).until(EC.presence_of_element_located(
            (By.XPATH,"/html/body/div/table/tbody/tr[1]/td[1]/div/table/tbody/tr[4]/td[2]/table/tbody/tr[2]/td/div")))
        update_date_on_text = updated_date_on_xpath.text
        update_date_on = str(datetime.strptime(update_date_on_text.split(" ")[2],"%m/%d/%Y").date())
        logger.info("Udated date value fetched")

        return update_date_on

    except Exception as e:
        logger.exception(e)
        raise e
    finally:      
        browser.quit()
        logger.info("Browser quit successfully")


def upload_in_sf(tablename, df, current_year):
    logger.info("Inside upload_in_sf function")
    total_rows = 0
    try:
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        conn = engine.connect()
        logger.info("Engine object created successfully")

        check_query = f"select * from {databasename}.{schemaname}.{tablename} where FLOW_MONTH >= '{current_year}'"
        check_rows = conn.execute(check_query).fetchall()
        logger.info(f"Number of rows fetched are {len(check_rows)} for data after {current_year}")
        if len(check_rows) > 0:
            delete_query = f"delete from {databasename}.{schemaname}.{tablename} where FLOW_MONTH >= '{current_year}'"
            delete_rows = conn.execute(delete_query).fetchall()
            logger.info(f"Total rows deleted {delete_rows[0][0]}")
        else:
            logger.info(f"No rows found for data after {current_year}")

        df.to_sql(tablename.lower(), 
                con=engine,
                index=False,
                if_exists='append',
                schema=schemaname,
                method=functools.partial(pd_writer, quote_identifiers=False)
                )
        total_rows += len(df)
    except Exception as e:
        logger.exception("Exception while inserting data into snowflake")
        logger.exception(e)
        raise e
    finally:
        try:        
            conn.close()      
            engine.dispose()
            logger.info("Engine object disposed successfully and connection object closed")
            return total_rows
        except Exception as e:
            logger.exception(e)
            raise e

if __name__ == "__main__":
    try:
        job_id=np.random.randint(1000000,9999999)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logfilename = bu_alerts.add_file_logging(logger,process_name= 'snp_coal_st_price')

        logger.info("Execution started")    
        credential_dict = bu_config.get_config('SUPPLY_STACK','SNP_COAL_ST_PRICE_FCST_MONTHLY')
        processname =  credential_dict['PROJECT_NAME']
        databasename = credential_dict['DATABASE']
        # databasename = 'POWERDB_DEV'
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        url = credential_dict['SOURCE_URL']
        process_owner = credential_dict['IT_OWNER']
        username = credential_dict['USERNAME']
        password = credential_dict['PASSWORD']
        receiver_email = credential_dict['EMAIL_LIST']
        business_email = receiver_email.split(';')[0]
        it_email = receiver_email.split(';')[1]
        # receiver_email = business_email = it_email = "Mrutunjaya.Sahoo@biourja.com, ayushi.joshi@biourja.com"

        logger.info("All the credential details fetched from creential dict")

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        files_location = os.getcwd() + '\\download_coal'
        executable_path = os.getcwd() + "\\geckodriver\\geckodriver.exe"

        logger.info("Calling remove_existing_files function")
        remove_existing_files(files_location)
        logger.info("Remove existing files completed successfully")

        logger.info("Calling download data function")
        update_date_on = download_data(url,files_location,executable_path,username,password)
        logger.info("Download data function completed")
        
        logger.info("Get df function calling")
        rows = get_and_upload_df(files_location, update_date_on)
        logger.info("Get df function completed successfully")

        bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        if rows > 0:
            subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"
        else:
            subject = f"JOB SUCCESS - {tablename}  NO new data found"

        bu_alerts.send_mail(
            receiver_email = business_email, 
            mail_subject = subject,
            mail_body=f'{tablename} completed successfully, Attached logs',
            attachment_location = logfilename
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        bu_alerts.send_mail(
            receiver_email = business_email,
            mail_subject = f'JOB FAILED - {tablename}',
            mail_body=f'{tablename} failed during execution, Attached logs',
            attachment_location = logfilename
        )
    
    






