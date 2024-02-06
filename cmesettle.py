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


def map_product(s):
    if s == 'CL':
        return 'WTI'
    elif s == 'CY':
        return 'BRENT'
    elif s == 'HO':
        return 'HEATING_OIL'
    elif s == 'MM':
        return 'FUEL_OIL'

def get_df(files_location):
    logger.info("Inside get_df function")
    total_rows = 0
    try:
        col_names = ['PRODUCT SYMBOL', 'CONTRACT MONTH', 'CONTRACT YEAR',
                'CONTRACT', 'PRODUCT DESCRIPTION', 'OPEN', 'HIGH','LOW','LAST','SETTLE','EST. VOL','PRIOR INT','TRADEDATE']
        # df= pd.read_csv(files_location+'\\nymex_future.csv', index_col=False)
        df = pd.read_csv('ftp://ftp.cmegroup.com/pub/settle/nymex_future.csv')
        df = df.filter(items = col_names)
        logger.info("Dataframe read from csv")
        logger.info("Excel file read successfully")
        df.rename(columns = {'PRODUCT SYMBOL':'PRODUCT_CODE','CONTRACT MONTH':'FLOW_MONTH','CONTRACT YEAR':'FLOW_YEAR',
            'PRODUCT DESCRIPTION':'PRODUCT_DETAIL','EST. VOL':'EST_VOL','PRIOR INT':'PRIOR_INT'},inplace=True)
        df = df[df.PRODUCT_CODE.isin (["CL", "CY", "HO", "MM"])]
        logger.info("Dataframe extracted for the selected product codes")
        df =  df.reset_index(drop = True)
        df['PRODUCT'] = df['PRODUCT_CODE'].map(map_product)
        logger.info("PRODUCT column created with mapping to PRODUCT_CODE column")
        df.TRADEDATE = pd.to_datetime(df.TRADEDATE).dt.date
        df['INSERT_DATE'] = str(datetime.now())
        df['UPDATE_DATE'] = str(datetime.now())
        logger.info("Final dataframe created with all the required columns")
        print("Done")

        return df
    except Exception as e:
        print(e)
        logger.exception(e)
        raise e

def upload_in_sf(tablename, df):
    logger.info("Inside upload_in_sf function")
    total_rows = 0
    trade_date = df.TRADEDATE[0]
    try:
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        conn = engine.connect()
        logger.info("Engine object created successfully")

        check_query = f"select * from {databasename}.{schemaname}.{tablename} where TRADEDATE = '{trade_date}'"
        check_rows = conn.execute(check_query).fetchall()
        if len(check_rows) > 0:
            logger.info(f"The values are already present for {trade_date}")
        else:
            df.to_sql(tablename.lower(), 
                    con=engine,
                    index=False,
                    if_exists='append',
                    schema=schemaname,
                    method=functools.partial(pd_writer, quote_identifiers=False)
                    )
            logger.info(f"Dataframe Inserted into the table {tablename} for TRADEDATE {trade_date}")
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

if __name__ == '__main__':
    try:
        job_id=np.random.randint(1000000,9999999)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logfilename = bu_alerts.add_file_logging(logger,process_name= 'CMESETTLE')

        logger.info("Execution started")    
        credential_dict = bu_config.get_config('SUPPLY_STACK','CMESETTLE')
        processname =  credential_dict['PROJECT_NAME']
        databasename = credential_dict['DATABASE']
        # databasename = 'POWERDB_DEV'
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        url = credential_dict['SOURCE_URL']
        process_owner = credential_dict['IT_OWNER']
        receiver_email = credential_dict['EMAIL_LIST']
        business_email = receiver_email.split(';')[0]
        it_email = receiver_email.split(';')[1]
        # receiver_email = "Mrutunjaya.Sahoo@biourja.com,radha.waswani@biourja.com"

        logger.info("All the credential details fetched from creential dict")

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        files_location = os.getcwd() + '\\download'
        executable_path = os.getcwd() + "\\geckodriver\\geckodriver.exe"

        logger.info("Calling remove_existing_files function")
        remove_existing_files(files_location)
        logger.info("Remove existing files completed successfully")
        
        logger.info("Get df function calling")
        df = get_df(files_location)
        logger.info("Get df function completed successfully")

        logger.info("Upload to sf function calling")
        rows = upload_in_sf(tablename, df)
        logger.info("Upload to sf function completed successfully")
        print("Done")

        bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        if datetime.now().time() > Datetime.time(19) and datetime.now().time() < Datetime.time(20) and rows == 0:
            receiver_email = business_email
            subject = f"JOB SUCCESS - {tablename}  NO new data found. Data will be uploaded in next run"
        elif datetime.now().time() > Datetime.time(20) and rows == 0:
            receiver_email = it_email
            subject = f"JOB SUCCESS - {tablename}  NO new data found"
        elif rows > 0:
            subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"
            receiver_email = business_email
        else:
            subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"
            receiver_email = it_email

        bu_alerts.send_mail(
            receiver_email = receiver_email, 
            mail_subject = subject,
            mail_body=f'{tablename} completed successfully, Attached logs',
            attachment_location = logfilename
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logger.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        bu_alerts.send_mail(
            receiver_email = it_email,
            mail_subject = f'JOB FAILED - {tablename}',
            mail_body=f'{tablename} failed during execution, Attached logs',
            attachment_location = logfilename
        )
    
    














