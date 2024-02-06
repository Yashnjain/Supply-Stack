import pandas as pd
import numpy as np
import requests
from datetime import datetime, date
import datetime as Datetime
import logging
import bu_alerts
import bu_config
import bu_snowflake
from functools import reduce
import functools
from snowflake.connector.pandas_tools import pd_writer


def get_max_market_date():
    try:
        logger.info("Getting max market date from table")
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        with engine.connect() as con:
            val = con.execute(f"select max(MARKET_DATE) from {databasename}.{schemaname}.{tablename}").fetchone()
            max_market_date = val[0]
            logger.info(f"Max market_date fetched and the date is {max_market_date}")

        return max_market_date

    except Exception as e:
        logger.exception(e)
        print(e)
        raise e

def get_df_from_url(url,max_market_date):
    try:
        res = requests.get(url)
        data = res.json()
        df = pd.DataFrame(data['response']['data'])
        df = df[['period','product-name','duoarea','value']]
        logger.info(f"Dataframe created successfully")
        df.period = pd.to_datetime(df.period,format="%Y-%m-%d").dt.date
        df = df[df.period > max_market_date]
        wti = df[df['product-name'] == 'WTI Crude Oil'][['period','value']].reset_index()[['period','value']] \
            .rename(columns= {'period':'MARKET_DATE','value':'WTI'})
        brent = df[df['product-name'] == 'UK Brent Crude Oil'][['period','value']].reset_index()[['period','value']] \
            .rename(columns= {'period':'MARKET_DATE','value':'BRENT'})
        heating_oil = df[df['product-name'] == 'No 2 Fuel Oil / Heating Oil'][['period','value']].reset_index()[['period','value']] \
            .rename(columns= {'period':'MARKET_DATE','value':'HEATING_OIL'})
        fuel_oil = df[(df['product-name'] == 'No 2 Diesel Low Sulfur (0-15 ppm)') & (df['duoarea'] == 'Y35NY')][['period','value']]\
            .reset_index()[['period','value']].rename(columns= {'period':'MARKET_DATE','value':'FUEL_OIL'})
        print(f"Dataframe fetched successfuly for the url {url}")
        return [wti, brent, heating_oil, fuel_oil]
    except Exception as e:
        logger.exception(e)
        raise e

def get_main_df(url,max_market_date):
    try:
        dfs_list = get_df_from_url(url,max_market_date)
        logger.info("Dfs list created for all the APIs")
        main_df = reduce(lambda  left,right: pd.merge(left,right,on=['MARKET_DATE'],
                                                how='outer'), dfs_list)

        logger.info("Main_df created after merging all the dataframes")
        
        if not main_df.empty:
            logger.info("Data found for the dataframe so insert_date and update_date added")
            main_df['INSERT_DATE'] = str(datetime.now())
            main_df['UPDATE_DATE'] = str(datetime.now())
        else:
            logger.info("No data found for the dataframe")
        logger.info("Main dataframe created")
        return main_df
    except Exception as e:
        print(e)
        logger.exception(e)
        raise e

def upload_in_sf(tablename, df):
    logger.info("Inside upload_in_sf function")
    total_rows = 0
    try:
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        with engine.connect() as conn:
            logger.info("Engine object created successfully")

            temp_tablename = f"TEMP_{tablename}"
            temp_table_query = f"""
                            create or replace temporary TABLE {databasename}.PTEMP.{temp_tablename} (
                            MARKET_DATE DATE NOT NULL,
                            WTI NUMBER(6,3),
                            BRENT NUMBER(6,3),
                            HEATING_OIL NUMBER(7,6),
                            FUEL_OIL NUMBER(7,6),
                            INSERT_DATE TIMESTAMP_NTZ(9),
                            UPDATE_DATE TIMESTAMP_NTZ(9),
                            primary key (MARKET_DATE)
                        );
                """
            conn.execute(temp_table_query)
            logger.info("Temporary table created successfully")
            df.to_sql(temp_tablename.lower(), 
                    con=conn,
                    index=False,
                    if_exists='append',
                    schema="PTEMP",
                    method=functools.partial(pd_writer, quote_identifiers=False)
                    )

            merge_query = f'''merge into {databasename}.{schemaname}.{tablename} t using {databasename}.PTEMP.{temp_tablename} s 
                            on t.MARKET_DATE = s.MARKET_DATE
                            when matched then
                            update
                            set 
                                t.MARKET_DATE = s.MARKET_DATE,
                                t.WTI = s.WTI,
                                t.BRENT = s.BRENT,
                                t.HEATING_OIL = s.HEATING_OIL,
                                t.FUEL_OIL = s.FUEL_OIL,
                                t.UPDATE_DATE = s.UPDATE_DATE
                                when not matched then
                            insert (
                                    MARKET_DATE,
                                    WTI,
                                    BRENT,
                                    HEATING_OIL,
                                    FUEL_OIL,
                                    INSERT_DATE,
                                    UPDATE_DATE
                                )
                            values (
                                    s.MARKET_DATE,
                                    s.WTI,
                                    s.BRENT,
                                    s.HEATING_OIL,
                                    s.FUEL_OIL,
                                    s.INSERT_DATE,
                                    s.UPDATE_DATE
                                )'''
            res = conn.execute(merge_query).fetchall()[0][0]

            logger.info(f"{res} number of rows updated")
                
        return res
    except Exception as e:
        logger.exception("Exception while inserting data into snowflake")
        logger.exception(e)
        raise e


if __name__ == "__main__":
    try:
        # logger created and all the basic configuration done and the logger is used in the code to keep track of code

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logfilename = bu_alerts.add_file_logging(logger,process_name='EIA_PETROLEUM_DAILY_SPOT_PRICES')
        

        logger.info("Execution started")

        job_id=np.random.randint(1000000,9999999)
        credential_dict = bu_config.get_config('SUPPLY_STACK','EIA_PETROLEUM_DAILY_SPOT_PRICES')
        processname = credential_dict['PROJECT_NAME']
        databasename = credential_dict['DATABASE']
        # databasename = 'POWERDB_DEV'
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        url = credential_dict['SOURCE_URL'] 
        api_key = credential_dict["API_KEY"]

        main_url = eval("f'{}'".format(url))
        process_owner = credential_dict['IT_OWNER'] 
        receiver_email = credential_dict['EMAIL_LIST']
        business_email = receiver_email.split(';')[0]
        it_email = receiver_email.split(';')[1]
        # receiver_email = "Mrutunjaya.Sahoo@biourja.com,radha.waswani@biourja.com"

        logger.info("All the credential details fetched from creential dict")

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        
        logger.info("Calling get_max_market_date function")
        max_market_date = get_max_market_date()
        logger.info("get_max_market_date completed successfully")

        logger.info("Getting main_df")
        df = get_main_df(main_url,max_market_date)
        logger.info("Main_df fetched successfully")

        logger.info("Upload to sf function calling")
        rows = upload_in_sf(tablename, df)
        logger.info("Upload sf function executed successfully")

        bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        if datetime.today().strftime("%A") == 'Wednesday' and rows == 0:
            receiver_email = business_email
            subject = f"JOB SUCCESS - TEST:{tablename}  NO new data found. Data will be uploaded in next run"
        elif datetime.today().strftime("%A") == 'Thursday' and rows == 0:
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
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        bu_alerts.send_mail(
            receiver_email = it_email,
            mail_subject = f'JOB FAILED - {tablename}',
            mail_body=f'{tablename} failed during execution, Attached logs',
            attachment_location = logfilename
        )
    
    