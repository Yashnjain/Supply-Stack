import requests
import pandas as pd
import datetime
import numpy as np
import bu_alerts
import logging
import bu_config
import bu_snowflake
import functools
from snowflake.connector.pandas_tools import pd_writer
import sys



products_info = [{
                'product' : 'WTI',
                'product_code' : 'CL',
                'Product_detail' : 'Light Sweet Crude Oil Futures',
                'site_code' : 425
             },
            {
                'product' : 'BRENT',
                'product_code' : 'CY',
                'Product_detail' : 'Brent Financial Futures',
                'site_code' : 4710
                },
            {
                'product' : 'HEATING_OIL',
                'product_code' : 'HO',
                'Product_detail' : 'Ny Harbor Ulsd Futures' ,
                'site_code' : 426 
            },
            {
                'product' : 'FUEL_OIL',
                'product_code' : 'MM',
                'Product_detail' : 'New York Harbor Residual Fuel 1.0% (platts) Future',
                'site_code' : 4911   
            }
            ]




def get_data(url :str) ->pd.DataFrame:
    """
    Retrieves data from the given URL and returns it as a DataFrame.

    Args:
        url (str): The URL to retrieve data from.

    Returns:
        pd.DataFrame: The retrieved data as a DataFrame.
    """
    try:
        logging.info("Inside get data function")
        response = session.get(url)
        data_dict = response.json()
        df = pd.DataFrame(data_dict['settlements'])
        print(df)
        return df
    except Exception as e:
        logger.exception('Exception while fetching data')
        raise e

def product_mapping(df : pd.DataFrame , date : str ,product : dict)->pd.DataFrame:
    """
    Perform mapping and transformations on the given DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to be mapped.
        date (str): The trade date for the product.
        product (dict): The dictionary containing product information.

    Returns:
        pd.DataFrame: The mapped DataFrame.
    """
    try:
        logger.info("Inside product_mapping func")
        df.insert(0,'PRODUCT_DETAIL',product['Product_detail'])
        df.insert(0,'CONTRACT',np.nan)
        
        df['month'] = df['month'].str.replace('JLY', 'JUL')
        df['month'] = pd.to_datetime(df['month'], format='%b %y')
        df.insert(0,'FLOW_YEAR',df['month'].dt.year)
        df.insert(0,'FLOW_MONTH',df['month'].dt.month)
        df.insert(0,'PRODUCT',product['product'])
        df.insert(0,'PRODUCT_CODE',product['product_code'])
        df.insert(0,'TRADEDATE',date)
        return df
    except Exception as e:
        logger.exception("Exception during Product Mapping") 
        raise e
    
def format_dataframe(df : pd.DataFrame)->pd.DataFrame:
    """
    Format the given DataFrame by performing specific transformations.

    Args:
        df (pd.DataFrame): The input DataFrame to be formatted.

    Returns:
        pd.DataFrame: The formatted DataFrame.
    """
    try:
        logger.info("Inside format_dataframe func")
        df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'], format='%m/%d/%Y')
        df['TRADEDATE'] = df['TRADEDATE'].dt.strftime('%Y-%m-%d')
        df.columns = [col.upper() for col in df.columns]
        df = df.rename(columns={'VOLUME':'EST_VOL','OPENINTEREST' : 'PRIOR_INT'})
        df['INSERT_DATE'] = str(datetime.datetime.now())
        df['UPDATE_DATE'] = str(datetime.datetime.now())
        df = df[['TRADEDATE', 'PRODUCT_CODE', 'PRODUCT', 'FLOW_MONTH', 'FLOW_YEAR',
        'CONTRACT', 'PRODUCT_DETAIL', 'OPEN', 'HIGH', 'LOW', 'LAST',
            'SETTLE', 'EST_VOL', 'PRIOR_INT', 'INSERT_DATE',
        'UPDATE_DATE']]
        df['OPEN'] = df['OPEN'].str.replace('[A-Za-z]', '')
        df['HIGH'] = df['HIGH'].str.replace('[A-Za-z]', '')
        df['LOW'] = df['LOW'].str.replace('[A-Za-z]', '')
        df['LAST'] = df['LAST'].str.replace('[A-Za-z]', '')
        df['PRIOR_INT'] = df['PRIOR_INT'].str.replace(',', '')
        df['EST_VOL'] = df['EST_VOL'].str.replace(',', '')
        df = df.replace('-',np.nan)
        return df
    except Exception as e:
        logger.exception("Exception during Dataframe formatting")



def upload_in_sf(df):
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
        
        
        
def main()-> int:
    """
    Main function to retrieve data for each product and perform mapping and upload to sf.
    
    Returns :
        Number of rows inserted.
    """
    try:
        list_df = []
        
        for product in products_info:
            site_code = product['site_code']
            # Construct the URL for retrieving the date
            date_url = f'https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/TradeDate/{site_code}'
            date = session.get(date_url).json()[1][0]
            # Construct the URL for retrieving the data
            data_url = f'https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/{site_code}/FUT?strategy=DEFAULT&tradeDate={date}'
            data_df = get_data(data_url)
            mapped_df = product_mapping(data_df[:-1],date,product)
            print(len(mapped_df))
            list_df.append(mapped_df)

        combined_df = pd.concat(list_df,ignore_index=True)    
        final_df = format_dataframe(combined_df)
        total_rows = upload_in_sf(final_df)
        return total_rows
    except Exception as e:
        logger.exception('Exception in main func')
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
            schemaname = credential_dict['TABLE_SCHEMA']
            tablename = credential_dict['TABLE_NAME']
            process_owner = credential_dict['IT_OWNER']
            receiver_email = credential_dict['EMAIL_LIST']
            business_email = receiver_email.split(';')[0]
            it_email = receiver_email.split(';')[1]
            # receiver_email = it_email = business_email = "rahul.sakarde@biourja.com,ayushi.joshi@biourja.com,abhisar.shrivastava@biourja.com"

            logger.info("All the credential details fetched from creential dict")

            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.datetime.now())+'"}]'

            bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
                row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
            session = requests.Session()
            headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.76'}
            session.headers.update(headers)
            rows = main()
            bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
                row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

            if rows == 0:
                subject = f"JOB SUCCESS - {tablename}  NO new data found. Data will be uploaded in next run"
                # subject = f"JOB SUCCESS - TEST:{tablename}  NO new data found. Data will be uploaded in next run"
            else:
                subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"
                # subject = f"JOB SUCCESS - TEST:{tablename}  inserted {rows} rows"
                
            bu_alerts.send_mail(
                receiver_email = receiver_email, 
                mail_subject = subject,
                mail_body=f'{tablename} completed successfully, Attached logs',
                attachment_location = logfilename
            )
            logger.info("success mail send successfully")
    except Exception as e:
        print("Exception caught during execution: ",e)
        # logging.exception(f'Exception caught during execution: {e}')
        # log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.datetime.now())+'"}]'
        # bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
        #     row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        # bu_alerts.send_mail(
        #     receiver_email = it_email,
        #     mail_subject = f'JOB FAILED - {tablename}',
        #     mail_body=f'{tablename} failed during execution, Attached logs',
        #     attachment_location = logfilename
        # )
        logger.info("Failure mail send successfully")
    finally:
        session.close()
        logger.info("Request Session Close")
        sys.exit()
        