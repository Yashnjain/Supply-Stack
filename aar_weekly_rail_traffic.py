from distutils.command.upload import upload
import os
import re
from typing import final
from unicodedata import name
import pandas as pd
import numpy as np
from datetime import datetime,date,timedelta
import bu_alerts
import bu_snowflake
import bu_config
import logging
from pip import main
from snowflake.connector.pandas_tools import pd_writer
import functools
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import tabula

def remove_existing_files(files_location):
    '''
        Takes the download location as the argument and deletes the existing files from there.

        Params:
        ------
        Files_location: str
            Path of the download folder
    '''
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

def download_data(url,files_location,executable_path):
    '''
        Takes the url, download folder path and geckdriver path as argument and pdf is downloaded in this process via automation from website.

        Params:
        -------
        url:str
            The value of the website url which is hit to download the pdf
        files_location:str
            Download location folder path where the pdfs needs to be downloaded.
        executable_path:str
            The path of the geckodriver which is used in automation

        Returns:
        --------
        None        
    '''
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

        browser.get(url)
        browser.maximize_window()
        logger.info("Website opened")
        browser.find_element(By.XPATH,'/html/body/div[1]/main/article/div/div/ul/li[1]/a/div[2]').click()
        time.sleep(3)
        browser.find_element(By.CSS_SELECTOR,'.wp-block-button__link > strong:nth-child(1)').click()
        time.sleep(7)
        logger.info("File downloaded successfully")

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


def  extract_pdf(file,i,read_area):
    ''' 
        Takes file, a range value for each page and the coordinate area as the argument and all these details are used to extract pdfs and 
        a meaningful dataframe is created.

        Params:
        -------
        file:str
            Name of the file for which the pdf needs to be extracted.
        i: int
            Page number for each page
        read_area:list
            A list with coordinate details for each axis for the file

        Returns:
        --------
        main_df:Dataframe
            The dataframe to be inserted in the Database
    '''
    logger.info("Inside extract_pdf function")
    try:
        main_df = tabula.read_pdf(file, multiple_tables=True,pages=i,silent=True,area = read_area)[0]
        logger.info("Initial dataframe created as main_df from which other details needs to be extracted")
        if len(main_df.columns) == 5:
            logger.info("The column length is 5 in this case")
            region = main_df.iloc[0,1].split(' Rail')[0]
            week_ending_str = main_df.iloc[1,1].split('Ended ')[-1]
            week_ending_date = str(datetime.strptime(week_ending_str,'%B %d, %Y').date())
            logger.info("Region and week ending date extracted")
            first_index = main_df[main_df['Unnamed: 0'] == 'Total Carloads'].index.to_list()[0]
            last_index = main_df[main_df['Unnamed: 0'] == 'Total Traffic'].index.to_list()[0]
            main_df = main_df.iloc[first_index:last_index+1]
            logger.info("Main dataframe  recreated on the basis of required indices")
            
            this_week_cars = main_df.iloc[:,1].apply(lambda x: x.split(' ')[0])
            prev_year_cars_pct = main_df.iloc[:,1].apply(lambda x: x.split(' ')[1].strip('%'))
            main_df.iloc[:,4] = main_df.iloc[:,4].apply(lambda x : x.strip('%'))
            logger.info("This_week_cars and prev_year_cars_pct column created to be inserted and percentage removed from the values")
            try:
                logger.info("Trying to extract ytd_cum and ytd_avg_per_week values incase if the values are merged in third column")
                ytd_cum = main_df.iloc[:,2].apply(lambda x: x.split(' ')[0])
                ytd_avg_per_week = main_df.iloc[:,2].apply(lambda x: x.split(' ')[1])
                main_df.drop(columns = [main_df.columns[2], main_df.columns[3]],inplace = True)
                main_df.insert(2,'YTD_CUM',ytd_cum)
                main_df.insert(3,'YTD_AVG_PER_WEEK',ytd_avg_per_week)
                logger.info("ytd_cum and ytd_avg_per_week extracted and inserted in the dataframe")
            except Exception as e:
                logger.info("No ytd_cum and ytd_avg_per_week value found merged in third column so error occurred. It will be left as it is")
                pass
            main_df.drop(columns = main_df.columns[1], inplace = True)
            main_df.insert(1,'THIS_WEEK_CARS',this_week_cars)
            main_df.insert(2,'PREV_YEAR_CARS_PCT',prev_year_cars_pct)
            logger.info("This_week_cars and prev_year_cars_pct inserted in the dataframe")
        elif len(main_df.columns) == 4:
            logger.info("The column length is 4 in this case")
            try:
                logger.warning("Trying to extract region and week_ending from column names")
                region = main_df.columns.to_list()[1].split(' Rail')[0]
                week_ending_str = main_df.iloc[0,1].split('Ended ')[-1]
                week_ending_date = str(datetime.strptime(week_ending_str,'%B %d, %Y').date())
                logger.info("Region and week_ending exracted from column name")
            except ValueError:
                logger.warning("Value error occurred due to values can't be found from coumn names so extracting from the first row of df")
                region = main_df.iloc[0,1].split(' Rail')[0]
                week_ending_str = main_df.iloc[1,1].split('Ended ')[-1]
                week_ending_date = str(datetime.strptime(week_ending_str,'%B %d, %Y').date())
                logger.info("Region and week_ending extracted from the first row of the dataframe")

            first_index = main_df[main_df['Unnamed: 0'] == 'Total Carloads'].index.to_list()[0]
            last_index = main_df[main_df['Unnamed: 0'] == 'Total Traffic'].index.to_list()[0]
            main_df = main_df.iloc[first_index:last_index+1]
            logger.info("Main dataframe  recreated on the basis of required indices")
            this_week_cars = main_df.iloc[:,1].apply(lambda x: x.split(' ')[0])
            prev_year_cars_pct = main_df.iloc[:,1].apply(lambda x: x.split(' ')[1].strip('%'))
            logger.info("This_week_cars and prev_year_cars_pct extracted from second column by splitting the column")
            try:  
                ytd_cum = main_df.iloc[:,1].apply(lambda x: x.split(' ')[2])
                ytd_avg_per_week = main_df.iloc[:,2]
                logger.info("Ytd_cum extracted from second column by picking the third value after splitting and ytd_avg_per_week is the \
                    third column")
            except IndexError:
                ytd_cum = main_df.iloc[:,2].apply(lambda x: x.split(' ')[0])
                ytd_avg_per_week = main_df.iloc[:,2].apply(lambda x: x.split(' ')[1])
                logger.info("Ytd_cum and ytd_avg_per_week extracted from the third column by spliting the column")

            main_df.iloc[:,3] = main_df.iloc[:,3].apply(lambda x : x.strip('%'))
            main_df.drop(columns = [main_df.columns[1],main_df.columns[2]], inplace = True)
            main_df.insert(1,'THIS_WEEK_CARS',this_week_cars)
            main_df.insert(2,'PREV_YEAR_CARS_PCT',prev_year_cars_pct)
            main_df.insert(3,'YTD_CUM',ytd_cum)
            main_df.insert(4,'YTD_AVG_PER_WEEK',ytd_avg_per_week)
            logger.info("This_week_cars, prev_year_cars_pct, ytd_cum and ytd_avg_per_week inserted in the dataframe")

        main_df.columns = ['TRAFFIC_TYPE','THIS_WEEK_CARS','PREV_YEAR_CARS_PCT','YTD_CUM','YTD_AVG_PER_WEEK','YTD_PREV_YEAR_PCT']
        logger.info("Columns renamed in the dataframe with the required column names")
        main_df['THIS_WEEK_CARS'] = main_df['THIS_WEEK_CARS'].apply(lambda x: x.replace(',',''))
        logger.info("',' removed from this_week_cars column")
        try:
            main_df['YTD_CUM'] = main_df['YTD_CUM'].apply(lambda x: x.replace(',',''))
            logger.info("',' removed from ytd_cum column")
        except AttributeError:
            logger.warning("Attribute error occurred while removing ',' from ytd_column due to some part of the values must be \
                appended in the next column that is ytd_avg_per_week")
            temp_df = main_df[main_df['YTD_AVG_PER_WEEK'].apply(lambda x: len(x.split(' ')) == 2)]
            logger.info("Temp_df created for the values appended in ytd_avg_per_week from utd_cum separated by space having length is 2")
            temp_ytd_cum = temp_df['YTD_AVG_PER_WEEK'].apply(lambda x: x.split(" ")[0]) 
            temp_ytd_avg_per_week = temp_df['YTD_AVG_PER_WEEK'].apply(lambda x: x.split(" ")[1])
            logger.info("Temp_ytd_cum and temp_ytd_avg_per_week extracted from the temp_df")
            temp_df.drop(columns = ['YTD_CUM','YTD_AVG_PER_WEEK'], inplace = True)
            temp_df.insert(3,'YTD_CUM',temp_ytd_cum)
            temp_df.insert(4,'YTD_AVG_PER_WEEK',temp_ytd_avg_per_week)
            logger.info("Ytd_cum and Ytd_avg_per_week column inserted with the temp values in temp_df")
            main_df.update(temp_df)
            main_df['YTD_CUM'] = main_df['YTD_CUM'].apply(lambda x: x.replace(',',''))
            logger.info("Main_df updated with the temp_df values and ',' removed from the ytd_cum column")

        main_df['YTD_AVG_PER_WEEK'] = main_df['YTD_AVG_PER_WEEK'].apply(lambda x: x.replace(',',''))
        logger.info("',' removed in ytd_avg_per_week column")
        main_df['PREV_YEAR_CARS_PCT'] = main_df['PREV_YEAR_CARS_PCT'].apply(lambda x: x if x != "–" else np.nan).astype(float)
        main_df['YTD_PREV_YEAR_PCT'] = main_df['YTD_PREV_YEAR_PCT'].apply(lambda x: x if x != "–" else np.nan).astype(float)
        logger.info("Prev_year_cars_pct and ytd_prev_year_pct column converted into int type and and null inserted in place of '-'")
        main_df.insert(0,'WEEK_ENDING',week_ending_date)
        main_df.insert(1,'REGION',region)
        logger.info("Week_ending and region inserted in the dataframe")
        logger.info("Dataframe is returned from the function")
        return main_df
    except Exception as e:
        logger.exception(e)
        raise e

def extract_and_upload_df(files_location):
    '''
        Iterates through all the files in the download folder and calls extract_pdf function from there and only the cordinates to be extracted 
        for each file is passed in this function. Dataframe is created from extract_pdf function and then upload to sf function is called
        for each file.

        Parmas:
        -------
        files_location:str
            Location of the folder where all the files are stored.

        Returns:
        --------
        rows:int
            Number of rows uploaded for all the file
    '''
    try:
        rows = 0
        files = os.listdir(files_location)
        logger.info("Inside extract and pdf function")

        for file in files:
            area_1 = [24.863,17.178,342.338,610.818]
            area_2 = [21.037,70.728,355.342,594.753]
            area_3 = [11.857,63.078,347.692,591.693]
            area_4 = [18.742,52.368,350.752,592.458]
            all_area = [area_1,area_2,area_3,area_4]
            logger.info("All the cordinates for the pdf are fetched in the list")
            pages = 4
            df_list = []
            for i in range(1,pages+1):
                logger.info(f"Calling extract_pdf function for {file} for page {i}")
                df = extract_pdf(files_location + '\\' + file,i,all_area[i-1])
                logger.info(f"Dataframe created for {file} for page {i}")
                df_list.append(df)
            
            logger.info("Data read from pdf")

            final_df = pd.concat(df_list, ignore_index=True)
            logger.info(f"Final dataframe created for file {file}")

            final_df['INSERT_DATE'] = str(datetime.now())
            final_df['UPDATE_DATE'] = str(datetime.now())
            logger.info("Insert date and update date added in the final dataframe")
            logger.info(f"Calling upload in sf function for {file}")
            rows += upload_in_sf(final_df)
            logger.info(f"{rows} rows uploaded for the file {file}")

        return rows
    except Exception as e:
        logger.exception(e)
        raise e

def upload_in_sf(df):
    """
        Takes the final Dataframe as argument and uploads the data in snowflake. This function checks on the basis
        of week ending column and uploads if data does not exist for that day.

        Params:
        -------
        df: Dataframe
            The final dataframe which to be uploaded in snowflake.
        Returns:
        total_rows:int
            The number of rows inserted in the from the insert-update query
    """
    logger.info("Inside upload_in_sf function")
    total_rows = 0
    week_ending = df.WEEK_ENDING[0]
    try:
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        conn = engine.connect()
        logger.info("Engine object created successfully")

        check_query = f"select * from {databasename}.{schemaname}.{tablename} where WEEK_ENDING = '{week_ending}'"
        check_rows = conn.execute(check_query).fetchall()
        if len(check_rows) > 0:
            logger.info(f"Data already exists for the day {week_ending}")
        else:
            logger.info(f"No rows found for data after {week_ending}")
            df.to_sql(tablename.lower(), 
                    con=engine,
                    index=False,
                    if_exists='append',
                    schema=schemaname,
                    method=functools.partial(pd_writer, quote_identifiers=False)
                    )
            logger.info(f"Dataframe Inserted into the table {tablename} for WEEK_ENDING {week_ending} and rows inserted {len(df)}")
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
        logfilename = bu_alerts.add_file_logging(logger,process_name= 'aar_weekly_rail_traffic')

        logger.info("Execution started")    
        credential_dict = bu_config.get_config('SUPPLY_STACK','AAR_WEEKLY_RAIL_TRAFFIC')
        processname =  credential_dict['PROJECT_NAME']
        databasename = credential_dict['DATABASE']
        # databasename = 'POWERDB_DEV'
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        url = credential_dict['SOURCE_URL']
        process_owner = credential_dict['IT_OWNER']
        receiver_email = credential_dict['EMAIL_LIST']
        # receiver_email = "Mrutunjaya.Sahoo@biourja.com, ayushi.joshi@biourja.com"

        logger.info("All the credential details fetched from creential dict")

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)


        logger.info("Url formated with the year, month and date details")

        files_location = os.getcwd() + '\\download_rail'
        executable_path = os.getcwd() + "\\geckodriver\\geckodriver.exe"

        logger.info("Calling remove_existing_files function")
        remove_existing_files(files_location)
        logger.info("Remove existing files completed successfully")

        logger.info("Calling download data function")
        download_data(url,files_location,executable_path)
        logger.info("Download data function completed")
        
        logger.info("Get df function calling")
        rows = extract_and_upload_df(files_location)
        logger.info("Get df function completed successfully")


        bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        if rows > 0:
            subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"
        else:
            subject = f"JOB SUCCESS - {tablename}  NO new data found"

        bu_alerts.send_mail(
            receiver_email = receiver_email, 
            mail_subject = subject,
            mail_body=f'{tablename} completed successfully, Attached logs',
            attachment_location = logfilename
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logger.exception(f'Exception caught during execution: {e}')
        logger.exception(e)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject = f'JOB FAILED - {tablename}',
            mail_body=f'{tablename} failed during execution, Attached logs',
            attachment_location = logfilename
        )
    
    






