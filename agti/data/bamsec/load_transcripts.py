import datetime
import itertools
import string
import time
# Third-Party Imports
import numpy as np
import pandas as pd
import requests
import selenium
from rauth import OAuth1Service
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import time
from PyPDF2 import PdfReader
import os
from agti.utilities.db_manager import DBConnectionManager

class BamSecUXDriver:
    def __init__(self,pw_map,driver_type):
        self.pw_map= pw_map
        self.local_folder = self.pw_map['local_data_dump']+'bamsec/'
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        if not os.path.exists(self.local_folder):
            os.makedirs(self.local_folder)
        
                #options = Options()
        #options = Options()
        #options.add_argument("-private")
        #firefox_profile = webdriver.FirefoxProfile()
        #firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
        

        # Initialize Chrome WebDriver
        if driver_type=='chrome':
            chrome_options = webdriver.ChromeOptions()

        # Enable Incognito mode
            chrome_options.add_argument("--incognito")
            driver = webdriver.Chrome(options=chrome_options)
        
        if driver_type=='firefox':
            firefox_options = webdriver.FirefoxOptions()
            firefox_options.add_argument("--private")
            driver = webdriver.Firefox(firefox_options=firefox_options)

        if driver_type=='edge':
            edge_options = Options()
            edge_options.add_argument("-inprivate")  # For InPrivate mode

            # Initialize Edge service
            #service = Service(executable_path=driver_path)

            # Initialize Edge WebDriver with options and service
            driver = webdriver.Edge(options=edge_options)#, service=service)

        #firefox_options = webdriver.FirefoxOptions()
       # firefox_options.add_argument("--private")
        #webdriver.Firefox(executable_path='C:\\Users\\goodalexander\\Downloads\\geckodriver-v0.33.0-win64\\geckodriver.exe',
        #                                        firefox_profile=firefox_profile,firefox_options=firefox_options)
        self.driver = driver


    def loginToPage(self):
        self.driver.get('https://www.bamsec.com/login?next=%2F')
        time.sleep(1)
        self.driver.find_element(By.ID, "username").send_keys(self.pw_map['bamsec_email'])
        
        # Locate the password field by its id and send a value
        self.driver.find_element(By.ID, "password").send_keys(self.pw_map['bamsec_password'])
        
        # Locate and click the submit button (if needed)
        #sign_in_button = self.driver.find_element(By.CLASS_NAME, 'c89c62a67')

        #sign_in_button.click()
        # Wait for the button to be clickable
        wait = WebDriverWait(self.driver, 10)  # 10 seconds timeout
        sign_in_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'][name='action'][value='default']")))
        
        # Click the button
        sign_in_button.click()

    def navigate_to_ticker_homepage(self, ticker_to_work):
        search_box = self.driver.find_element(By.NAME, "q")
        search_box.send_keys(ticker_to_work)
        
        # Locate and click the submit button if needed
        submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        submit_button.click()

    def clear_search_bar(self):
        """
        Clear the search bar on the BamSEC page.
        """
        try:
            # Wait for the search input to be present
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input.form-control.search-autocomplete"))
            )
            
            # Clear the search input
            search_input.clear()
            
            print("Search bar cleared successfully.")
        except Exception as e:
            print(f"Error clearing search bar: {str(e)}")


    
    def navigate_to_transcript_page(self):
        # once you're already on the ticker page
        current_url = self.driver.current_url
        self.driver.get(current_url+'/transcripts/')
    
    def output_next_x_available_transcript_df(self, first_x_elements):
        ''' gets a dataframe of the available transcripts on the page''' 
        transcript_elements = self.driver.find_elements(By.CSS_SELECTOR, '.list-group-item.single-line.transcript')
        
        # Initialize empty list to store rows
        data = []
        
        # Loop through elements and extract data
        for element in transcript_elements[0:first_x_elements]:
            label_left = element.find_element(By.CSS_SELECTOR, '.label-left').text
            label_center = element.find_element(By.CSS_SELECTOR, '.label-center').text.split('\n')[1]
            label_right = element.find_element(By.CSS_SELECTOR, '.label-right').text
            float_right = element.find_element(By.CSS_SELECTOR, '.float-right').text
            href = element.get_attribute('href')
            row = [label_left, label_center, float_right, label_right, href]
            data.append(row)
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=['type', 'description', 'revision_date', 'event_date','url'])
        return df

    def go_to_first_earnings_page(self):
        transcript_df= self.output_next_x_available_transcript_df(first_x_elements=20)
        earnings_calls_only =transcript_df[transcript_df['type']=='Earnings'].copy()
        earnings_url = self.driver.get(list(earnings_calls_only['url'].head(1))[0])
        return earnings_calls_only

    def get_pdf_details(self):
        pdf_element = self.driver.find_element(By.CSS_SELECTOR, 'a.pdf-link')
        
        # Extract the PDF URL
        pdf_url = pdf_element.get_attribute('href')
        
        file_name = pdf_url.split('.pdf')[0].split('/')[-1:][0]+'.pdf'
        return {'pdf_file_name':file_name,
                'pdf_url': pdf_url}

    def write_pdf_per_detail_format_and_go_back(self,pdf_details):
        pdf_file_name = pdf_details['pdf_file_name']
        #list(earnings_calls_only['url'].head(1))[0]
        file_to_write = os.path.join(self.local_folder, pdf_file_name)
        if os.path.isfile(file_to_write) == True:
            print("FILE ALREADY EXISTS")
        
        if os.path.isfile(file_to_write) == False:
            pdf_response = requests.get(pdf_details['pdf_url'])
            
            with open(file_to_write, 'wb') as f:
                f.write(pdf_response.content)
            print('DOWNLOADED PDF')
        self.driver.back()

    # Your existing folder
    #local_folder = self.pw_map['local_data_dump'] + 'bamsec/'
    def process_pdf_file_path_to_output_df(self, pdf_file_path):
    # File path
        
        
        # Initialize an empty string to store text
        pdf_text = ""
        
        # Initialize PdfReader object
        with open(pdf_file_path, 'rb') as file:
            reader = PdfReader(file)
        
            # Loop through each page
            for i in range(len(reader.pages)):
                # Get each page
                page = reader.pages[i]
                
                # Extract text from the page
                page_text = page.extract_text()
                
                # Append the text to the final string
                pdf_text += page_text
        
        # Now pdf_text contains all the text from the PDF
        sp_global_copyright = 'Copyright © 2023 S&P Global Market Intelligence, a division of S&P Global Inc.'
        sp_global_copyright2 = 'S&P Global Market  Intelligence, a division of S&P Global  Inc. All rights reserved'
        sp_global_copyright3 = 'S&P Global Inc.'
        sp_global_copyright4= 'All rights reserved'
        account_manager_bullshit= 'Capability needed to view  estimates data. Please  contact your account \nmanager'
        full_text_resource = pdf_text.split('These materials have been prepared solely for information purposes')[0].replace(sp_global_copyright,'').replace(sp_global_copyright2,'').replace(account_manager_bullshit,'').replace(sp_global_copyright4,'').replace(sp_global_copyright3,'').replace('All Rights reserved','')
        imputed_ticker = full_text_resource.split('\nEarnings Call\n')[0].split(':')[-1:][0]
        date_string = full_text_resource.split('\nEarnings Call\n')[1].split('\n')[0]
        precise_upload_time = pd.to_datetime(date_string)
        quarter = int(full_text_resource.split('FQ')[1].split(' ')[0])
        year = int(full_text_resource.split(f'FQ{quarter}')[1].split(' EARNINGS CALL')[0].strip())
        xdfx= pd.DataFrame({'ticker':imputed_ticker,'upload_time__utc':precise_upload_time,
         'quarter':quarter,'year':year,'raw_transcript':full_text_resource},index=[0])
        return xdfx

    def output_tickers_most_recent_eps_transcript_df(self,ticker_to_work = 'UAL'):
        
        self.navigate_to_ticker_homepage(ticker_to_work=ticker_to_work)
        self.navigate_to_transcript_page()
        earnings_df = self.go_to_first_earnings_page()
        earnings_event_string = pd.to_datetime(list(earnings_df.head(1)['event_date'])[0]).strftime('%Y-%m-%d')
        pdf_details = self.get_pdf_details()
        self.write_pdf_per_detail_format_and_go_back(pdf_details=pdf_details)
        pdf_full_file_path = self.local_folder+pdf_details['pdf_file_name']
        output_df = self.process_pdf_file_path_to_output_df(pdf_file_path=pdf_full_file_path)
        output_df['as_of_time'] = datetime.datetime.now()

        
        output_df['upload_time'] = output_df['upload_time__utc'].dt.tz_convert('US/Eastern')
        output_df['resource_unique_identifier']=output_df['ticker']+'_Earnings_Call_Transcript_'+output_df['upload_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        output_df['document_type']='Earnings_Call_Transcript'
        return output_df

    def write_tickers_most_recent_eps_transcript_df(self,ticker_to_work='BAC'):
        output_df = self.output_tickers_most_recent_eps_transcript_df(ticker_to_work=ticker_to_work)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        output_df.to_sql('bamsec___full_transcripts_raw',dbconnx, if_exists='append', index=False)
        dbconnx.dispose()

    def write_bamsec_earnings_filings_for_ticker_list(self,tickers_to_update = ['JPM','V','NFLX','UAL','SSTK','PANW','TWLO','SEDG','ENPH','C','JNJ'],
                                                      force=False,
                                                      days_stale_since_transcript_max=14,
                                                      days_stale_since_update_max=0):
    
        '''
        days_stale_since_transcript_max references the upload date of the actual transcript
        whereas days_stale_since_update references the update date on our side 
        EXAMPLE:
        tickers_to_update = ['JPM','V','NFLX','UAL','SSTK','PANW','TWLO','SEDG','ENPH','C','JNJ'],
                                                      force=False,
                                                      days_stale_since_transcript_max=14,
                                                      days_stale_since_update_max=0
        
        '''
        
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        updated_as_of_df = pd.read_sql('select * from bamsec___full_transcripts_raw;',dbconnx)
        dbconnx.dispose()
        updated_as_of_df['days_stale_from_update_time']=(datetime.datetime.now()
                                                         -updated_as_of_df['as_of_time']).apply(lambda x: x.days)
        updated_as_of_df['days_stale_from_transcript_date'] = (datetime.datetime.now() 
                                                               - updated_as_of_df['upload_time'].dt.tz_localize(None)).apply(lambda x: x.days)
        #recently_updated = updated_as_of_df[updated_as_of_df['days_stale']<days_stale_max]
        #recently_updated_tickers = list(recently_updated['ticker'])
        recently_updated_per_transcript = updated_as_of_df[updated_as_of_df.days_stale_from_transcript_date <= days_stale_since_transcript_max]
        recently_updated_per_update = updated_as_of_df[updated_as_of_df.days_stale_from_update_time <= days_stale_since_update_max].copy()
        all_updated_tickers = list(set(list(recently_updated_per_transcript['ticker'])+list(recently_updated_per_update['ticker'])))
        remaining_tickers_to_update=tickers_to_update
        if force == False:
            remaining_tickers_to_update = [i for i in tickers_to_update if i not in all_updated_tickers]
            for xticker in remaining_tickers_to_update:
                try:
                    self.write_tickers_most_recent_eps_transcript_df(xticker)
                except:
                    print(f'FAILED WRITING {xticker}')
                    pass

    def process_pdf_file_path_to_raw_pdf_text(self, pdf_file_path):
    # File path
        
        
        # Initialize an empty string to store text
        pdf_text = ""
        
        # Initialize PdfReader object
        with open(pdf_file_path, 'rb') as file:
            reader = PdfReader(file)
        
            # Loop through each page
            for i in range(len(reader.pages)):
                # Get each page
                page = reader.pages[i]
                
                # Extract text from the page
                page_text = page.extract_text()
                
                # Append the text to the final string
                pdf_text += page_text
        return pdf_text

    def generate_recent_update_for_ticker(self,ticker_to_work='NFLX',last_x_days=1):
        self.navigate_to_ticker_homepage(ticker_to_work=ticker_to_work)
        self.navigate_to_transcript_page()
        next_x = self.output_next_x_available_transcript_df(first_x_elements=2)
        next_x['event_date']=pd.to_datetime(next_x['event_date'])
        x_past_days=datetime.datetime.now()-datetime.timedelta(last_x_days)
        update_df = next_x[next_x['event_date']>x_past_days]
        number_of_updates = len(update_df)
        if number_of_updates == 0:
            print("NO NEW UPDATES")
        if number_of_updates > 0:
            recent_update = update_df.head(1)
            type_string = list(recent_update['type'])[0]
            if type_string == 'Earnings':
                type_string='Earnings_Call_Transcript'
            url_to_get = list(recent_update['url'])[0]
            transcript_url = self.driver.get(url_to_get)
            pdf_details = self.get_pdf_details()
            self.write_pdf_per_detail_format_and_go_back(pdf_details=pdf_details)
            pdf_file_name = pdf_details['pdf_file_name']
            update_df=update_df.copy()
            update_df['ticker']=ticker_to_work
            update_df['pdf_file_name']=pdf_file_name
            update_df['local_file_path']= update_df['pdf_file_name'].apply(lambda x: os.path.join(self.local_folder, pdf_file_name))
            update_df['full_transcript_text']=update_df['local_file_path'].apply(lambda x: self.process_pdf_file_path_to_raw_pdf_text(x))
            upload_time = pd.to_datetime(list(update_df['full_transcript_text'])[0].split('GMT\n  \n')[0].split('\n')[-1:][0].strip())
            update_df['upload_time']=upload_time
            update_df['internal_resource_type']= type_string
            full_text_resource = list(update_df['full_transcript_text'])[0]
            quarter=None
            try:
                quarter = int(full_text_resource.split('FQ')[1].split(' ')[0])
            except:
                pass
            
            year = None
            try:
                year = int(full_text_resource.split(f'FQ{quarter}')[1].split(' EARNINGS CALL')[0].strip())
            except:
                pass
            update_df['quarter']=quarter
            update_df['year']=year
            processed_text_resource = full_text_resource.split('These materials have been prepared solely for information purposes')[0]
            processed_text_resource
            kill_strings = ['2023 S&P Global Market Intelligence',
                            '2023 S&P Global Market  Intelligence',
                            'a division of S&P Global  Inc.',
                            'a division of S&P Global Inc.',
                            'All rights reserved',
                            'COPYRIGHT © 2022',
                            'COPYRIGHT © 2023',
                            'COPYRIGHT © 2024',
                            'COPYRIGHT © 2025']
            for xstring in kill_strings:
                processed_text_resource=processed_text_resource.replace(xstring,'')
            update_df['processed_text_resource']=processed_text_resource
            update_df['transcript_code'] = update_df['ticker']+'__earnings_transcript__'+(update_df['upload_time'].astype(str).apply(lambda x: x.split(' ')[0]))
            return update_df


    def get_most_recent_event_calendar(self):
        path_to_dir = self.pw_map['local_data_dump'] + 'bloomberg/event_calendars/'
        all_event_calendars = os.listdir(path_to_dir)
        
        # Sort files by modification time
        all_event_calendars.sort(key=lambda x: os.path.getmtime(os.path.join(path_to_dir, x)))
        
        # Get the most recent file
        most_recent_file = all_event_calendars[-1] if all_event_calendars else None
        return path_to_dir+most_recent_file

    def output_recently_updated_transcripts(self, updated_within_x_days= 7):
        
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        existing_bamsec_transcripts = pd.read_sql(f"SELECT * FROM bamsec___transcripts_raw WHERE upload_time::date > (current_date - interval '{updated_within_x_days} days')", dbconnx)
        dbconnx.dispose()
        return existing_bamsec_transcripts

    def get_most_recent_transcripts(self, tickers):
        """
        Get the most recent transcript for each ticker in the given list.
        
        Args:
        tickers (list): List of ticker symbols to query.
        
        Returns:
        pandas.DataFrame: DataFrame containing the most recent transcript for each ticker.
        """
        tickers_str = "', '".join(tickers)
        query = f"""
        SELECT t.*
        FROM bamsec___full_transcripts_raw t
        INNER JOIN (
            SELECT ticker, MAX(upload_time__utc) as max_upload_time
            FROM bamsec___full_transcripts_raw
            WHERE ticker IN ('{tickers_str}')
            GROUP BY ticker
        ) m ON t.ticker = m.ticker AND t.upload_time__utc = m.max_upload_time
        """
        
        dbconn = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        return pd.read_sql(query, dbconn)