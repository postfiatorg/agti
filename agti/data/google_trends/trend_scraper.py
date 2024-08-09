import hashlib
import time
from glob import glob
import shutil 
from agti.utilities.scraping import ScrapingFileManager
import sqlalchemy
from agti.utilities.db_manager import DBConnectionManager
from selenium import webdriver
import datetime
import requests
import pandas as pd
import numpy as np
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import os
import re
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from urllib.parse import quote_plus


class GoogleTrendsScraper:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        scraper = self.pw_map['gtrends_firefox_scraper']
        self.profile_path =scraper
        self.download_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
        self.driver = self._setup_driver()
        self.target_path = self.pw_map['local_data_dump']
        
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
    def _setup_driver(self):
        firefox_profile = webdriver.FirefoxProfile(self.profile_path)
        # Set preferences for file download
        #firefox_profile.set_preference("browser.download.folderList", 2) # Use custom download directory (2)
        #firefox_profile.set_preference("browser.download.manager.showWhenStarting", False)
        #firefox_profile.set_preference("browser.download.dir", self.download_dir)
        #firefox_profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

        firefox_options = Options()
        firefox_options.profile = firefox_profile
        service = Service(executable_path=GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
        return driver

    
    def construct_url(self, terms, country='US', time_frame='today 3-m'):
        base_url = "https://trends.google.com/trends/explore"
        encoded_terms = ','.join([quote_plus(term.lower()) for term in terms])
        geo_param = '' if country.lower() == 'worldwide' else f"&geo={country}"
        url = f"{base_url}?date={quote_plus(time_frame)}{geo_param}&q={encoded_terms}&hl=en-US"
        return url

    def navigate_to_trends(self, terms, country='US', time_frame='today 3-m'):
        url = self.construct_url(terms, country, time_frame)
        self.driver.get(url)

    def generate_filename(self, terms, country, time_frame):
        """
        Generates a unique filename for the query to cache results.

        The filename is a hash of the search terms, country, and time frame,
        ensuring uniqueness for different queries.
        """
        # Create a consistent string representation of the query
        query_string = f"{'_'.join(terms)}_{country}_{time_frame}"
        # Use MD5 hash to generate a unique filename
        filename = hashlib.md5(query_string.encode('utf-8')).hexdigest()
        return f"{filename}.html"

    def close(self):
        self.driver.quit()


    def click_export_csv(self):
        """
        Clicks the "Export CSV" button on the Google Trends page to download the CSV file.
        """
        try:
            # Wait for the export button to be clickable
            export_button = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.widget-actions-item.export'))
            )
            export_button.click()
            print("Clicked the 'Export CSV' button.")
        except Exception as e:
            print(f"Error clicking the 'Export CSV' button: {e}")
            

    def wait_for_download_complete(self, expected_filename_contains, timeout=30):
        """
        Waits for the download to complete by checking for the most recent file that contains
        the expected filename substring in the default download directory. Assumes that the newest
        file matching the criteria is the desired download.
    
        Args:
        - expected_filename_contains (str): Substring to identify the relevant file.
        - timeout (int): Maximum time to wait for the download to complete, in seconds.
    
        Returns:
        - str: Path to the most recently downloaded file that matches the criteria.
        """
        print(f"Waiting for file containing '{expected_filename_contains}' to download...")
        end_time = time.time() + timeout
        most_recent_file = None
        while True:
            if time.time() > end_time:
                if most_recent_file:
                    return most_recent_file  # Return the most recent file if found
                else:
                    raise Exception("Timeout waiting for download to complete")
            downloaded_files = [f for f in glob(os.path.join(self.download_dir, '*')) if expected_filename_contains in f and not f.endswith(('.part', '.crdownload'))]
            if downloaded_files:
                # Sort files by modification time in descending order; newest first
                downloaded_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
                most_recent_file = downloaded_files[0]  # Pick the newest file
                if os.path.exists(most_recent_file):
                    print(f"Download detected: {most_recent_file}")
                    # Wait a bit longer to ensure the file is fully written and released
                    time.sleep(2)
                    return most_recent_file
            time.sleep(1)  # Wait and retry if no matching files found yet

    
    
    def rename_and_move_download(self, downloaded_file_path, new_path, retries=5, delay=2):
        """
        Tries to rename and move a downloaded file to a new path, retrying up to 'retries' times
        with a 'delay' seconds delay between attempts.
    
        Args:
        - downloaded_file_path (str): The current path of the downloaded file.
        - new_path (str): The target path to move and rename the file to.
        - retries (int): Number of retry attempts.
        - delay (int): Delay in seconds between retries.
        """
        for attempt in range(retries):
            try:
                shutil.move(downloaded_file_path, new_path)
                print(f"File moved and renamed to: {new_path}")
                break  # Exit the loop if move was successful
            except PermissionError as e:
                print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)  # Wait before retrying
                else:
                    raise  # Reraise the last exception if all retries fail
    def safe_rename_and_move_download(self, downloaded_file_path, new_path, retries=5, delay=2):
        for attempt in range(retries):
            try:
                # Copy then delete instead of move
                shutil.copy2(downloaded_file_path, new_path)
                os.remove(downloaded_file_path)  # Remove the original file
                print(f"File copied and original deleted: {new_path}")
                break
            except PermissionError as e:
                print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise

    def load_google_trends_df_for_terms(self, terms = ['sloth','koala','kangaroo'],
                                        country = 'US',time_frame = 'now 7-d'):
        """
        Constructs a Google Trends URL with proper encoding and parameters.
        Valid Time frames 
        today 3-m
        now 7-d
        today 5-y
        
        Valid Countries:
        US
        worldwide 
        
        Args:
        - terms (list of str): A list of search terms.
        - country (str): The country code (e.g., 'US') or 'worldwide' for global trends.
        - time_frame (str): The time frame for the trend data.
        
        Returns:
        - str: The constructed Google Trends URL.
        """
        # Example terms and setup
        
        self.navigate_to_trends(terms, country, time_frame)
        
        # Allow some time for the page to load before clicking the export button
        # time.sleep(2.5)  # Adjust based on your connection speed and response time
        
        # Click the 'Export CSV' button to start the download
        self.click_export_csv()
        
        # The expected filename part could be tricky since Google Trends names its downloads in a specific format.
        # You might not know this ahead of time without seeing the format. If you know the pattern or a unique part of it,
        # use that in the next step. Otherwise, you may need to adjust this logic to handle the naming convention used by Google Trends.
        expected_filename_part = 'Timeline'  # This is a placeholder; adjust based on actual filenames downloaded by Google Trends
        
        # Wait for the download to complete
        downloaded_file_path = self.wait_for_download_complete(expected_filename_part)
        df_to_write = pd.read_csv(downloaded_file_path,skiprows=1)
        periodicity = df_to_write.columns[0]
        col_converter = {'Time':'datetime_of_trend','Day':'datetime of trend','Week':'datetime_of_trend', 'Month':'datetime_of_trend'}
        mapped_columns = [col_converter.get(i, i) for i in df_to_write.columns]
        df_to_write.columns=mapped_columns
        file_name= '_'.join(terms)+'__'+country+'__'+time_frame
        raw_gdf= pd.DataFrame(df_to_write.set_index('datetime_of_trend').stack())
        raw_gdf.columns=['value']
        raw_gdf.index.names=['date_of_trend','verbose_term']
        raw_gdf['date_of_pull']=datetime.datetime.now()
        raw_gdf.reset_index(inplace=True)
        raw_gdf['region']=raw_gdf['verbose_term'].apply(lambda x: x.split('(')[1].replace(')',''))
        raw_gdf['term']=raw_gdf['verbose_term'].apply(lambda x: x.split(':')[0])
        raw_gdf['query']=file_name
        raw_gdf['value']=raw_gdf['value'].apply(lambda x: str(x).replace('<','')).astype(float)
        raw_gdf['date_of_trend']=pd.to_datetime(raw_gdf['date_of_trend'])
        raw_gdf['periodicity']=periodicity
        return raw_gdf

    def write_and_output_google_trends_df_for_terms(self, terms = ['sloth','koala','kangaroo'],
                                            country = 'US',time_frame = 'now 7-d'):
    
        """
        Constructs and writes a Google Trends URL with proper encoding and parameters.
        Valid Time frames 
        today 3-m
        now 7-d
        today 5-y
        
        Valid Countries:
        US
        worldwide 
        
        Args:
        - terms (list of str): A list of search terms.
        - country (str): The country code (e.g., 'US') or 'worldwide' for global trends.
        - time_frame (str): The time frame for the trend data.
        
        Returns:
        - str: The constructed Google Trends URL.
        """
        df_to_write = self.load_google_trends_df_for_terms(terms = terms,
                                                country = country,time_frame = time_frame)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        df_to_write.to_sql('google_trends_repository', dbconnx, if_exists='append', index=False)
        dbconnx.dispose()
        return df_to_write

    def get_all_recent_update_dates_with_staleness(self):
        """
        Queries the database for the most recent update dates of all term combinations and calculates how many days and hours each has been stale.
    
        Returns:
        - pandas.DataFrame: A DataFrame containing each term combination ('query'), its most recent update date ('last_update_date'), 
          the number of days since the last update ('days_stale'), and the number of hours since the last update ('hours_stale'). 
          If no data is found, returns an empty DataFrame.
        """
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        query = """
        SELECT query, MAX(date_of_pull) AS last_update_date
        FROM google_trends_repository
        GROUP BY query
        """
        # Execute the query and load the results into a DataFrame
        recent_updates_df = pd.read_sql_query(query, dbconnx)
        dbconnx.dispose()
    
        # Ensure last_update_date is in datetime format
        recent_updates_df['last_update_date'] = pd.to_datetime(recent_updates_df['last_update_date'])
    
        # Calculate days_stale and hours_stale by subtracting the last_update_date from the current datetime
        current_datetime = datetime.datetime.now()
        timedelta_stale = current_datetime - recent_updates_df['last_update_date']
    
        # Calculate days stale
        recent_updates_df['days_stale'] = timedelta_stale.dt.days
    
        # Calculate hours stale (total_seconds() / 3600 to convert seconds to hours)
        recent_updates_df['hours_stale'] = timedelta_stale.dt.total_seconds() / 3600
        return recent_updates_df
