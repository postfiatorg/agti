import requests
import pandas as pd
import sqlalchemy
import datetime
from bs4 import BeautifulSoup
from io import StringIO
from ai.openai import OpenAIRequestTool
from sec_cik_mapper import StockMapper
from sec_methods.update_cik import RunCIKUpdate
from sec_methods.request_utility import SECRequestUtility
from utilities.db_manager import DBConnectionManager
import time

class SECFilingUpdateManager:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        self.sec_request_utility = SECRequestUtility(pw_map=self.pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.user_name = 'spm_typhus'
        self.cik_update_tool = RunCIKUpdate(pw_map=self.pw_map, user_name=self.user_name)
        self.cik_to_ticker_map = self.cik_update_tool.output_cached_cik_df().groupby('cik').first()['ticker']
        self.openai_request_tool = OpenAIRequestTool(pw_map=pw_map)
        self.cik_to_ticker_map__stockmapper = StockMapper().cik_to_tickers

    def load_existing_filing_df(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        try:
            existing_updates = pd.read_sql('SELECT * FROM sec__update_recent_filings', dbconnx)
        except Exception as e:
            print(f'No existing updates table: {e}')
            existing_updates = pd.DataFrame()
        return existing_updates

    @staticmethod
    def extract_99_urls_from_index_page_html(html_content):
        base_url = "https://www.sec.gov"
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table', class_='tableFile')
        data = []

        for table in tables:
            df = pd.read_html(StringIO(str(table)))[0]
            df['URL'] = [base_url + link['href'] if link else '' for link in table.find_all('a', href=True)]
            for _, row in df.iterrows():
                data.append({
                    'Seq': row['Seq'],
                    'Description': row['Description'],
                    'Document Type': row['Type'],
                    'URL': row['URL']
                })

        results_df = pd.DataFrame(data)
        url_output = '|'.join(results_df[results_df['Document Type'].apply(lambda x: '99.' in str(x))]['URL'])
        return url_output

    def output_recent_sec_index_page_html_table(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        inspector = sqlalchemy.inspect(dbconnx)
        if inspector.has_table('sec__index_page_html'):
            return pd.read_sql('sec__index_page_html', dbconnx)
        else:
            return pd.DataFrame()

    def write_full_sec_extractable_data(self, force=False):
        index_html_table = self.output_recent_sec_index_page_html_table()
        all_updated_urls = list(index_html_table['html_url'])
        existing_filing_df = self.load_existing_filing_df()
        existing_filing_df = existing_filing_df[existing_filing_df['is_eps']].copy()
        existing_filing_df.set_index('html_url', inplace=True)
        
        if not force:
            existing_filing_df = existing_filing_df[~existing_filing_df.index.isin(all_updated_urls)]
        
        existing_filing_df.reset_index(inplace=True)
        if not existing_filing_df.empty:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
            existing_filing_df['updated'] = False
            existing_filing_df['raw_index_page_html'] = existing_filing_df['html_url'].apply(lambda x: self.sec_request_utility.compliant_request(x).text)
            existing_filing_df['99_page_urls'] = existing_filing_df['raw_index_page_html'].apply(self.extract_99_urls_from_index_page_html)

            raw_index_page_url_content = existing_filing_df[['raw_index_page_html', 'html_url']].copy()
            raw_index_page_url_content['date_of_update'] = datetime.datetime.now()
            raw_index_page_url_content.set_index('html_url', inplace=True)
            raw_index_page_url_content_to_write = raw_index_page_url_content[~raw_index_page_url_content.index.isin(all_updated_urls)]
            
            raw_index_page_url_content_to_write.to_sql('sec__index_page_html', dbconnx, if_exists='append', index=False)
            print('wrote sec__index_page_html')

            valid_earnings_df = existing_filing_df[existing_filing_df['99_page_urls'] != ''].copy()
            valid_earnings_df['99_page_urls__split'] = valid_earnings_df['99_page_urls'].str.split('|')

            exploded_df = valid_earnings_df.explode('99_page_urls__split')
            final_html_extraction_df = exploded_df[['99_page_urls__split', 'html_url', 'Document', 'simple_name',
                                                    'cik', 'items_string', 'is_eps', 'full_datetime']].copy()
            final_html_extraction_df['full_html_of_99_page'] = final_html_extraction_df['99_page_urls__split'].apply(lambda x: self.sec_request_utility.compliant_request(x).text)
            final_html_extraction_df.rename(columns={
                '99_page_urls__split': 'filing_url',
                'html_url': 'index_page_url',
                'Document': 'document'
            }, inplace=True)

            final_html_extraction_df.to_sql('sec_update__extractable_filing_information', dbconnx)
            return final_html_extraction_df

    def output_recent_sec_index_page_html_table(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        inspector = sqlalchemy.inspect(dbconnx)
        if inspector.has_table('sec_update__extractable_filing_information'):
            return pd.read_sql('sec_update__extractable_filing_information', dbconnx)
        else:
            return pd.DataFrame()
        
    def run_write_full_sec_extractable_data_for_3_hours(self):
        """Runs the SEC data batch load every 2 minutes for 3 hours."""
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(hours=3)

        while datetime.datetime.now() < end_time:
            print(f"Running update at {datetime.datetime.now()}")
            self.write_full_sec_extractable_data()
            print("Sleeping for 2 minutes...")
            time.sleep(120)
