import requests
import pandas as pd
import sqlalchemy
import datetime
from bs4 import BeautifulSoup
from io import StringIO
from agti.ai.openai import OpenAIRequestTool
from sec_cik_mapper import StockMapper
from agti.data.sec_methods.update_cik import RunCIKUpdate
from agti.data.sec_methods.request_utility import SECRequestUtility
from agti.utilities.db_manager import DBConnectionManager
import time
class SECFilingUpdateManager:
    def __init__(self,pw_map, user_name):
        self.pw_map = pw_map
        self.sec_request_utility = SECRequestUtility(pw_map=self.pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.user_name = user_name
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
    def extract_99_urls_from_index_page_html(self, html_content):
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
        url_output = '|'.join(results_df[results_df['Document Type'].apply(lambda x: '99' in str(x))]['URL'])
        return url_output

    def get_updated_index_urls(self):
        """
        Reads the 'sec__index_page_html' table and outputs a list of updated index_urls.
        If the database or table doesn't exist, outputs an empty list.
        
        Returns:
        list: A list of updated index_urls or an empty list if the table doesn't exist.
        """
        # Initialize database connection
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        
        # Check if the table exists
        inspector = sqlalchemy.inspect(dbconnx)
        if not inspector.has_table('sec__index_page_html'):
            return []
        
        # Read the table
        index_html_table = pd.read_sql('sec__index_page_html', dbconnx)
        
        # Extract and return the list of updated index_urls
        updated_index_urls = index_html_table['index_url'].tolist()
        return updated_index_urls

    def write_incremental_index_page_html(self):
        """ This loads the existing filings generated in real time 
        and checks if they're earnings. then for the earnings that have not
        been written it writes the index pages for them to the sec__index_page_html database
        """ 
        existing_filing_df = self.load_existing_filing_df()
        existing_eps = existing_filing_df[existing_filing_df['is_eps']==True].copy()
        existing_eps.set_index('html_url', inplace=True)
        
        existing_filings = []
        ## Add function to get existing eps filings
        existing_eps['index_url']=existing_eps.index
        
        update_urls = self.get_updated_index_urls()
        non_updated_urls = [i for i in existing_eps.index if i not in update_urls]
        existing_eps = existing_eps.loc[non_updated_urls]
        if len(existing_eps)>0:
            ## get all the raw html of the index pages which contain multiple filing urls
            existing_eps['raw_index_page_html'] = existing_eps['index_url'].apply(lambda x: self.sec_request_utility.compliant_request(x).text)
            existing_eps['99_page_urls'] = existing_eps['raw_index_page_html'].apply( lambda x: self.extract_99_urls_from_index_page_html(x))
            raw_index_page_url_content = existing_eps[['raw_index_page_html', 'index_url','99_page_urls']].copy()
            raw_index_page_url_content['date_of_update'] = datetime.datetime.now()
            raw_index_page_url_content.set_index('index_url', inplace=True)
            raw_index_page_url_content.reset_index()
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)    
            raw_index_page_url_content.reset_index().to_sql('sec__index_page_html', dbconnx, if_exists='append', index=False)


    def get_ticker_for_sec_cik_double_try(self,cik = '0000004457'):
        ticker = ''
        try:
            ticker = self.cik_to_ticker_map[cik]
        except:
            try:
                ticker = list(self.cik_to_ticker_map__stockmapper[cik])[0]
            except:
                pass
            pass
        return ticker
    def create_final_constructor_pre_html_load(self):
        # First load in the index pages that are already written
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)    
        sec_index_pages = pd.read_sql('sec__index_page_html', dbconnx)
        valid_earnings_df = sec_index_pages[sec_index_pages['99_page_urls'] != ''].copy()
        valid_earnings_df['99_page_urls__split'] = valid_earnings_df['99_page_urls'].str.split('|')
        exploded_df = valid_earnings_df.explode('99_page_urls__split')
        exploded_df['filing_html']=exploded_df['99_page_urls__split']
        ## the final constructor is an exploded version of the index_html with all the filing htmls on different rows
        final_constructor = exploded_df[['index_url','raw_index_page_html','filing_html']].copy()
        existing_filing_df = self.load_existing_filing_df()
        index_url_mapping = existing_filing_df.groupby('html_url').first()
        final_constructor['cik']= final_constructor['index_url'].map(index_url_mapping['CIK'])
        final_constructor['upload_date']= final_constructor['index_url'].map(index_url_mapping['full_datetime'])
        final_constructor['sec_name']= final_constructor['index_url'].map(index_url_mapping['simple_name'])
        final_constructor['sec_item_string']= final_constructor['index_url'].map(index_url_mapping['items_string'])
        final_constructor['ticker']=final_constructor['cik'].apply(lambda x: self.get_ticker_for_sec_cik_double_try(x))
        return final_constructor
    def output_updated_filing_urls(self):
        updated_filing_urls = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
            updated_filing_urls = pd.read_sql('sec__full_filing_details', dbconnx)['filing_html'].unique()
        except:
            pass
        return updated_filing_urls

    def update_full_filing_details_with_incremental_html_reads(self):
        updated_filing_urls = self.output_updated_filing_urls()
        final_constructor_pre_html_load = self.create_final_constructor_pre_html_load()
        final_constructor_pre_html_load.set_index('filing_html',inplace=True)
        final_constructor_pre_html_load= final_constructor_pre_html_load[~final_constructor_pre_html_load.index.get_level_values(0).isin(updated_filing_urls)].copy().reset_index()
        final_constructor_pre_html_load['filing_full_text']=final_constructor_pre_html_load['filing_html'].apply(lambda x: self.sec_request_utility.compliant_request(x).text)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        final_constructor_pre_html_load.to_sql('sec__full_filing_details', dbconnx, if_exists='append')

    def run_full_filing_update(self):
        """
        This writes the incremental index page htmls for all the existing earnings loaded from
        sec__update_recent_filings. Then once they are written (including the extracted filings - the 99s)
        with the function write_incremental_index_page_html - the output of this is passed in to
        create_final_constructor_pre_html_load which is an exploded dataframe consisting of a unique row
        for every 99 filing. this exploded df does not yet have the html loaded in. once it does, it is
        written to sec__full_filing_details
        """ 
        self.write_incremental_index_page_html()
        print("wrote incremental SEC index page html to sec__index_page_html")
        self.update_full_filing_details_with_incremental_html_reads()
        print('updated sec__full_filing_details to sec__full_filing_details')
