from io import StringIO
from agti.data.sec_methods.request_utility import SECRequestUtility
from sec_cik_mapper import StockMapper
from agti.data.sec_methods.update_cik import RunCIKUpdate
from agti.data.sec_methods.sec_filing_update import SECFilingUpdateManager
from agti.ai.openai import OpenAIRequestTool
import re
from bs4 import BeautifulSoup
import pandas as pd
import json
import numpy as np
from agti.utilities.db_manager import DBConnectionManager
import io 
class GetTickerMostRecentSECFiling:
    def __init__(self,pw_map):
        self.pw_map=pw_map 
        self.sec_request_utility = SECRequestUtility(pw_map=self.pw_map)
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=self.pw_map)
        self.user_name ='spm_typhus'
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.sharadar_data = self.output_sharadar_data_slice()
        self.cik_update_tool = RunCIKUpdate(pw_map=self.pw_map, user_name=self.user_name)
        self.ticker_to_primary_cik= self.generate_ticker_to_primary_cik_map()
        self.sec_filing_update_manager = SECFilingUpdateManager(pw_map=self.pw_map, user_name=self.user_name)
    def generate_cik_to_ticker_map(self):
        stockmapper = StockMapper()
        example_map  = stockmapper.cik_to_tickers
        ## Convert the example_map into a list of tuples
        data = [(key, value) for key, values in example_map.items() for value in values]
        shar_netinc= self.sharadar_data.groupby('ticker').last()['netinc']
        ## Create a DataFrame from the list of tuples
        stock_mapper_ticker_cik_grouping = pd.DataFrame(data, columns=['cik', 'ticker'])
        stock_mapper_ticker_cik_grouping['netinc']=stock_mapper_ticker_cik_grouping['ticker'].map(
            shar_netinc)
        live_stock_mapper_cik_to_ticker_map = stock_mapper_ticker_cik_grouping.dropna()[['cik','ticker']]
        live_stock_mapper_cik_to_ticker_map['source']='livemapper'
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        sql_query = """
        SELECT *
        FROM sharadar__tickers;
        """
        sharadar_ticker_details = pd.read_sql_query(sql_query, dbconnx)
        sharadar_ticker_details['cik']=sharadar_ticker_details['secfilings'].apply(lambda x: str(x).split('&CIK=')[-1:][0])
        sharadar_ticker_details['is_stock']=np.where(sharadar_ticker_details['category'].apply(lambda x: 
                                                                                               'stock' in x.lower()),'stock','notstock')
        
        sharadar_ticker_cik_grouping = sharadar_ticker_details[(sharadar_ticker_details['ticker']!='N/A') 
        & (sharadar_ticker_details['isdelisted']=='N')& (sharadar_ticker_details['is_stock']=='stock')][['ticker','cik']].copy()
        sharadar_ticker_cik_grouping['source']='sharadar'
        #secik_update_tool = RunCIKUpdate(pw_map=self.pw_map, user_name=self.user_name)
        sec_cik_to_ticker_map = self.cik_update_tool.output_cached_cik_df().groupby('cik').first()[['ticker']].reset_index()
        sec_cik_to_ticker_map['source']='sec'
        full_cik_mapping = pd.concat([sec_cik_to_ticker_map,sharadar_ticker_cik_grouping 
                   ,live_stock_mapper_cik_to_ticker_map])
        #cik_to_ticker_map__default = full_cik_mapping.groupby('cik').first()['ticker']
        full_cik_mapping['netinc']=full_cik_mapping['ticker'].map(shar_netinc)
        full_cik_mapping['netinc_abs']=full_cik_mapping['netinc'].abs()
        full_cik_mapping__final = full_cik_mapping.sort_values('netinc_abs',ascending=False).groupby('cik').first()['ticker']
        return full_cik_mapping__final
    def output_sharadar_data_slice(self):
        ## Fixing the SQL query to select the required fields with the correct type casting for datekey
        sql_query = """
        SELECT calendardate::timestamp, datekey::timestamp, fcf, netinc, fxusd, ticker
        FROM sharadar__sf1
        WHERE datekey::date >= (CURRENT_DATE - INTERVAL '5 years') 
        AND dimension = 'ARQ';
        """
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        sharadar_data = pd.read_sql_query(sql_query, dbconnx)
        sharadar_data['calendardate'] = pd.to_datetime(sharadar_data['calendardate'])
        sharadar_data['datekey'] = pd.to_datetime(sharadar_data['datekey'])
        sharadar_data = sharadar_data.sort_values(['ticker', 'calendardate']).groupby(['ticker', 'calendardate']).last().reset_index()
        dexed = sharadar_data.groupby(['ticker','datekey']).last()
        dexed['fcf__usd']=dexed['fcf']/dexed['fxusd']
        dexed['netinc__usd']=dexed['netinc']/dexed['fxusd']
        return dexed
    def generate_ticker_to_primary_cik_map(self):
        cik_to_ticker_map = self.generate_cik_to_ticker_map()
        ticker_to_cik_map = {ticker: cik for cik, ticker in cik_to_ticker_map.items()}
        xdf = pd.DataFrame(ticker_to_cik_map, index=[0]).transpose()
        xdf.index.name='ticker'
        xdf.columns =['cik']
        ticker_to_primary_cik = xdf.reset_index().groupby('ticker').first()['cik']
        return ticker_to_primary_cik
    def get_all_recent_earnings_filings_for_ticker(self,ticker_to_work='AMZN'):
        if ticker_to_work not in self.ticker_to_primary_cik.index:
            print(f'{ticker_to_work} not in the ticker_to_primary_cik index')
            return None
        if ticker_to_work in self.ticker_to_primary_cik.index:
            cik_to_get = self.ticker_to_primary_cik[ticker_to_work]
            
            cik_formatter = str(int(cik_to_get))
            ## Construct the URL for the SEC EDGAR search page
            all_filings_page = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_formatter}&type=8-K&dateb=&owner=exclude&count=100'
            print(f'working {all_filings_page}')
            ## Make a request to the SEC EDGAR page
            response = self.sec_request_utility.compliant_request(all_filings_page)
            
            stringio_obj = io.StringIO(response.text)
            ## Read the HTML content using `pd.read_html` with `io.StringIO`
            recent_100_8ks = pd.read_html(stringio_obj)
            default_table = recent_100_8ks[2]
            all_2_filings = default_table[default_table['Description'].apply(lambda x: '2.02' in x)].copy()
            all_2_filings['stripped_filing_number']=all_2_filings['Description'].apply(lambda x: x.split('Acc-no: ')[-1:][0].split('\xa0')[0])
            all_2_filings['cik__full_length']= cik_to_get
            all_2_filings['cik__smol_format']= cik_formatter

            all_2_filings['full_filing_num']=all_2_filings['stripped_filing_number'].apply(lambda x: x.replace('-',''))
            ## Apply function to generate URLs for recent_filings dataframe
            def generate_sec_url(row):
                smol_cik = row['cik__smol_format']
                no_dash_filing = row['full_filing_num']
                stripped_filing_number = row['stripped_filing_number']
                return f'https://www.sec.gov/Archives/edgar/data/{smol_cik}/{no_dash_filing}/{stripped_filing_number}-index.htm'
            
            all_2_filings['index_page_url'] = all_2_filings.apply(generate_sec_url, axis=1)

            return all_2_filings

    def get_most_recent_filing_html_files_for_ticker(self,ticker_to_work='AMZN'):
        recent_filings = self.get_all_recent_earnings_filings_for_ticker(ticker_to_work=ticker_to_work)
        most_recent_filing = recent_filings.head(1)
        xhtml=self.sec_request_utility.compliant_request(list(most_recent_filing['index_page_url'])[0]).text
        op= self.sec_filing_update_manager.extract_99_urls_from_index_page_html(xhtml).split('|')
        return op