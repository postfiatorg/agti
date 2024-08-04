from agti.utilities.scraping import ScrapingFileManager
from agti.utilities.db_manager import DBConnectionManager
import datetime
from selenium import webdriver
import datetime
import requests
import pandas as pd
import numpy as np
import itertools
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import re
import os
class CoinMarketCapDataTool:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.scraping_file_manager = ScrapingFileManager()
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        
        driver_type='edge'
        if driver_type=='edge':
            edge_options = Options()
            edge_options.add_argument("-inprivate")  # For InPrivate mode

            # Initialize Edge service
            #service = Service(executable_path=driver_path)

            # Initialize Edge WebDriver with options and service
            driver = webdriver.Edge(options=edge_options)#, service=service)
        self.scraper = driver
        self.data_dump_directory_path  =self.pw_map['local_data_dump']
        self.write_all_cmc_currency_pages(days_stale_max=15)
        self.cmc_details_df = self.parse_coinmarketcap_details_df()
        self.cmc_details_df['fdv']=self.cmc_details_df['fdv'].astype(float)
        self.cmc_details_df= self.cmc_details_df.sort_values('fdv',ascending=False)

    def get_coinmarket_cap_page(self, page_num=1):
        self.scraper.get(f'https://coinmarketcap.com/?page={page_num}')
        psource=self.scraper.page_source
        return psource
    def get_all_unique_currencies_from_cmc_page(self, page_html):
        all_currency_refs = [i.split('/')[0] for i in page_html.split('href="/currencies/')]
        all_currency_list = [i for i in all_currency_refs if '<' not in i]
        unique_currencies = list(set(all_currency_list))
        return unique_currencies

        
    def get_recent_coin_page(self,coin_to_work='tron'):
        ftr=self.scraping_file_manager.get_most_recent_file_for_item_in_dir(item_str=coin_to_work, 
                                                storage_dir='coinmarketcap',
                                            file_dir='coinpages')
        with open(ftr, "r", encoding="utf-8") as f:
            psource=f.read()
            f.close()
        return psource


    def write_cmc_top_500(self):
        yarr=[]
        for pagex in [1,2,3,4,5]:
            p1=self.get_coinmarket_cap_page(page_num=pagex)
            all_currencies = self.get_all_unique_currencies_from_cmc_page(page_html=p1)
            yarr=yarr+all_currencies
        top_500_currencies = pd.DataFrame(yarr)
        top_500_currencies.columns=['currency_code']
        top_500_currencies['write_date']=datetime.datetime.now()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        top_500_currencies.to_sql('coinmarketcap__top500',dbconnx, if_exists='append')

    def write_coin_page_if_stale(self,coin_to_work='bitcoin',days_stale_max=5):
        days_stale=self.scraping_file_manager.determine_how_out_of_date_item_file_is(item_str=coin_to_work, 
                                            storage_dir='coinmarketcap',
                                    file_dir='coinpages')
        if days_stale >= days_stale_max:
            self.scraper.get(f'https://coinmarketcap.com/currencies/{coin_to_work}/')
            ftw = self.scraping_file_manager.format_item_file_to_write(item_str=coin_to_work, 
                                                storage_dir='coinmarketcap',
                                                file_dir='coinpages', 
                                                file_extension='html')
            dq_text = '<h1>403 ERROR</h1>\n<h2>The request could not be satisfied.</h2>'
            full_page_text = self.scraper.page_source
            if dq_text not in full_page_text:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(ftw), exist_ok=True)
                
                with open(ftw, "w", encoding="utf-8") as f:
                    f.write(self.scraper.page_source)
            
            if dq_text in full_page_text:
                print('COINMARKETCAP IS GENERATING A 403 - temporarily sleeping')
                time.sleep(60)

    def output_coin_id_map_from_coin_page(self,coin_to_work='bittensor'): 
        
        coin_page = self.get_recent_coin_page(
        coin_to_work=coin_to_work)
        coin_symbol=''
    
        try:
            coin_symbol= str(coin_page.split('nameSymbol">')[1].split('<')[0]).lower()
        except:
            pass
        coin_id = coin_page.split('<meta property="og:image" ')[1].split('.png')[0].split('/')[-1:][0]
        website_split = coin_page.split('"urls":{"website":')[1].split(']')[0]
        # Your input string
        #input_string = '["https://www.ethereum.org/","https://en.wikipedia.org/wiki/Ethereum"]'
        
        # Regular expression to match URLs within quotes
        urls = re.findall(r'"(https?://[^"]+)"', website_split)
        urls = [i for i in urls if 'wikipedia' not in i]
        url_list = '|'.join(urls)

        twitter_url_pattern = r'https://twitter\.com/[A-Za-z0-9_]+'

        twitter_urls = re.findall(twitter_url_pattern, coin_page)
        twitter_url_list = '|'.join(list(set([i for i in twitter_urls if 'CoinMarketCap' not in i])))
        
        coin_ticker = coin_page.split('to USD live price')[0].split('<title>')[-1:][0].split(',')[-1:][0].strip()
        source_code_page = website_split
        source_code_urls = coin_page.split('"source_code":')[1].split(']')[0]
        source_code_list = '|'.join(re.findall(r'"(https?://[^"]+)"', source_code_urls))
        all_tags = [i.split("name")[0] for i in coin_page.split('"tags":')[1].split('urls"')[0].split('slug":')]
        all_non_port_tags = [i for i in all_tags if ('portfolio' not in i) &('estate' not in i) &('sec-' not in i)]
        processed_list = [item.strip('",[]{}') for item in all_non_port_tags]
        processed_list = [i for i in processed_list if i!='']
        tag_list = '|'.join(processed_list)
        fdv=coin_page.split('"fullyDilutedMarketCap":')[1].split('")')[0].split(',')[0]#.replace('
        marketCap=coin_page.split('"marketCap":')[1].split('")')[0].split(',')[0]#.replace('
        faq_text = coin_page.split('faqDescription')[1].split('}]')[0]
        #"volume":10988207
        coin_volume = coin_page.split('"volume":')[1].split(',"')[0]
        
        key_coin_detail_map ={'coin_id':coin_id,'coin_symbol':coin_symbol,'coin_ticker':coin_ticker, 
                              'twitter_list':twitter_url_list, 
                              'urls': url_list,'source_code_list': source_code_list, 
                              'tag_list': tag_list,'fdv':fdv,'marketCap':marketCap,
                              'faq_text':faq_text,'coin_volume':coin_volume}
        output_df = pd.DataFrame(key_coin_detail_map, index=[coin_to_work])
        output_df.index.name = 'currency_name'
        return output_df

    def write_all_cmc_currency_pages(self, days_stale_max=15):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        top_500 = pd.read_sql('coinmarketcap__top500',dbconnx)
        all_currency_codes = list(top_500['currency_code'].unique())
        for xcode in all_currency_codes:
            self.write_coin_page_if_stale(coin_to_work=xcode, days_stale_max=days_stale_max)

    def parse_coinmarketcap_details_df(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        top_500 = pd.read_sql('coinmarketcap__top500',dbconnx)
        all_cmc_top500 = list(top_500['currency_code'].unique())
        varr=[]
        for xcurr in all_cmc_top500:
            try:
                varr.append(self.output_coin_id_map_from_coin_page(coin_to_work=xcurr))
            except:
                print(xcurr)
                pass
        #return pd.concat(varr)
        full_output = pd.concat(varr)
        cmc_details_df = full_output
        return cmc_details_df



    def output_cmc_history_for_currency(self, currency_to_get='bitcoin'):
        
        details_df= self.cmc_details_df
        id_specific = details_df.loc[currency_to_get]['coin_id']
        ticker_specific = details_df.loc[currency_to_get]['coin_ticker']
        #req_url=f'https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical?id={id_specific}&convert=USD&time_start=1401744000&time_end=1811014400'
        #f=requests.get(req_url)
        req_url = f'https://api.coinmarketcap.com/data-api/v3.1/cryptocurrency/historical?id={id_specific}&timeStart=1356998400&interval=1d&convertId=2781'
        #xo = requests.get('https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart?id=1&range=1D')
        fjson_hist  = requests.get(req_url)#.json()
        px_frame = pd.DataFrame(fjson_hist.json()['data']['quotes'])
        px_frame['currency_name']= currency_to_get
        px_frame['ticker']= ticker_specific
        
        px_frame['close']=px_frame['quote'].apply(lambda x: x['close'])
        px_frame['open']=px_frame['quote'].apply(lambda x: x['open'])
        px_frame['high']=px_frame['quote'].apply(lambda x: x['high'])
        px_frame['low']=px_frame['quote'].apply(lambda x: x['low'])
        px_frame['volume']=px_frame['quote'].apply(lambda x: x['volume'])
        px_frame['market_cap']=px_frame['quote'].apply(lambda x: x['marketCap'])
        px_frame['timestamp']=px_frame['quote'].apply(lambda x: x['timestamp'])
        #px_frame['simple_date']=pd.to_datetime(px_frame['time_close'].apply(lambda x: str(x)[0:10]))
        full_px_frame = px_frame[[
               'currency_name', 'ticker', 'close', 'open', 'high', 'low', 'volume',
               'market_cap', 'timestamp']].copy()
        full_px_frame['date']= pd.to_datetime(full_px_frame['timestamp'].apply(lambda x: str(x)[0:10]))
        return full_px_frame
        
    def write_full_coinmarketcap_price_history(self):
        xlist = list(self.cmc_details_df.index)
        full_hist=[]
        for xcurr in xlist:
            full_hist.append(self.output_cmc_history_for_currency(currency_to_get=xcurr))
            print(xcurr)
        full_df = pd.concat(full_hist)
        full_price_history = full_df
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        full_price_history.to_sql('coinmarketcap__price_history', dbconnx, if_exists='replace')