import time
from agti.data.bloomberg.standard_pulls import BloombergDailyDataTool
#import time
from agti.utilities.google_sheet_manager import GoogleSheetManager
from agti.data.tiingo.equities import TiingoDataTool
import pandas as pd
import datetime
from agti.utilities.db_manager import DBConnectionManager
from pandas.tseries.offsets import BDay
from agti.brokerage.ibrokers.market_data import AsyncStockDataManager
import asyncio
from agti.utilities.db_manager import DBConnectionManager
import datetime

import requests
class TyphusRealTimeStockFrame:
    def __init__(self,pw_map, equity_df, ib_connection_spawn):
        self.pw_map = pw_map
        self.equity_df = equity_df
        self.ib_connection_spawn=ib_connection_spawn
        self.all_equities = list(equity_df.index.get_level_values(0).unique())
        self.async_stock_data_manager =  AsyncStockDataManager(ib=self.ib_connection_spawn.ib_connection)
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.gsheet_manager = GoogleSheetManager(prod_trading=True)
        self.tiingo_data_tool = TiingoDataTool(pw_map=self.pw_map)
        self.bloomberg_daily_data_tool = BloombergDailyDataTool(pw_map=self.pw_map, bloomberg_connection=True)
    def write_full_ibkr_contract_ids(self):
        """ This writes all the IBKR contract ids to the database -- needed for fast real time price references
        needs to be run weekly""" 
        tickers= self.all_equities
        all_loaded_tickers = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            all_loaded_tickers = list(pd.read_sql('spm_typhus__us_equity_contract_ids', dbconnx)['ticker'].unique())
        except:
            pass
        tickers=[i for i in tickers if i not in all_loaded_tickers]
        
        ## Split self.all_equities into lists of 100
        def split_equities(equities, chunk_size=100):
            """
            Splits the list of equities into chunks of specified size.
            
            Parameters:
            equities (list): List of all equities.
            chunk_size (int): Size of each chunk. Default is 100.
            
            Returns:
            list: A list of lists, where each sublist contains up to chunk_size equities.
            """
            return [equities[i:i + chunk_size] for i in range(0, len(equities), chunk_size)]
        tickers = [i for i in tickers if ('-' not in i)&('.' not in i)]
        
        ## Example usage
        equity_chunks = split_equities(tickers)
        chunk_size = 100
        
        ## Loop through each chunk and write to the database
        for chunk in equity_chunks:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            contract_id_chunk = asyncio.run(self.async_stock_data_manager.get_contract_ids_for_us_equities(tickers=chunk))
            contract_id_chunk.to_sql('spm_typhus__us_equity_contract_ids', dbconnx, if_exists='append')
            dbconnx.dispose()
            time.sleep(30)
            print("DID CHUNK")

    def output_existing_open_updates(self):
        updated_tickers = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            todays_date_string = datetime.datetime.now().strftime('%Y-%m-%d')
            existing_px_open_data = pd.read_sql(f"select * from spm_typhus__bloomberg_open_price_frame where write_date = '{todays_date_string}';", dbconnx)
            updated_tickers = list(existing_px_open_data['bbgTicker'])
        except:
            pass
        return updated_tickers

    def output_existing_volume_updates(self):
        updated_tickers = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            todays_date_string = datetime.datetime.now().strftime('%Y-%m-%d')
            existing_volume_data = pd.read_sql(f"select * from spm_typhus__bloomberg_volume_frame where write_date = '{todays_date_string}';", dbconnx)
            updated_tickers = list(existing_volume_data['bbgTicker'])
        except:
            pass
        return updated_tickers
    def update_stale_bloomberg_open_updates(self):
        all_bloomberg_tickers = [i.lower()+' us equity' for i in self.all_equities]
        existing_open_updates = self.output_existing_open_updates()
        all_tickers_to_update_open_px = [i for i in all_bloomberg_tickers if i not in existing_open_updates]
        if len(all_tickers_to_update_open_px) == 0:
            print("Bloomberg Opens have been updated")
        if len(all_tickers_to_update_open_px)>0:
            open_price = self.bloomberg_daily_data_tool.BDP(all_tickers_to_update_open_px,"px_open")
            open_price['write_datetime']= datetime.datetime.now()
            open_price['write_date']= datetime.datetime.now().strftime('%Y-%m-%d')
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            open_price.to_sql('spm_typhus__bloomberg_open_price_frame', dbconnx, if_exists='append')
            print("Bloomberg Open Records written to spm_typhus__bloomberg_open_price_frame")


    def last_night_dividends(self):
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Calculate the previous business day
        next_business_date = pd.Timestamp(datetime.datetime.now().date())
        last_business_date = next_business_date #- BDay(1)
        
        # Assuming `self.pw_map['tiingo']` correctly accesses your API token
        # You might need to adjust this part if 'self' is not defined in this context
        api_token = self.pw_map['tiingo'] # Replace this with your actual API token
        
        # Making a GET request to the Tiingo API for corporate actions distributions
        request_url = f"https://api.tiingo.com/tiingo/corporate-actions/distributions?exDate={last_business_date.strftime('%Y-%m-%d')}&token={api_token}"
        request_response = requests.get(request_url, headers=headers)
        
        # Check if the request was successful
        if request_response.status_code == 200:
            # Convert the JSON response to a pandas DataFrame
            df = pd.DataFrame(request_response.json())
            df.name = "last_night_dividends"  # Rename the DataFrame for clarity
            return df
        else:
            print(f"Request failed with status code: {request_response.status_code}")
            return None

    def output_share_split_frame(self):
        headers = {
            'Content-Type': 'application/json'
        }
        business_date = pd.Timestamp(datetime.datetime.now().date())
        bdate_format = business_date.strftime('%Y-%m-%d')
        requestResponse = requests.get(f"https://api.tiingo.com/tiingo/corporate-actions/splits?exDate={business_date}&token=66b7620a9897e67af4730f8059dc3b85e43e82fc", headers=headers)
        share_split_frame = pd.DataFrame(requestResponse.json())
        share_split_frame['ticker_upper']= share_split_frame['ticker'].apply(lambda x: x.upper())
        return share_split_frame

    def augment_equity_df_with_recent_price_info(self):
        self.update_stale_bloomberg_open_updates()
        last_night_divs = self.last_night_dividends()
        last_night_divs['sharadar_ticker']=last_night_divs['ticker'].apply(lambda x: x.upper())
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        live_bloomberg_prices = pd.read_sql('spm_typhus__bloomberg_open_price_frame', dbconnx)
        live_bloomberg_prices['shar_ticker']=live_bloomberg_prices['bbgTicker'].apply(lambda x: x.split(' us equity')[0].upper())
        live_bloomberg_prices['open_price']=pd.to_numeric(live_bloomberg_prices['value'], errors='coerce')
        rt_px_frame = self.tiingo_data_tool.output_tiingo_real_time_price_frame()
        tiingo_grouped = rt_px_frame.groupby('ticker').first()
        live_bloomberg_prices['tiingo_last']=live_bloomberg_prices['shar_ticker'].map(tiingo_grouped['tngoLast'])
        live_bloomberg_prices['tiingo_high']=live_bloomberg_prices['shar_ticker'].map(tiingo_grouped['high'])
        live_bloomberg_prices['tiingo_low']=live_bloomberg_prices['shar_ticker'].map(tiingo_grouped['low'])
        price_bump= last_night_divs.groupby('sharadar_ticker').first()['distribution']
        share_split_frame = self.output_share_split_frame()
        live_bloomberg_prices['share_split_multiplier']=live_bloomberg_prices['shar_ticker'].map(share_split_frame.groupby('ticker_upper').first()['splitFrom']).fillna(1)
        live_bloomberg_prices['price_bump'] = live_bloomberg_prices['shar_ticker'].map(price_bump)
        live_bloomberg_prices['openadj']=(live_bloomberg_prices['open_price']/live_bloomberg_prices['share_split_multiplier'])+live_bloomberg_prices['price_bump'].fillna(0)
        live_bloomberg_prices['closeadj']=(live_bloomberg_prices['tiingo_last']/live_bloomberg_prices['share_split_multiplier'])+live_bloomberg_prices['price_bump'].fillna(0)
        live_bloomberg_prices['close']=live_bloomberg_prices['tiingo_last']
        live_bloomberg_prices['open']=live_bloomberg_prices['open_price']
        live_bloomberg_prices['high']=(live_bloomberg_prices['tiingo_high']/live_bloomberg_prices['share_split_multiplier'])+live_bloomberg_prices['price_bump'].fillna(0)
        live_bloomberg_prices['low']=(live_bloomberg_prices['tiingo_low']/live_bloomberg_prices['share_split_multiplier'])+live_bloomberg_prices['price_bump'].fillna(0)
        percent_of_trading_day_elapsed =(datetime.datetime.now().hour - 9)/7
        all_bloomberg_tickers = list(live_bloomberg_prices['bbgTicker'].unique())
        volume_frame = self.bloomberg_daily_data_tool.BDP(bbgTickers=all_bloomberg_tickers, field='volume', overrides={})
        live_bloomberg_prices['volume']= live_bloomberg_prices['bbgTicker'].map(volume_frame['value'])
        live_bloomberg_prices['volume_shares']=pd.to_numeric(live_bloomberg_prices['volume'],errors='coerce')
        live_bloomberg_prices['dv']=live_bloomberg_prices['volume_shares']*live_bloomberg_prices['close']
        live_bloomberg_prices['date']=pd.to_datetime(live_bloomberg_prices['write_date'])
        live_bloomberg_prices['ticker']=live_bloomberg_prices['shar_ticker']
        appender = live_bloomberg_prices[['ticker','date','closeadj', 'openadj', 'dv', 
                                          'close','open','high','low']].groupby(['ticker','date']).first()
        recent_df = pd.concat([self.equity_df,appender]).sort_index()
        return recent_df
