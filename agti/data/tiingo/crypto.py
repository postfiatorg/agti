import requests 
from io import TextIOWrapper, BytesIO
from io import BytesIO as Buffer
from zipfile import ZipFile
import csv
import json
import sqlalchemy
import pandas as pd
import datetime 
from agti.utilities.db_manager import DBConnectionManager

class TiingoCryptoTool:
    def __init__(self, pw_map):
        self.pw_map= pw_map
    ### There is a lot more to do with this but for now using it for live pricing 
    def output_all_tiingo_meta_data(self):
        headers = {
        'Content-Type': 'application/json'
        }
        requestResponse = requests.get(f"https://api.tiingo.com/tiingo/crypto?token={self.pw_map['tiingo']}", headers=headers)
        tiingo_meta_data = pd.DataFrame((requestResponse.json()))
        return tiingo_meta_data

    def output_crypto_usd_price_jsons(self, start_date='2019-01-02', end_date='2024-03-05',tickers_to_work = ['btc','eth'], resample_window='4hour'):
        """ EXAMPLE
        start_date='2019-01-02', tickers_to_work = ['btcusd','ethusd'], resample_window='4hour'
        """
        small_cap_ticker =[i.lower()+'usd' for i in tickers_to_work]
        ticker_str= ','.join(small_cap_ticker)
        
        headers = {
        'Content-Type': 'application/json'
        }
        requestResponse = requests.get(f"""https://api.tiingo.com/tiingo/crypto/prices?
        tickers=btcusd&resampleFreq={resample_window}&tickers={ticker_str}&startDate={start_date}
        &endDate={end_date}&token={self.pw_map['tiingo']}""", headers=headers)
        #tiingo_meta_data = pd.DataFrame((requestResponse.json()))
        return requestResponse

    def output_crypto_usd_price_dfs(self,start_date='2019-01-02', end_date='2024-03-05',
                                    tickers_to_work = ['btc','eth'], resample_window='4hour'):
        xjson = self.output_crypto_usd_price_jsons(start_date=start_date, 
                                                               end_date=end_date,
                                                               tickers_to_work = tickers_to_work, 
                                                               resample_window=resample_window).json()
        crypto_px_data = pd.DataFrame(xjson)
        df_arr=[]
        for ind_to_work in crypto_px_data.index:
            try:
                df_to_write = pd.DataFrame(crypto_px_data.loc[ind_to_work]['priceData'])
                df_to_write['ticker']=crypto_px_data.loc[ind_to_work]['ticker']
                df_to_write['base_currency']=crypto_px_data.loc[ind_to_work]['baseCurrency']
                df_to_write['quote_currency']=crypto_px_data.loc[ind_to_work]['quoteCurrency']
                df_arr.append(df_to_write)
            except:
                print(ind_to_work)
                pass
        xdf = pd.concat(df_arr)
        return xdf

    def output_recent_usd_crypto_price_df(self, tickers_to_work = ['btc','eth']):
        end_date = (datetime.datetime.now()+datetime.timedelta(1)).strftime('%Y-%m-%d')
        start_date = (datetime.datetime.now()-datetime.timedelta(1)).strftime('%Y-%m-%d')
        resample_window = '4hour'
        full_px_df = self.output_crypto_usd_price_dfs(tickers_to_work=tickers_to_work, start_date=start_date,
                                           end_date=end_date, resample_window=resample_window)
        return full_px_df
        
        
    def output_live_usd_price_snapshot(self,tickers_to_work=['btc','eth','xrp','bch','xlm']):
        px_df = self.output_recent_usd_crypto_price_df(tickers_to_work=tickers_to_work)
        recent_price_df = px_df.groupby('base_currency').last()[['close','volumeNotional']].copy()
        recent_price_df['max_size']=recent_price_df['volumeNotional']/100
        return recent_price_df
