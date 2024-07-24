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
class TiingoDataTool:
    def __init__(self,pw_map):
        self.pw_map = pw_map 
        self.all_supported_tickers = self.get_supported_tickers_csv()
        self.tiingo_key = self.pw_map['tiingo']
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.user_name ='spm_typhus'
        self.populate_tiingo_table_if_not_exists()
    def get_zipfile_from_response(self, response):
        buffered = Buffer(response.content)
        return ZipFile(buffered)
    
    
    def get_buffer_from_zipfile(self, zipfile, filename):
        op = TextIOWrapper(BytesIO(zipfile.read(filename)))
        return op
    
    def dict_to_object(self, item, object_name):
        """Converts a python dict to a namedtuple, saving memory."""
        fields = item.keys()
        values = item.values()
        return json.loads(
            json.dumps(item), object_hook=lambda d: namedtuple(object_name, fields)(*values)
        )


    def get_supported_tickers_csv(self):
        listing_file_url = (
            "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
        )
        response = requests.get(listing_file_url)
        zipdata = self.get_zipfile_from_response(response)
        raw_csv = self.get_buffer_from_zipfile(zipdata, "supported_tickers.csv")
        #reader = csv.DictReader(raw_csv)
        return pd.read_csv(raw_csv)

    def generate_all_live_tickers(self):
        existing_tickers = self.get_supported_tickers_csv()#['exchange'].unique()
        existing_tickers#[existing_tickers['exchange']=='CSE'].head(50)
        valid_exchanges= ['NYSE NAT','NYSE MKT','NYSE ARCA',"NYSE",'NASDAQ','BATS','AMEX']
        live_universe = existing_tickers[existing_tickers['exchange'].apply(lambda x: x in valid_exchanges)].dropna()
        live_universe['endDate']=pd.to_datetime(live_universe['endDate'])
        all_live_tickers = live_universe[live_universe['endDate']>= datetime.datetime.now()-datetime.timedelta(7)].copy()
        return all_live_tickers

    def raw_load_tiingo_data(self,ticker='SPY',start_date='2010-01-01', end_date='2023-01-21'):
        format_map = {'start_date':start_date,
                    'end_date': end_date,
                    'token': self.tiingo_key,
                    'ticker': ticker}
        temp_df=pd.read_json("https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start_date}&endDate={end_date}&token={token}".format(**format_map))
        temp_df['ticker']= format_map['ticker']  
        temp_df['simple_date']=temp_df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        temp_df['simple_date']=pd.to_datetime(temp_df['simple_date'])
        return temp_df

    def get_all_sharadar_tickers(self):
        """ Sharadar is a bulk data query so it's less intensive than Tiingo so reduce tiingo universe down to 
        stuff that is not in sharadar""" 
        all_sharadar_tickers= []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
            ## Query to get the max date for sharadar__sep
            sql_query = 'SELECT MAX(date) as max_date FROM sharadar__sep'
            max_date_df = pd.read_sql(sql_query, dbconnx)
            max_date_of_sharadar = list(max_date_df['max_date'])[0]
            
            ## Query to pull all of sharadar_sep as of the max_date_of_sharadar
            sql_query = f"SELECT * FROM sharadar__sep WHERE date = '{max_date_of_sharadar}'"
            sharadar_sep_max_date_df = pd.read_sql(sql_query, dbconnx)
            all_sharadar_tickers = list(sharadar_sep_max_date_df['ticker'].unique())
        except:
            pass
        return all_sharadar_tickers

    def output_max_date_of_equity_update(self):
        ## SQL query to select the max date for each ticker in tiingo__equities and load it as a DataFrame
       
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        
        sql_query = """
        SELECT ticker, MAX(simple_date) as max_date
        FROM tiingo__equities
        GROUP BY ticker
        """
        
        ## Load the result into a DataFrame
        df = pd.read_sql(sql_query, dbconnx)
        return df

    def populate_tiingo_table_if_not_exists(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        if 'tiingo__equities' not in sqlalchemy.inspect(dbconnx).get_table_names():
            print("POPULATING INITIAL TIINGO UPDATE TABLE")
            temp_df=self.raw_load_tiingo_data(ticker='SPY',start_date='1993-01-29', end_date='2023-04-01')
            temp_df.to_sql('tiingo__equities', dbconnx, if_exists='replace')
    def generate_tiingo_data_loading_cue(self):
        all_shar = self.get_all_sharadar_tickers()
        live_tickers = self.generate_all_live_tickers().set_index('ticker')
        all_tiingo_to_load = live_tickers[~live_tickers.index.get_level_values(0).isin(all_shar)]
        non_warrant = [i for i in list(all_tiingo_to_load.index) if '-' not in i]
        tiingo_loading_frame = all_tiingo_to_load.loc[non_warrant]
        tiingo_loading_frame['startDate']=pd.to_datetime(tiingo_loading_frame['startDate'])
        tiingo_loading_frame['endDate']=pd.to_datetime(tiingo_loading_frame['endDate'])
        
        tiingo_loading_frame['updated_as_of__no_update']= tiingo_loading_frame['startDate']
        tiingo_loading_frame[tiingo_loading_frame['assetType']!='Mutual Fund'].copy()
        
        updated_as_of_map = self.output_max_date_of_equity_update().groupby('ticker').first()['max_date']
        tiingo_loading_frame['updated_as_of__db']=updated_as_of_map
        tiingo_loading_frame['start_date_data_pull']=tiingo_loading_frame[['updated_as_of__no_update','updated_as_of__db']].max(1)
        tiingo_loading_frame['days_out_of_date']=(tiingo_loading_frame[['start_date_data_pull',
                                                                        'endDate']].max(1)-tiingo_loading_frame['updated_as_of__db']).apply(lambda x: x.days)
        tiingo_loading_frame['days_out_of_date']=tiingo_loading_frame['days_out_of_date'].fillna(100)
        return tiingo_loading_frame

    def update_all_stale_tiingo_data(self):
        # updated_as_of_map = self.output_max_date_of_equity_update().groupby('ticker').first()['max_date']
        tiingo_loading_frame = self.generate_tiingo_data_loading_cue()
        tickers_out_of_date = list(tiingo_loading_frame[tiingo_loading_frame['days_out_of_date']>0].index)
        for ticker_to_work in tickers_out_of_date:
            try:
                start_date = tiingo_loading_frame.loc[ticker_to_work]['start_date_data_pull'].strftime('%Y-%m-%d')
                end_date = tiingo_loading_frame.loc[ticker_to_work]['endDate'].strftime('%Y-%m-%d')
                xdf = self.raw_load_tiingo_data(ticker=ticker_to_work, start_date=start_date, end_date=end_date)
                dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
                xdf.to_sql('tiingo__equities', dbconnx, if_exists='append', index=False)
                dbconnx.dispose()
                print(ticker_to_work)
            except:
                print("FAILED " +ticker_to_work)
                pass

    def output_tiingo_real_time_price_frame(self):
        ''' this function returns a real time output
        of all tiingo securities and their prices in a dataframe
        ''' 
        headers = {'Content-Type': 'application/json'}
        requestResponse = requests.get(f"https://api.tiingo.com/iex/?token={self.tiingo_key}", 
        headers=headers)
        big_response= requestResponse.json()
        real_time_tiingo_price_frame = pd.DataFrame(big_response)
        return real_time_tiingo_price_frame
    
    def output_todays_dividends(self):
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Adding 1 business day to today's date using Pandas
        next_business_date = pd.Timestamp(datetime.datetime.now().date())
        
        # Assuming `self.pw_map['tiingo']` correctly accesses your API token
        # You might need to adjust this part if 'self' is not defined in this context
        api_token = self.tiingo_key # Replace this with your actual API token
        
        # Making a GET request to the Tiingo API for corporate actions distributions
        request_url = f"https://api.tiingo.com/tiingo/corporate-actions/distributions?exDate={next_business_date.strftime('%Y-%m-%d')}&token={api_token}"
        request_response = requests.get(request_url, headers=headers)
        
        # Check if the request was successful
        if request_response.status_code == 200:
            # Convert the JSON response to a pandas DataFrame
            df = pd.DataFrame(request_response.json())
            #print(df)
        return df

