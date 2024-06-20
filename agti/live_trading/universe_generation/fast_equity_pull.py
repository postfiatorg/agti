from agti.utilities.db_manager import DBConnectionManager
import sqlalchemy
import pandas as pd 
import datetime
class FastEquityPull:
    def __init__(self,pw_map):
        self.pw_map = pw_map
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.default_equity_map = self.generate_default_equity_map()
    def generate_default_equity_map(self):
        dbconn_x = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        full_active_universe = pd.read_sql('sharadar__tickers', dbconn_x)
        active_uni_redux = full_active_universe.groupby('ticker').first().copy()
        stock_types = {
            'Domestic Common Stock': 'sharadar',
            'ADR Common Stock Secondary Class': 'sharadar',
            'ETF': 'tiingo',
            'ADR Common Stock': 'sharadar',
            'ADR Common Stock Primary Class': 'sharadar',
            'CEF': 'sharadar',
            'Domestic Common Stock Primary Class': 'sharadar',
            'Domestic Common Stock Secondary Class': 'sharadar',
            'Domestic Common Stock Warrant': 'sharadar',
            'Canadian Common Stock': 'sharadar',
            'Domestic Preferred Stock': 'sharadar',
            'ETD': 'tiingo',
            'ETN': 'tiingo',
            'ADR Common Stock Warrant': 'sharadar',
            'CEF Warrant': 'sharadar',
            'ADR Preferred Stock': 'sharadar',
            'Canadian Common Stock Primary Class': 'sharadar',
            'Canadian Common Stock Secondary Class': 'sharadar',
            'CEF Preferred': 'sharadar',
            'UNIT': 'sharadar',
            'ETMF': 'tiingo',
            'Canadian Common Stock Warrant': 'sharadar',
            'Institutional Investor': 'sharadar',
            'Canadian Preferred Stock': 'sharadar',
            'IDX': 'tiingo'}
        active_uni_redux['default_data_set']=active_uni_redux['category'].map(stock_types)
        return active_uni_redux

    def get_full_equity_history_for_ticker_list(self, list_of_tickers, start_date):
        """ 
            list_of_tickers = ['AMZN','SPY','QQQ','BABA','AGNC','REM',"GLD",'EZA']
            start_date = '2004-01-01'
        """
        list_of_tickers = [i for i in list_of_tickers if i in self.default_equity_map.index]
        invalid_tickers = [i for i in list_of_tickers if i not in self.default_equity_map.index]
        print("INVALID TICKERS")
        print(' '.join(invalid_tickers))
        
        pull_map_creation = self.default_equity_map.loc[list_of_tickers][['default_data_set']]
        sharadar_tickers = list(pull_map_creation[pull_map_creation['default_data_set'] =='sharadar'].index)
        tiingo_tickers = list(pull_map_creation[pull_map_creation['default_data_set'] =='tiingo'].index)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        
        query = f"""
        SELECT *
        FROM sharadar__sep
        WHERE ticker IN ({', '.join([f"'{ticker}'" for ticker in sharadar_tickers])})
        AND date > '{start_date}'
        """
        
        ## Execute the query and load the result into a DataFrame
        sharadar_df__full = pd.read_sql(query, dbconnx,dtype={'date': 'datetime64[ns]'})
        #result_df.head()
        ## Create a SQL query to get all columns/rows from tiingo__equities where the ticker is in tiingo_tickers and simple_date > start_date
        query = f"""
        SELECT *
        FROM tiingo__equities
        WHERE ticker IN ({', '.join([f"'{ticker}'" for ticker in tiingo_tickers])})
        AND simple_date > '{start_date}'
        """
        
        ## Execute the query and load the result into a DataFrame
        tiingo_df = pd.read_sql(query, dbconnx, dtype = {'simple_date': 'datetime64[ns]'})
        sharadar_df__full['openadj']=(sharadar_df__full['closeadj']/sharadar_df__full['close']) * sharadar_df__full['open']
        sharadar_df__full['dv']=sharadar_df__full['close']*sharadar_df__full['volume']
        tiingo_df['dv']=tiingo_df['volume']*tiingo_df['close']
        tiingo_ohlc = tiingo_df[['simple_date','adjClose','adjOpen','dv','ticker','close','open','high','low']].copy()
        sharadar_ohlc = sharadar_df__full[['date','closeadj','openadj','dv','ticker','close','open','high','low']]
        tiingo_ohlc.columns=['date','closeadj','openadj','dv','ticker','close','open','high','low']
        full_combined_output=pd.concat([tiingo_ohlc, sharadar_ohlc]).groupby(['ticker','date']).last().sort_index()
        dbconnx.dispose()
        return full_combined_output

    def write_active_universe(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        start_date = (datetime.datetime.now()-datetime.timedelta(90)).strftime('%Y-%m-%d')
        query = f"""
        SELECT ticker,close,volume,simple_date
        FROM tiingo__equities
        WHERE simple_date > '{start_date}'
        """
        tiingo_df = pd.read_sql(query, dbconnx, dtype = {'simple_date': 'datetime64[ns]'})
        tiingo_df['dv']=tiingo_df['close']*tiingo_df['volume']
        total_dollar_volume = tiingo_df[['ticker','dv']].groupby('ticker').sum()
        etfs_dv_above_1m= total_dollar_volume[total_dollar_volume['dv']>=1_000_000].copy()
        
        query = f"""
        SELECT date,ticker,volume,close
        FROM sharadar__sep
        WHERE date > '{start_date}'
        """
        sharadar_df = pd.read_sql(query, dbconnx, dtype = {'date': 'datetime64[ns]'})
        sharadar_df['dv']=sharadar_df['close']*sharadar_df['volume']
        stocks_dv_above_1m = sharadar_df[['dv','ticker']].groupby('ticker').sum()
        
        stocks_dv_above_1m['data_source']='sharadar'
        etfs_dv_above_1m['data_source']='tiingo'
        
        full_active_universe = pd.concat([stocks_dv_above_1m, etfs_dv_above_1m])
        full_active = full_active_universe.reset_index()
        full_active_universe_df  =full_active.sort_values('dv',ascending=False)
        full_active_universe_df=full_active_universe_df.sort_values('dv',ascending=False)
        full_active_universe_df['bloomberg_ticker']=full_active_universe_df['ticker'].apply(lambda x: x.lower()+ ' us equity')
        full_active_write = full_active_universe_df.groupby('ticker').first().sort_values('dv',ascending=False).copy().reset_index()
        full_active_write['date']=datetime.datetime.now().strftime('%Y-%m-%d')
        full_active_write['weight']= full_active_write['dv']/full_active_write['dv'].sum()
        full_active_write['universe_name']='active_us_equities'
        full_active_write['universe_owner']='agti_corp'
        print('WRITING FULL ACTIVE UNIVERSE TO full_active_universe')
        full_active_write.to_sql('full_active_universe', dbconnx, if_exists='append')

    ## SQL query to get the most recent active universe
    def get_most_recent_active_universe(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        query = """
        SELECT *
        FROM full_active_universe
        WHERE date = (SELECT MAX(date) FROM full_active_universe)
        """
        
        ## Execute the query and load the result into a DataFrame
        most_recent_active_universe = pd.read_sql(query, dbconnx)
        return most_recent_active_universe
    
    def load_standard_equity_df(self):
        recent_active_uni = self.get_most_recent_active_universe()
        valid_stocks = recent_active_uni[recent_active_uni['dv']>=1_000_000].copy()
        full_list_of_tickers = list(valid_stocks['ticker'])
        full_eq_h=self.get_full_equity_history_for_ticker_list(full_list_of_tickers, start_date=datetime.datetime.now()-datetime.timedelta(365*4))
        return full_eq_h