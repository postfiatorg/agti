from agti.data.bloomberg.intraday_pulls import *
from agti.utilities.db_manager import DBConnectionManager
from agti.data.tiingo.forex import TiingoFXTool
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.settings import CredentialManager
from agti.live_trading.universe_generation.fast_equity_pull import FastEquityPull
from agti.utilities.db_manager import DBConnectionManager
import datetime
from agti.utilities.google_sheet_manager import GoogleSheetManager
from agti.data.bloomberg.standard_pulls import BloombergDailyDataTool
class FXDroidCache:
    def __init__(self,pw_map):
        self.pw_map=pw_map
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.intraday_bloomberg_tool =IntradayBloombergTool()
        self.google_sheet_manager = GoogleSheetManager(prod_trading=True)
        self.bloomberg_daily_data_tool= BloombergDailyDataTool(pw_map=pw_map, 
                                                               bloomberg_connection=True)
    def update_all_forex_half_hourly_history(self):
        existing_unique_ids = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            all_hist = pd.read_sql('spm_angron__bloomberg_halfhour_cache__close', dbconnx)
            existing_unique_ids = all_hist['unique_identifier'].unique()
        except:
            pass
        g10_fx_crosses = ['eurusd', 'usdjpy', 'gbpusd', 'audusd', 'nzdusd', 'usdcad', 'usdchf', 'usdnok', 'usdsek']
        liquid_non_g10_fx_crosses = ['usdmxn', 'usdsgd', 'usdhkd', 'usdzar','usdpln','usdhuf','usdcnh']
        all_bloomberg_ticker_constructor = g10_fx_crosses+liquid_non_g10_fx_crosses
        all_bloomberg_currencies = [i+' curncy' for i in all_bloomberg_ticker_constructor]
        startDateTime = datetime.datetime(2022,11,1,1,30)
        for xcurrency in all_bloomberg_currencies:
            fx_hist=self.intraday_bloomberg_tool.get_intraday_history(ticker =xcurrency, field_name='close', interval=30, startDateTime=startDateTime,
                                    endDateTime=datetime.datetime.now())  
            fx_dexed = fx_hist.set_index('unique_identifier')
            incremental_write = fx_dexed[~fx_dexed.index.get_level_values(0).isin(existing_unique_ids)].reset_index().copy()
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            incremental_write.to_sql('spm_angron__bloomberg_halfhour_cache__close', dbconnx, if_exists='append')
            dbconnx.dispose()                   
            print(f'Completed {xcurrency}')
            length_added = len(incremental_write)
            print(f"{length_added} length added")
        existing_unique_ids = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            all_hist = pd.read_sql('spm_angron__bloomberg_halfhour_cache__open', dbconnx)
            existing_unique_ids = all_hist['unique_identifier'].unique()
        except:
            pass
        for xcurrency in all_bloomberg_currencies:
            fx_hist=self.intraday_bloomberg_tool.get_intraday_history(ticker =xcurrency, field_name='open', interval=30, startDateTime=startDateTime,
                                    endDateTime=datetime.datetime.now())  
            fx_dexed = fx_hist.set_index('unique_identifier')
            incremental_write = fx_dexed[~fx_dexed.index.get_level_values(0).isin(existing_unique_ids)].reset_index().copy()
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            incremental_write.to_sql('spm_angron__bloomberg_halfhour_cache__open', dbconnx, if_exists='append')
            dbconnx.dispose()                   
            print(f'Completed {xcurrency}')
            length_added = len(incremental_write)
            print(f"{length_added} length added")
    def generate_initial_minutely_cache(self):
        ticker_to_work='usdjpy index'
        startDateTime= datetime.datetime.now()-datetime.timedelta(365*5)
        update_to_append=self.intraday_bloomberg_tool.get_intraday_history(ticker =ticker_to_work, field_name='close', interval=1, startDateTime=startDateTime,
                                endDateTime=datetime.datetime.now()+timedelta(1))  
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        all_table_names = sqlalchemy.inspect(dbconnx).get_table_names()
        if 'spm_angron__bloomberg_minutely_cache' not in all_table_names:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            update_to_append.to_sql('spm_angron__bloomberg_minutely_cache', dbconnx, if_exists='replace')

        if 'spm_angron__bloomberg_minutely_cache' in all_table_names:
            print('Already initialized')


    def get_past_week_minutely_cache(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        end_date = datetime.datetime.now()
        start_date = end_date - timedelta(days=7)
        
        query = f"""
        SELECT * FROM spm_angron__bloomberg_minutely_cache
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """

        past_week_data = pd.read_sql(query, dbconnx)
        dbconnx.dispose()

        return past_week_data


    def update_minutely_cache_with_unique_records(self, ticker='eurusd curncy', xdays_ago=365*5):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        
        # Fetch all unique identifiers from the existing minutely cache
        all_unique_identifiers = list(pd.read_sql(
            """SELECT unique_identifier FROM spm_angron__bloomberg_minutely_cache;""",
            dbconnx
        )['unique_identifier'].unique())
        
        # Get new data from Bloomberg
        startDateTime = datetime.datetime.now() - datetime.timedelta(xdays_ago)
        endDateTime = datetime.datetime.now() + timedelta(1)
        new_data = self.intraday_bloomberg_tool.get_intraday_history(
            ticker=ticker,
            field_name='close',
            interval=1,
            startDateTime=startDateTime,
            endDateTime=endDateTime
        )
        
        # Filter new data to only include records with unique identifiers not already in the database
        new_data_dexed = new_data.set_index('unique_identifier')
        incremental_write = new_data_dexed[~new_data_dexed.index.isin(all_unique_identifiers)].reset_index().copy()
        
        # Append the new unique data to the database
        incremental_write.to_sql('spm_angron__bloomberg_minutely_cache', dbconnx, if_exists='append')
        
        dbconnx.dispose()
        
        print(f'Completed updating {ticker}')
        length_added = len(incremental_write)
        print(f"{length_added} new records added")

    def update_minutely_cache_for_multiple_tickers(self, tickers, xdays_ago=365*5):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')

        # Fetch all unique identifiers from the existing minutely cache
        all_unique_identifiers = set(pd.read_sql(
            """SELECT unique_identifier FROM spm_angron__bloomberg_minutely_cache;""",
            dbconnx
        )['unique_identifier'].unique())

        startDateTime = datetime.datetime.now() - datetime.timedelta(xdays_ago)
        endDateTime = datetime.datetime.now() + timedelta(1)

        for ticker in tickers:
            # Get new data from Bloomberg
            new_data = self.intraday_bloomberg_tool.get_intraday_history(
                ticker=ticker,
                field_name='close',
                interval=1,
                startDateTime=startDateTime,
                endDateTime=endDateTime
            )

            # Filter new data to only include records with unique identifiers not already in the database
            new_data_dexed = new_data.set_index('unique_identifier')
            incremental_write = new_data_dexed[~new_data_dexed.index.isin(all_unique_identifiers)].reset_index().copy()
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
            # Append the new unique data to the database
            incremental_write.to_sql('spm_angron__bloomberg_minutely_cache', dbconnx, if_exists='append')
            dbconnx.dispose()
            print(f'Completed updating {ticker}')
            length_added = len(incremental_write)
            print(f"{length_added} new records added")

        dbconnx.dispose()

    def update_all_core_interest_rate_data(self, xdays_of_update=3):
        fx_map = self.google_sheet_manager.load_google_sheet_as_df(workbook='odv', 
                                                                   worksheet='fx_map')
        all_currencies = [i for i in fx_map[['ois_swap','standard_fx']].head(10)['standard_fx'].unique() if i !='']
        all_swaps = list(fx_map[['ois_swap','standard_fx']].head(10)['ois_swap'].unique())
        droid_items = all_currencies+all_swaps
        self.update_minutely_cache_for_multiple_tickers(tickers =droid_items, xdays_ago=xdays_of_update)

    def generate_all_core_fx_histories(self):
        fx_map = self.google_sheet_manager.load_google_sheet_as_df(workbook='odv', 
                                                                       worksheet='fx_map')
        full_fx_df = fx_map[['fx','localbigmac','citi_economic_index','bbg_equity','bbg_total_ret']].head(10).copy()
        full_fx_df['localbigmac']=full_fx_df['localbigmac'].apply(lambda x: str(x).lower())
        all_extra_tickers = list(full_fx_df['localbigmac'])+list(full_fx_df['citi_economic_index'])+list(full_fx_df['bbg_equity'])+list(full_fx_df['bbg_total_ret'])
        yarr=[]
        for xticker in all_extra_tickers:
            hist_df = self.bloomberg_daily_data_tool.BDH(bbgTicker=xticker,
                field='px_last',
                startDate='2006-01-01',
                endDate=(datetime.datetime.now()+datetime.timedelta(1)).strftime('%Y-%m-%d'),
                periodicity='DAILY',
                overrides={})
            yarr.append(hist_df)
        fx_df = pd.concat(yarr)
        return fx_df