from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.data_update_details import DataUpdateDetails
from agti.data.fmp.market_data import FMPMarketDataRetriever
import pandas as pd
import datetime
class FMPLiveMarketData:
    def __init__(self,pw_map):
        self.pw_map=pw_map
        #self.db_connection_manager = DBConnectionManager
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.fmp_market_data_retriever = FMPMarketDataRetriever(pw_map=self.pw_map)
        self.equity_peers = self.get_equity_peers()
    def get_equity_peers(self):
        """
        Retrieves the latest equity peers data from the database.
        
        Returns:
            pd.DataFrame: A DataFrame containing the latest equity peers data.
        """
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        query = """
        SELECT *
        FROM spm_typhus__us_equity_peers
        WHERE peer_date = (
            SELECT MAX(peer_date)
            FROM spm_typhus__us_equity_peers
        )
        """
        equity_peers_dexed = pd.read_sql(query, dbconnx)
        
        # Additional processing if needed
        op_df = equity_peers_dexed.reset_index()
        op_df['unique_key'] = op_df['ticker_to_peer'] + '__' + op_df['ticker'] + '__' + op_df['peer_date'].astype(str) + '__' + op_df['peer_type'].astype(str)
        op_df = op_df.groupby('unique_key').last().reset_index()
        
        return op_df
    def output_full_live_market_data(self):
        equity_peer_df= self.equity_peers
        all_tickers_to_work = list(equity_peer_df['ticker'].unique())+list(equity_peer_df['ticker_to_peer'].unique())
        full_live_quote_batch__intraday= self.fmp_market_data_retriever.retrieve_batch_equity_data(symbols=all_tickers_to_work, batch_size=1000)
        full_live_quote_batch__intraday['update_time']=datetime.datetime.now()
        return full_live_quote_batch__intraday
    def write_full_live_market_data(self):
        equity_peer_df = self.get_equity_peers()
        all_tickers_to_work = list(equity_peer_df['ticker'].unique())+list(equity_peer_df['ticker_to_peer'].unique())
        full_live_quote_batch__intraday= self.fmp_market_data_retriever.retrieve_batch_equity_data(symbols=all_tickers_to_work, batch_size=1000)
        full_live_quote_batch__intraday['update_time']=datetime.datetime.now()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        full_live_quote_batch__intraday.to_sql('fmp__live_intraday_data', dbconnx, if_exists='replace')

    def write_full_live_market_data_and_update_node(self):
        """
        Writes full live market data and updates the node.
        """
        data_update_details = DataUpdateDetails(pw_map=self.pw_map)
        self.write_full_live_market_data()
        db_table_ref = 'fmp__live_intraday_data'
        data_update_details.update_node_on_user_data_update(
            user_name='spm_kaleb',
            node_name='agti_corp',
            task_id='2024-05-29_14:30__TK38',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/spms/typhus/run_fmp_market_data_update.py',
            date_column='update_time',
            db_table_ref=db_table_ref
        )
        print("UPDATED FMP LIVE MARKET DATA")

    def schedule_market_data_update(self):
        """
        Schedules the write_full_live_market_data_and_update_node function to run
        at 10 AM, 12 PM, 2 PM, 2:30 PM, and 2:55 PM on weekdays.
        """
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        times = ["10:00", "12:00", "14:00", "14:30", "14:55"]
        self.task_scheduler.schedule_tasks_for_days_and_times(
            self.write_full_live_market_data_and_update_node,
            "write_full_live_market_data_and_update_node",
            days,
            times
        )