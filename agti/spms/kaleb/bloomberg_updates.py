import sqlalchemy
from agti.utilities.scheduler import TaskScheduler
from agti.utilities.data_update_details import DataUpdateDetails
from agti.utilities.db_manager import DBConnectionManager
from agti.data.bloomberg.standard_pulls import BloombergDailyDataTool
import datetime
from agti.utilities.settings import PasswordMapLoader
import pandas as pd
class KalebRequiredBloombergWrites:
    def __init__(self,pw_map):
        self.pw_map = pw_map
        self.bloomberg_daily_data_tool = BloombergDailyDataTool(pw_map=self.pw_map, bloomberg_connection=True)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.task_scheduler =  TaskScheduler()
    def output_recent_equity_peer_df(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        #equity_peers_dexed= pd.read_sql('select * from spm_typhus__us_equity_peers limit 1;', dbconnx)
        query = """
        SELECT *
        FROM spm_typhus__us_equity_peers
        WHERE peer_date = (
            SELECT MAX(peer_date)
            FROM spm_typhus__us_equity_peers
        )
        """
        
        max_peer_date = pd.read_sql(query, dbconnx)#['max_peer_date'].iloc[0]
        return max_peer_date
    def write_bloomberg_explict_table(self,field_name='px_open'):
        ''' example field name: px_open '''
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        # equity_peers_dexed= pd.read_sql('spm_typhus__us_equity_peers', dbconnx)
        equity_peers_dexed = self.output_recent_equity_peer_df()
        recent_peers = equity_peers_dexed[equity_peers_dexed['peer_date']==equity_peers_dexed['peer_date'].max()]
        all_tickers = list(set(list(recent_peers['ticker'].unique())
                 +list(recent_peers['ticker_to_peer'].unique())))
        all_bloomberg_tickers = list(set([i.lower().replace('.','/')+' us equity' for i in all_tickers]))
        px_opens = self.bloomberg_daily_data_tool.BDP(bbgTickers=all_bloomberg_tickers,field=field_name, overrides={})
        px_opens['date_of_update']=datetime.datetime.now()
        px_opens = px_opens.reset_index()
        px_opens.columns=['bloomberg_ticker','value','field','overrides','date_of_update']
        px_opens['sharadar_ticker']=px_opens['bloomberg_ticker'].apply(lambda x: x.split(' ')[0].upper().replace('/','.'))
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        px_opens.to_sql(f'bloomberg__{field_name}', 
                        dbconnx, if_exists='replace', index=False)
        print(f"Wrote {field_name}")

        
    def write_bloomberg_field_if_after_time(self, field_name='px_open', time_to_filter=datetime.time(9, 50)):
        valid_time=False


        valid_time =datetime.datetime.now().time()>time_to_filter

        if valid_time:
            self.write_bloomberg_explict_table(field_name=field_name)

    def write_bid_ask_prices__morning(self):
        self.write_bloomberg_field_if_after_time(field_name='px_bid_all_session', 
                                         time_to_filter=datetime.time(7, 00))
        self.write_bloomberg_field_if_after_time(field_name='px_ask_all_session', 
                                                 time_to_filter=datetime.time(7, 00))

    def write_open_prices_and_ivol(self):
        self.write_bloomberg_field_if_after_time(field_name='px_open', 
                                                 time_to_filter=datetime.time(11, 50))
        
        self.write_bloomberg_field_if_after_time(field_name='call_imp_vol_10d', 
                                                 time_to_filter=datetime.time(11, 50))
        
        self.write_bloomberg_field_if_after_time(field_name='px_volume', 
                                                 time_to_filter=datetime.time(11, 50))

    def run_morning_open_and_ivol_update_and_update_node(self):
        self.write_open_prices_and_ivol()
        data_update_details = DataUpdateDetails(pw_map=self.pw_map)
        db_table_ref ='bloomberg__px_open'
        data_update_details.update_node_on_user_data_update(user_name='spm_kaleb',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/spms/kaleb/bloomberg_updates.py',
            date_column='date_of_update',                                     
            db_table_ref=db_table_ref)

        db_table_ref ='bloomberg__call_imp_vol_10d'
        data_update_details.update_node_on_user_data_update(user_name='spm_kaleb',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/spms/kaleb/bloomberg_updates.py',
            date_column='date_of_update',                                     
            db_table_ref=db_table_ref)

    def write_morning_bid_offers_and_update_node(self):
        self.write_bid_ask_prices__morning()
        data_update_details = DataUpdateDetails(pw_map=self.pw_map)
        db_table_ref ='bloomberg__px_bid_all_session'
        data_update_details.update_node_on_user_data_update(user_name='spm_kaleb',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/spms/kaleb/bloomberg_updates.py',
            date_column='date_of_update',                                     
            db_table_ref=db_table_ref)

        db_table_ref ='bloomberg__px_ask_all_session'
        data_update_details.update_node_on_user_data_update(user_name='spm_kaleb',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/spms/kaleb/bloomberg_updates.py',
            date_column='date_of_update',                                     
            db_table_ref=db_table_ref)

    def schedule_pre_market_and_post_market_bid_ask_updates(self):
        """
        Schedules the run_full_sharadar_update_and_update_node function to run at 11:59 PM
        on Monday, Tuesday, Wednesday, Thursday, and Friday.
        """
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        times = ["07:00","08:45","09:15"]
        self.task_scheduler.schedule_tasks_for_days_and_times(self.write_morning_bid_offers_and_update_node, 
                                                              "write_morning_bid_offers_and_update_node", 
                                                              days, 
                                                              times)

    def schedule_post_open_ivol_and_open_updates(self):
        """
        Schedules the run_full_sharadar_update_and_update_node function to run at 11:59 PM
        on Monday, Tuesday, Wednesday, Thursday, and Friday.
        """
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        times = ["09:55"]
        self.task_scheduler.schedule_tasks_for_days_and_times(self.run_morning_open_and_ivol_update_and_update_node, 
                                                              "run_morning_open_and_ivol_update_and_update_node", 
                                                              days, 
                                                              times)
        
        