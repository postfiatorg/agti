import numpy as np
from agti.data.sharadar.sharadar_bulk_update import SharadarDataUpdate
from agti.utilities.db_manager import DBConnectionManager
import pandas as pd 
class SF1Aggregates:
    def __init__(self,pw_map):
        self.password_map_loader = pw_map
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.ticker_table = pd.read_sql("select * from sharadar__tickers;", 
                           self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp'))
        self.ticker_map = self.ticker_table.groupby('ticker').first()
        self.us_corps_only = self.output_sf1_dexed()
        self.us_corps_only['ticker_copy']= self.us_corps_only.index.get_level_values(0)
        self.us_corps_only['ticker_is4']=np.where(self.us_corps_only['ticker_copy']==self.us_corps_only['ticker_copy'].shift(4),1,np.nan)
        self.us_corps_only['ticker_is12']=np.where(self.us_corps_only['ticker_copy']==self.us_corps_only['ticker_copy'].shift(12),1,np.nan)
        self.us_corps_only['ebt__4']= (self.us_corps_only['ticker_is4']*self.us_corps_only['ebt']).rolling(4).sum()
        self.us_corps_only['netinc__4']= (self.us_corps_only['ticker_is4']*self.us_corps_only['netinc']).rolling(4).sum()
        self.us_corps_only['fcf__4']= (self.us_corps_only['ticker_is4']*self.us_corps_only['fcf']).rolling(4).sum()
        self.all_non_financial_and_utes = list(self.ticker_map[(self.ticker_map['sector'] !='Financial Services') 
                                               & (self.ticker_map['sector'] !='Utilities')].index.unique())

    def output_sf1_dexed(self):
        full_sf1_hist = pd.read_sql("select * from sharadar__sf1 WHERE dimension='ARQ';", self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp'))
        full_sf1_dex = full_sf1_hist.groupby(['ticker','calendardate']).last().sort_index().reset_index()#.groupby(['ticker','datekey']).last()
        datekey_map = pd.DataFrame(full_sf1_dex['datekey'].unique())
        datekey_map.columns=['datemapper']
        datekey_map['datekey']=pd.to_datetime(datekey_map['datemapper'])
        date_mapper_to_datekey = datekey_map.groupby('datemapper').first()['datekey']
        
        full_sf1_hist['datekey']=full_sf1_hist['datekey'].map(date_mapper_to_datekey)
        sf1_dexed = full_sf1_hist.groupby(['ticker','datekey']).last()
        usd_only = sf1_dexed[sf1_dexed['fxusd'] == 1].copy()
        all_us_corps = list(self.ticker_map[self.ticker_map['location'].apply(lambda x: 'U.S.A' in str(x))].index)
        us_corps_only= sf1_dexed[sf1_dexed.index.get_level_values(0).isin(all_us_corps)].copy()
        return us_corps_only

    def output_field_total(self, field_to_get='ebt'):
        ebt_value = self.us_corps_only[field_to_get].unstack(0).sort_index().resample('D').last()
        total_ebt =ebt_value.fillna(method='pad',limit=180).sum(1)
        return total_ebt

    def calculate_4_sum_field(self,field_to_run='ebt'):
        self.us_corps_only[f'{field_to_run}__4']= (self.us_corps_only['ticker_is4']*self.us_corps_only[field_to_run]).rolling(4).sum()

    def calculate_12_sum_field(self,field_to_run='ebt'):
        self.us_corps_only[f'{field_to_run}__12']= (self.us_corps_only['ticker_is12']*self.us_corps_only[field_to_run]).rolling(12).sum()
#self = USDebtProblemSimulation(pw_map=password_map_loader.pw_map)