from agti.data.sec_methods.request_utility import SECRequestUtility
from agti.utilities.db_manager import DBConnectionManager
import datetime
from io import StringIO
import pandas as pd
## CIKs are SEC unique identifiers. This script updates them to the nodes postgres
## database 
class RunCIKUpdate:
    def __init__(self,pw_map, user_name):
        self.pw_map= pw_map
        self.sec_request_utility = SECRequestUtility(pw_map=self.pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        if user_name != None:
            self.user_name = user_name
        if user_name == None:
            self.user_name = pw_map['node_name']
        #self.load_sec_cik_map()
    ## Utility functions     
    def load_sec_cik_df(self):
        """ This pulls down all the SEC Internal Identifiers""" 
        cik_item = self.sec_request_utility.compliant_request('https://www.sec.gov/include/ticker.txt')
        ticker_to_cik_map=pd.read_csv(StringIO(cik_item.text),sep="\t",header=None)
        # ticker_to_cik_map=pd.read_csv('https://www.sec.gov/include/ticker.txt',sep="\t",header=None)
        ticker_to_cik_map.columns=['ticker','cik']
        ticker_to_cik_map['ticker']=ticker_to_cik_map['ticker'].apply(lambda x: str(x).upper())
        ticker_to_cik_map['cik']=ticker_to_cik_map['cik'].apply(lambda x: str(x).zfill(10))
        output=ticker_to_cik_map.groupby('ticker').last()[['cik']]
        output['date_of_update']= datetime.datetime.now()
        return output

    def write_cik_df(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        cik_df = self.load_sec_cik_df()
        cik_df.to_sql('sec__update_cik', dbconnx, if_exists='replace')
        dbconnx.dispose()

    def write_cik_df_if_stale(self):
        days_stale = self.determine_how_many_days_stale_cik_update_is()
        if days_stale>0:
            self.write_cik_df()

    def determine_how_many_days_stale_cik_update_is(self):
        days_stale=10000
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
            cik_df = pd.read_sql('sec__update_cik', dbconnx)
            days_stale= (datetime.datetime.now()-list(cik_df['date_of_update'])[0]).days
            dbconnx.dispose()
        except:
            pass
        return days_stale
    
    def output_cached_cik_df(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        cik_df = pd.read_sql('sec__update_cik', dbconnx)
        dbconnx.dispose()
        return cik_df
    