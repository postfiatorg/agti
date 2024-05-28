from agti.utilities.settings import PasswordMapLoader
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
import pandas as pd
import time
import datetime 
import re
from agti.utilities.db_manager import DBConnectionManager
## The alerting tool checks the current state of the node and determines if there are any new memos
## if there are then it outputs a string.
## this is often used in Discord functionality to alert the user of new memos
class AlertingTool:
    def __init__(self,pw_map,node_name='postfiat_node'):
        self.pw_map = pw_map
        self.user_name = node_name
        self.node_name = node_name
        self.node_address = self.pw_map[f'{node_name}__v1xrpaddress']
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        #self.node_address = node_address
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.full_memo_detail = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address, 
                                                                transaction_limit=5000, pft_only=True)
   
        self.update_unique_hash_db_for_node()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        self.current_hash_date_df = pd.read_sql(self.node_name+'__unique_hash_db',dbconnx)
        dbconnx.dispose()
    def update_unique_hash_db_for_node(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        included_hashes = []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
            all_node_hahes = pd.read_sql(self.node_name+'__unique_hash_db',dbconnx)
            included_hashes = list(all_node_hahes['hash'].unique())
        except:
            pass
        full_memo_detail = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address, 
                                                                        transaction_limit=5000, pft_only=True)
        hash_to_write_df__dexed = full_memo_detail.set_index('hash')[['datetime']].copy()
        hash_to_write_undexed = hash_to_write_df__dexed[~hash_to_write_df__dexed.index.get_level_values(0).isin(included_hashes)].reset_index().copy()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        hash_to_write_undexed.to_sql(self.node_name+'__unique_hash_db',dbconnx,if_exists='append')
        length_of_new_writes = len(hash_to_write_undexed)
        print(f'wrote {length_of_new_writes}')
        return length_of_new_writes
    def determine_if_there_are_new_memos(self):
        test_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address, 
                                                               transaction_limit=5000, pft_only=True)
        update_str=''
        if test_df == self.full_memo_detail:
            print("No New Memos")

        if test_df != self.full_memo_detail:
            print("New Memos on the Tape")

    def get_incremental_hash_dataframe(self):
        """ for the node address get all the hashes that are not reflected in the current state"""
        test_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address, 
                                                                       transaction_limit=5000, pft_only=True)
        test_df_dexed = test_df.set_index('hash')
        all_accounted_for_hashes = list(self.current_hash_date_df['hash'].unique())
        incremental_data = test_df_dexed[test_df_dexed.index.get_level_values(0).isin(all_accounted_for_hashes) 
        == False].copy()
        return {'incremental_data': incremental_data, 'new_state':test_df}
        
    def create_incremental_update_string(self, incremental_data):
        ystr=''
        incremental_data_cap = incremental_data# .tail(30)
        for hash_to_work in incremental_data_cap.index:
            
            hash_memo_map = incremental_data['converted_memos'][hash_to_work]
            user_name = hash_memo_map['MemoFormat']
            task_id = hash_memo_map['MemoType']
            full_output = hash_memo_map['MemoData']
            formatted_incremental_string = f"""User: {user_name}
Task ID: {task_id}
Memo: {full_output}
URL: https://livenet.xrpl.org/transactions/{hash_to_work}/detailed

"""
            ystr= ystr+formatted_incremental_string
        return ystr
    def get_incremental_update_string_and_reset_df(self):
        incremental_hash_map = self.get_incremental_hash_dataframe()
        incremental_update_string = self.create_incremental_update_string(incremental_hash_map['incremental_data'])
        op = self.update_unique_hash_db_for_node()
        if op == 0:
            op = self.update_unique_hash_db_for_node()
            time.sleep(3)
        if op == 0:
            op = self.update_unique_hash_db_for_node()
            time.sleep(3)
        time.sleep(5)
        self.update_unique_hash_db_for_node()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        self.current_hash_date_df = pd.read_sql(self.node_name+'__unique_hash_db',dbconnx)
        return incremental_update_string
        
    def extract_written_hashes_from_discord_output(self,discord_output):
        """
        example:
        
        User: goodalexander
        Task ID: 2024-05-27_08:38==BS40
        Memo: working on the csv bulk data load functionality and moving to dbconnection manager xx
        URL: https://livenet.xrpl.org/transactions/C433E4501C2E16163768C149C3D510FC033DB149C0572B23CDF2E78A7605DF4F/detailed
        User: goodalexander
        Task ID: 2024-05-27_08:38==BS40
        Memo: working on the csv bulk data load functionality and moving to dbconnection manager yy
        URL: https://livenet.xrpl.org/transactions/77E2E3671003A75C73DF994911C536D2A75F7F305A3DB6A875ED66F2DAE08897/detailed
        """

        data = discord_output
        
        
        # Regular expression to find the transaction hash values
        pattern = r"https://livenet\.xrpl\.org/transactions/([A-F0-9]+)/detailed"
        hashes = re.findall(pattern, data)
        discord_hash_coverage = pd.DataFrame(hashes)
        discord_hash_coverage.columns=['hash']
        discord_hash_coverage['datetime']= datetime.datetime.now()
        return discord_hash_coverage