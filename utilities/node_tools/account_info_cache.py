from agti.utilities.settings import CredentialManager
from agti.utilities.node_tools.alerting_tool import AlertingTool
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
import time 
import pandas as pd 
import numpy as np
class AccountCaching:
    def __init__(self,pw_map, node_name):
        self.node_name = node_name
        self.pw_map = pw_map
        self.account_address = self.pw_map[f'{self.node_name}__v1xrpaddress']
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.recent_node_transactions =self.output_recent_node_transactions()
    def output_db_unique_keys(self):
        unique_keys= []
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
            existing_cache = pd.read_sql(f'{self.node_name}__node_pft_transaction_info_cache',dbconnx)
            unique_keys= existing_cache['unique_key'].unique()
            dbconnx.dispose()
        except:
            print('error connecting to db')
            pass
        return unique_keys
        
    def update_node_transactions(self):
        full_memo_detail_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.account_address,
            transaction_limit=2000,
            pft_only=True,
            exhaustive=True)
        existing_unique_keys = list(self.output_db_unique_keys())
        full_caching_df = self.generic_pft_utilities.convert_memo_detail_df_into_essential_caching_details(memo_details_df=full_memo_detail_df)
        sorted_write = full_caching_df.groupby('unique_key').last().sort_values('datetime')
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        records_to_write = sorted_write[sorted_write.index.get_level_values(0).isin(existing_unique_keys) 
        == False]
        length_of_records = len(records_to_write)
        print(f'writing {length_of_records} records')
        records_to_write.to_sql(f'{self.node_name}__node_pft_transaction_info_cache',dbconnx, if_exists='append')
        dbconnx.dispose()

    def output_recent_node_transactions(self):
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
            output = pd.read_sql(f'{self.node_name}__node_pft_transaction_info_cache',dbconnx)
            dbconnx.dispose()
        except:
            print("WRITING NODE TRANSACTIONS")
            self.update_node_transactions()
            time.sleep(5)
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
            output = pd.read_sql(f'{self.node_name}__node_pft_transaction_info_cache',dbconnx)
            dbconnx.dispose()
            return output
        return output

    def generate_recent_update_map(self):
        recent_node_tx = self.output_recent_node_transactions().copy()
        print(len(recent_node_tx))
        self.update_node_transactions()
        updated_node_tx = self.output_recent_node_transactions().copy()
        print(len(updated_node_tx))
        updated_node_tx__dexed = updated_node_tx.set_index('unique_key')
        recent_node_tx__dexed = recent_node_tx.set_index('unique_key')
        unique_hashes = [i for i in list(updated_node_tx__dexed.index) if i not in recent_node_tx__dexed.index]
        full_df_to_print_info_on= updated_node_tx__dexed.loc[unique_hashes]
        hashes_to_work = list(full_df_to_print_info_on.index)
        string_constructor = ''
        for xhash in hashes_to_work:
            slice_select = full_df_to_print_info_on.loc[xhash]
            memo_data= slice_select['memo_data']
            task_id = slice_select['memo_type']
            user= slice_select['memo_format']
            hash= slice_select['hash']
            directional_pft= slice_select['directional_pft']
            url = f'https://livenet.xrpl.org/transactions/{hash}/detailed'
            string_appender = f"""User: {user}
Task ID: {task_id}
Message: {memo_data}
PFT: {directional_pft}
URL: {url}
"""
            string_constructor= string_constructor+string_appender
        output_map = {'update_string':string_constructor, 'full_df_to_print_info_on':full_df_to_print_info_on}
        return output_map

    def initialize_discord_log_db(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        recent_node_tx = self.output_recent_node_transactions()
        recent_node_tx.to_sql(f'{self.node_name}__node_pft_discord_log_cache',dbconnx, if_exists='replace')
        dbconnx.dispose()
    def output_discord_log_db(self):
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
            op= pd.read_sql(f'{self.node_name}__node_pft_discord_log_cache', dbconnx)
            dbconnx.dispose()
        except:
            self.initialize_discord_log_db()
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
            op= pd.read_sql(f'{self.node_name}__node_pft_discord_log_cache', dbconnx)
            dbconnx.dispose()
            pass
        return op

    def update_discord_log_with_outputs_returned_as_string(self):
        discord_log_db = self.output_discord_log_db()
        update_map = self.generate_recent_update_map()
        update_dexed = update_map['full_df_to_print_info_on']
        discord_dexed = discord_log_db.set_index('unique_key')
        append_to_discord_db = update_dexed[~update_dexed.index.isin(discord_dexed.index)].copy().reset_index()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        append_to_discord_db.to_sql(f'{self.node_name}__node_pft_discord_log_cache',dbconnx, if_exists='append', index=False)
        final_string_output = update_map['update_string']
        return final_string_output

    def output_discord_output_and_writing_map(self):
        discord_log_db = self.output_discord_log_db()
        discord_dexed = discord_log_db.set_index('unique_key')
        self.update_node_transactions()
        updated_node_tx = self.output_recent_node_transactions().copy()
        update_dexed = updated_node_tx.set_index('unique_key')
        full_df_to_print_info_on=  update_dexed[~update_dexed.index.isin(discord_dexed.index)]
        hashes_to_work = list(full_df_to_print_info_on.index)
        string_constructor = ''
        for xhash in hashes_to_work:
            slice_select = full_df_to_print_info_on.loc[xhash]
            memo_data= slice_select['memo_data']
            task_id = slice_select['memo_type']
            user= slice_select['memo_format']
            hash= slice_select['hash']
            directional_pft= slice_select['directional_pft']
            url = f'https://livenet.xrpl.org/transactions/{hash}/detailed'
            string_appender = f"""User: {user}
Task ID: {task_id}
Message: {memo_data}
PFT: {directional_pft}
URL: {url}
"""
            string_constructor= string_constructor+string_appender
        to_write_to_df = full_df_to_print_info_on.reset_index()
        return {'string_to_output': string_constructor,'to_write_to_discord_db':to_write_to_df }

    def process_map_to_write_to_discord_db(self, discord_map):
        """
        writing_map = account_caching.output_discord_output_and_writing_map()
        raw_string = writing_map['string_to_output']
        ## add discord message logic here 
        account_caching.process_map_to_write_to_discord_db(discord_map =writing_map)
        """
        df = discord_map['to_write_to_discord_db']
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.node_name)
        df.to_sql(f'{self.node_name}__node_pft_discord_log_cache',dbconnx, if_exists='append', index=False)
    