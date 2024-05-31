from agti.utilities.ai_jupyter import NotebookAITool
#sqlalchemy.inspect(engine).get_table_names()
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.settings import CredentialManager
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
from agti.utilities.user_tools.user_node_request import UserNodeTaskRequest
from agti.utilities.db_manager import DBConnectionManager
import datetime
import zlib
import pandas as pd
import numpy as np
class DataUpdateDetails:
    def __init__(self,pw_map):
        #print(x)
        self.pw_map= pw_map
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)

    def get_most_recent_update_date_for_table(self,table_name = 'sec__update_cik', 
                                              date_column = 'date_of_update',user_name = 'spm_typhus'):
        
        xquery = f"""SELECT MAX({date_column}) AS most_recent_date FROM {table_name};"""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=user_name)
        result = pd.read_sql(xquery, dbconnx)
        return list(result['most_recent_date'])[0]

    
    
    def construct_data_update_row(self, user_name,node_name, 
                                  task_id, full_evidence_url,date_column,db_table_ref):
        shortened_url = self.generic_pft_utilities.shorten_url(full_evidence_url)
        node_address = self.pw_map[f'{node_name}__v1xrpaddress']
        user_address = self.pw_map[f'{user_name}__v1xrpaddress']
        most_recent_date = self.get_most_recent_update_date_for_table(table_name = db_table_ref, 
                                              date_column = date_column,user_name = user_name)
        output_json = pd.DataFrame({'user_address': user_address,
                                    'node_address': node_address,
                                    'user_name':user_name,
                                    'node_name':node_name,
                                   'task_id':task_id,
                                    'url': shortened_url,
                                   'date_column':date_column,
                                    'db_table_ref':db_table_ref,
                                    'table_most_recent_update':most_recent_date,
                                    'write_time':datetime.datetime.now()}, index=[0])
        return output_json

    def update_node_on_user_data_update(self,user_name = 'spm_typhus',
        node_name = 'agti_corp',
        task_id = '2024-05-28_23:54__FJ37',
        full_evidence_url = 'https://www.google.com',
        date_column = 'date_of_update',
        db_table_ref = 'sec__update_cik', transaction=True):
        """ example params:
        user_name = 'spm_typhus'
        node_name = 'agti_corp'
        task_id = '2024-05-28_23:54__FJ37'
        full_evidence_url = 'https://www.google.com'
        date_column = 'date_of_update'
        db_table_ref = 'sec__update_cik'
        """
        
        
        def create_update_string(df_to_write):
            """
            Create a single update string from the given DataFrame.
            
            Parameters:
            df_to_write (pd.DataFrame): DataFrame containing the necessary columns.
            
            Returns:
            str: A single update string.
            """
            row = df_to_write.iloc[0]
            update_string = f"DB_TABLE_REF '{row['db_table_ref']}' DATE_COLUMN '{row['date_column']}' = '{row['table_most_recent_update']}' WHERE 'url' = '{row['url']}'"
            return update_string
        
        ## Example usage
        
        #print(update_string)
        
        ## Pass the variables into the function
        df_to_write = self.construct_data_update_row(
            user_name=user_name,
            node_name=node_name,
            task_id=task_id,
            full_evidence_url=full_evidence_url,
            date_column=date_column,
            db_table_ref=db_table_ref
        )
        update_hash = ''
        if transaction==True:
            try:
                memo_content = create_update_string(df_to_write)
                memo_content = self.generic_pft_utilities.construct_basic_postfiat_memo(user=user_name,
                    task_id=task_id,
                    full_output=memo_content)
                user_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user_name)
                node_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=node_name)
                resp_string = self.generic_pft_utilities.send_PFT_with_info(sending_wallet=user_wallet,
                    amount=1,
                    memo=memo_content,
                    destination_address=node_wallet.classic_address,
                    url=None)
                update_hash = resp_string.result['hash']
            except:
                print("FAILED XRP UPDATE FOR UPDATE")
                pass
        df_to_write['tx_hash']= update_hash
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=user_name)
        df_to_write.to_sql(self.pw_map['node_name']+'__'+'data_update_details', dbconnx, 
                           if_exists='append', index=False)