import xrpl
import binascii
import datetime 
import random
from xrpl.models.transactions import Payment, Memo
from agti.utilities.generic_pft_utilities import GenericPFTUtilities

### This is a tool for a User wallet to initiate a task request to a node wallet
class UserNodeTaskRequest:
    def __init__(self,pw_map,user_name):
        self.pw_map= pw_map
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        self.user_wallet = xrpl.wallet.Wallet.from_seed(self.pw_map[f'{user_name}__v1xrpsecret']) 
        self.user_name = user_name
        node_name = self.pw_map['node_name']
        self.node_address = self.pw_map[f'{node_name}__v1xrpaddress']
    def create_node_request_memo(self, request_string):
        full_output ='NODE REQUEST ___ '+request_string
        memo = self.generic_pft_utilities.construct_basic_postfiat_memo(user=self.user_name, 
                                                                 task_id=self.generic_pft_utilities.generate_custom_id(), 
                                                                 full_output=full_output)
        return memo
    def send_node_request(self,amount,destination_address,request_string):
        memo = self.create_node_request_memo(request_string)
        response = self.generic_pft_utilities.send_PFT_with_info(sending_wallet=self.user_wallet, 
                                                                 amount=amount, 
                                                                 memo=memo,
                                                                 destination_address=destination_address)
        return response
    
    def output_outstanding_node_requests(self,full_memo_detail_df):
        """ 
        
        all_node_transactions = self.generic_pft_utilities.get_memo_detail_df_for_account('rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN',pft_only=True,
        exhaustive=True)
        """
        all_node_transactions =full_memo_detail_df
        outsanding_node_memos = all_node_transactions[all_node_transactions['converted_memos'].apply(lambda x: 
                                                                         'NODE REQUEST ___' in str(x))].copy()
        outsanding_node_memos['pft_bounty']=outsanding_node_memos['tx'].apply(lambda x: x['Amount']['value'])
        outsanding_node_memos= outsanding_node_memos[outsanding_node_memos['message_type']=='INCOMING'].copy()
        outsanding_node_memos['user']=outsanding_node_memos['converted_memos'].apply(lambda x: x['MemoFormat'])
        outsanding_node_memos['task_id']=outsanding_node_memos['converted_memos'].apply(lambda x: x['MemoType'])
        outsanding_node_memos['memo']=outsanding_node_memos['converted_memos'].apply(lambda x: x['MemoData'].replace('NODE REQUEST ___','').strip())
        fin_output = outsanding_node_memos[['user','task_id','memo','pft_bounty']]
        return fin_output
        