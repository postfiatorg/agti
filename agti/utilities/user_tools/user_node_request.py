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
    
    