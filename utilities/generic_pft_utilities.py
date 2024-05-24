import xrpl
import binascii
import datetime 
import random
from xrpl.models.transactions import Payment, Memo
from xrpl.models.requests import AccountTx
from xrpl.models.transactions import Payment, Memo
import time
import string
import asyncio
import nest_asyncio
import pandas as pd
import numpy as np
import binascii
import re
import string
nest_asyncio.apply()
import requests
import zlib
import base64


class GenericPFTUtilities:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.pft_issuer = 'rnQUEEg8yyjrwk9FhyXpKavHyCRJM9BDMW'
        self.mainnet_url= "https://xrplcluster.com"
        self.mainnet_urls = [
            "https://xrplcluster.com",
            "https://xrpl.ws",
            "https://s1.ripple.com:51234",
            "https://s2.ripple.com:51234"
        ]
        self.testnet_urls = [
            "https://s.altnet.rippletest.net:51234",
            "https://testnet.xrpl-labs.com",
            "https://clio.altnet.rippletest.net:51234"
        ]
        self.devnet_urls = [
            "https://s.devnet.rippletest.net:51234",
            "https://clio.devnet.rippletest.net:51234",
            "https://sidechain-net2.devnet.rippletest.net:51234"
        ]
    def to_hex(self,string):
        return binascii.hexlify(string.encode()).decode()
    def convert_ripple_timestamp_to_datetime(self, ripple_timestamp = 768602652):
        ripple_epoch_offset = 946684800
        unix_timestamp = ripple_timestamp + ripple_epoch_offset
        date_object = datetime.datetime.fromtimestamp(unix_timestamp)
        return date_object
    def hex_to_text(self,hex_string):
        bytes_object = bytes.fromhex(hex_string)
        ascii_string = bytes_object.decode("utf-8")
        return ascii_string
    

    def shorten_url(self,url):
        api_url = "http://tinyurl.com/api-create.php"
        params = {'url': url}
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            return response.text
        else:
            return None
    
    def check_if_tx_pft(self,tx):
        ret= False
        try:
            if tx['Amount']['currency'] == "PFT":
                ret = True
        except:
            pass
        return ret
    
    def convert_memo_dict__generic(self, memo_dict):
        """Constructs a memo object with user, task_id, and full_output from hex-encoded values."""
        MemoFormat= ''
        MemoType=''
        MemoData=''
        try:
            MemoFormat = self.hex_to_text(memo_dict['MemoFormat'])
        except:
            pass
        try:
            MemoType = self.hex_to_text(memo_dict['MemoType'])
        except:
            pass
        try:
            MemoData = self.hex_to_text(memo_dict['MemoData'])
        except:
            pass
        
        return {
            'MemoFormat': MemoFormat,
            'MemoType': MemoType,
            'MemoData': MemoData
        }
    
    def classify_task_string(self,string):
        """ These are the canonical classifications for task strings 
        on a Post Fiat Node
        """ 
        categories = {
                'ACCEPTANCE': ['ACCEPTANCE REASON ___'],
                'PROPOSAL': [' .. ','PROPOSED PF ___'],
                'REFUSAL': ['REFUSAL REASON ___'],
                'VERIFICATION_PROMPT': ['VERIFICATION PROMPT ___'],
                'VERIFICATION_RESPONSE': ['VERIFICATION RESPONSE ___'],
                'REWARD': ['REWARD RESPONSE __'],
                'TASK_OUTPUT': ['COMPLETION JUSTIFICATION ___'],
                'USER_GENESIS': ['USER GENESIS __'],
                'REQUEST_POST_FIAT':['REQUEST_POST_FIAT ___'],
                'NODE_REQUEST': ['NODE REQUEST ___'],
            }
    
        for category, keywords in categories.items():
            if any(keyword in string for keyword in keywords):
                return category
    
        return 'UNKNOWN'
    
    def generate_custom_id(self):
        """ These are the custom IDs generated for each task that is generated
        in a Post Fiat Node """ 
        letters = ''.join(random.choices(string.ascii_uppercase, k=2))
        numbers = ''.join(random.choices(string.digits, k=2))
        second_part = letters + numbers
        date_string = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        output= date_string+'__'+second_part
        output = output.replace(' ',"_")
        return output
    
    def construct_basic_postfiat_memo(self, user, task_id, full_output):
        user_hex = self.to_hex(user)
        task_id_hex = self.to_hex(task_id)
        full_output_hex = self.to_hex(full_output)
        memo = Memo(
        memo_data=full_output_hex,
        memo_type=task_id_hex,
        memo_format=user_hex)  
        return memo
       
    def send_PFT_with_info(self, sending_wallet, amount, memo, destination_address, url=None):
        """ This sends PFT tokens to a destination address with memo information
        memo should be 1kb or less in size and needs to be in hex format
        """
        if url is None:
            url = self.mainnet_url

        client = xrpl.clients.JsonRpcClient(url)
        amount_to_send = xrpl.models.amounts.IssuedCurrencyAmount(
            currency="PFT",
            issuer=self.pft_issuer,
            value=str(amount)
        )
        payment = xrpl.models.transactions.Payment(
            account=sending_wallet.address,
            amount=amount_to_send,
            destination=destination_address,
            memos=[memo]
        )
        response = xrpl.transaction.submit_and_wait(payment, client, sending_wallet)

        return response

    def spawn_user_wallet_based_on_name(self,user_name):
        """ outputs user wallet initialized from password map""" 
        user_seed= self.pw_map[f'{user_name}__v1xrpsecret']
        wallet = xrpl.wallet.Wallet.from_seed(user_seed)
        print(f'User wallet for {user_name} is {wallet.address}')
        return wallet
    
    def test_url_reliability(self, user_wallet, destination_address):
        """_summary_
        
        EXAMPLE
        user_wallet = self.spawn_user_wallet_based_on_name(user_name='goodalexander')
        url_reliability_df = self.test_url_reliability(user_wallet=user_wallet,destination_address='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN')
        """
        results = []

        for url in self.mainnet_urls:
            for i in range(7):
                memo = self.construct_basic_postfiat_memo(
                    user='test_tx', 
                    task_id=f'999_{i}', 
                    full_output=f'NETWORK FUNCTION __ {url}'
                )
                start_time = time.time()
                try:
                    self.send_PFT_with_info(
                        sending_wallet=user_wallet, 
                        amount=1, 
                        memo=memo, 
                        destination_address=destination_address, 
                        url=url
                    )
                    success = True
                except Exception as e:
                    success = False
                    print(f"Error: {e}")
                end_time = time.time()
                elapsed_time = end_time - start_time
                results.append({
                    'URL': url,
                    'Test Number': i + 1,
                    'Elapsed Time (s)': elapsed_time,
                    'Success': success
                })

        df = pd.DataFrame(results)
        return df
    
    def get_account_transactions(self, account_address,
                                    ledger_index_min=-1,
                                    ledger_index_max=-1, limit=10):
            client = xrpl.clients.JsonRpcClient(self.mainnet_url) # Using a public server; adjust as necessary
        
            request = AccountTx(
                account=account_address,
                ledger_index_min=ledger_index_min,  # Use -1 for the earliest ledger index
                ledger_index_max=ledger_index_max,  # Use -1 for the latest ledger index
                limit=limit,                        # Adjust the limit as needed
                forward=True                        # Set to True to return results in ascending order
            )
        
            response = client.request(request)
            transactions = response.result.get("transactions", [])
        
            if "marker" in response.result:  # Check if a marker is present for pagination
                print("More transactions available. Marker for next batch:", response.result["marker"])
        
            return transactions
    

    def get_memo_detail_df_for_account(self,account_address,transaction_limit=5000, pft_only=True):
        """ This function gets all the memo details for a given account """
        
        full_transaction_history = self.get_account_transactions(account_address=account_address, 
                                                                 limit=transaction_limit)


        validated_tx = pd.DataFrame(full_transaction_history)
        validated_tx['has_memos']=validated_tx['tx'].apply(lambda x: 'Memos' in x.keys())
        live_memo_tx = validated_tx[validated_tx['has_memos']== True].copy()
        live_memo_tx['main_memo_data']=live_memo_tx['tx'].apply(lambda x: x['Memos'][0]['Memo'])
        live_memo_tx['converted_memos']=live_memo_tx['main_memo_data'].apply(lambda x: 
                                                                             self.convert_memo_dict__generic(x))
        live_memo_tx['hash']=live_memo_tx['tx'].apply(lambda x: x['hash'])
        live_memo_tx['account']= live_memo_tx['tx'].apply(lambda x: x['Account'])
        live_memo_tx['destination']=live_memo_tx['tx'].apply(lambda x: x['Destination'])
        
        live_memo_tx['message_type']=np.where(live_memo_tx['destination']==account_address, 'INCOMING','OUTGOING')
        live_memo_tx['user_account']= live_memo_tx[['destination','account']].sum(1).apply(lambda x: 
                                                         str(x).replace(account_address,''))
        live_memo_tx['datetime']= live_memo_tx['tx'].apply(lambda x: self.convert_ripple_timestamp_to_datetime(x['date']))
        if pft_only == True:
            live_memo_tx= live_memo_tx[live_memo_tx['tx'].apply(lambda x: self.pft_issuer in str(x))].copy()
        return live_memo_tx
    