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
import brotli
import base64
import hashlib
import time
import os

class GenericPFTUtilities:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.pft_issuer = 'rnQUEEg8yyjrwk9FhyXpKavHyCRJM9BDMW'
        self.mainnet_url= "https://s2.ripple.com:51234"
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

    def hex_to_text(self, hex_string):
        bytes_object = bytes.fromhex(hex_string)
        try:
            ascii_string = bytes_object.decode("utf-8")
            return ascii_string
        except UnicodeDecodeError:
            return bytes_object  # Return the raw bytes if it cannot decode as utf-8

    def output_post_fiat_holder_df(self):
        """ This function outputs a detail of all accounts holding PFT tokens
        with a float of their balances as pft_holdings. note this is from
        the view of the issuer account so balances appear negative so the pft_holdings 
        are reverse signed.
        """
        client = xrpl.clients.JsonRpcClient(self.mainnet_url)
        print("Getting all accounts holding PFT tokens...")
        response = client.request(xrpl.models.requests.AccountLines(
            account=self.pft_issuer,
            ledger_index="validated",
            peer=None,
            limit=None))
        full_post_fiat_holder_df = pd.DataFrame(response.result)
        for xfield in ['account','balance','currency','limit_peer']:
            full_post_fiat_holder_df[xfield] = full_post_fiat_holder_df['lines'].apply(lambda x: x[xfield])
        full_post_fiat_holder_df['pft_holdings']=full_post_fiat_holder_df['balance'].astype(float)*-1
        return full_post_fiat_holder_df
        
    def generate_random_utf8_friendly_hash(self, length=6):
        # Generate a random sequence of bytes
        random_bytes = os.urandom(16)  # 16 bytes of randomness
        
        # Create a SHA-256 hash of the random bytes
        hash_object = hashlib.sha256(random_bytes)
        hash_bytes = hash_object.digest()
        
        # Encode the hash to base64 to make it URL-safe and readable
        base64_hash = base64.urlsafe_b64encode(hash_bytes).decode('utf-8')
        
        # Take the first `length` characters of the base64-encoded hash
        utf8_friendly_hash = base64_hash[:length]
        
        return utf8_friendly_hash
    def get_number_of_bytes(self, text):
        text_bytes = text.encode('utf-8')
        return len(text_bytes)
        
    def split_text_into_chunks(self, text, max_chunk_size=760):
        chunks = []
        text_bytes = text.encode('utf-8')
        
        for i in range(0, len(text_bytes), max_chunk_size):
            chunk = text_bytes[i:i+max_chunk_size]
            chunk_number = i // max_chunk_size + 1
            chunk_label = f"chunk_{chunk_number}__".encode('utf-8')
            chunk_with_label = chunk_label + chunk
            chunks.append(chunk_with_label)
        
        return [chunk.decode('utf-8', errors='ignore') for chunk in chunks]

    def compress_string(self,input_string):
        # Compress the string using Brotli
        compressed_data = brotli.compress(input_string.encode('utf-8'))
        
        # Encode the compressed data to a Base64 string
        base64_encoded_data = base64.b64encode(compressed_data)
        
        # Convert the Base64 bytes to a string
        compressed_string = base64_encoded_data.decode('utf-8')
        
        return compressed_string

    def decompress_string(self, compressed_string):
        # Decode the Base64 string to bytes
        base64_decoded_data = base64.b64decode(compressed_string)
        
        # Decompress the data using Brotli
        decompressed_data = brotli.decompress(base64_decoded_data)
        
        # Convert the decompressed bytes to a string
        decompressed_string = decompressed_data.decode('utf-8')
        
        return decompressed_string

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
    
    def get_account_transactions__limited(self, account_address,
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
        


    def get_account_transactions(self, account_address='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n',
                                 ledger_index_min=-1,
                                 ledger_index_max=-1, limit=10):
        client = xrpl.clients.JsonRpcClient(self.mainnet_url)  # Using a public server; adjust as necessary
        all_transactions = []  # List to store all transactions
        marker = None  # Initialize marker to None
        previous_marker = None  # Track the previous marker
        max_iterations = 1000  # Safety limit for iterations
        iteration_count = 0  # Count iterations

        while max_iterations > 0:
            iteration_count += 1
            print(f"Iteration: {iteration_count}")
            print(f"Current Marker: {marker}")

            request = AccountTx(
                account=account_address,
                ledger_index_min=ledger_index_min,  # Use -1 for the earliest ledger index
                ledger_index_max=ledger_index_max,  # Use -1 for the latest ledger index
                limit=limit,                        # Adjust the limit as needed
                marker=marker,                      # Use marker for pagination
                forward=True                        # Set to True to return results in ascending order
            )

            response = client.request(request)
            transactions = response.result.get("transactions", [])
            print(f"Transactions fetched this batch: {len(transactions)}")
            all_transactions.extend(transactions)  # Add fetched transactions to the list

            if "marker" in response.result:  # Check if a marker is present for pagination
                if response.result["marker"] == previous_marker:
                    print("Pagination seems stuck, stopping the loop.")
                    break  # Break the loop if the marker does not change
                previous_marker = marker
                marker = response.result["marker"]  # Update marker for the next batch
                print("More transactions available. Fetching next batch...")
            else:
                print("No more transactions available.")
                break  # Exit loop if no more transactions

            max_iterations -= 1  # Decrement the iteration counter

        if max_iterations == 0:
            print("Reached the safety limit for iterations. Stopping the loop.")

        return all_transactions
    
    def get_account_transactions__exhaustive(self,account_address='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n',
                                ledger_index_min=-1,
                                ledger_index_max=-1,
                                max_attempts=3,
                                retry_delay=.2):

        client = xrpl.clients.JsonRpcClient(self.mainnet_url)  # Using a public server; adjust as necessary
        all_transactions = []  # List to store all transactions

        # Fetch transactions using marker pagination
        marker = None
        attempt = 0
        while attempt < max_attempts:
            try:
                request = xrpl.models.requests.account_tx.AccountTx(
                    account=account_address,
                    ledger_index_min=ledger_index_min,
                    ledger_index_max=ledger_index_max,
                    limit=1000,
                    marker=marker,
                    forward=True
                )
                response = client.request(request)
                transactions = response.result["transactions"]
                all_transactions.extend(transactions)

                if "marker" not in response.result:
                    break
                marker = response.result["marker"]

            except Exception as e:
                print(f"Error occurred while fetching transactions (attempt {attempt + 1}): {str(e)}")
                attempt += 1
                if attempt < max_attempts:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max attempts reached. Transactions may be incomplete.")
                    break

        return all_transactions

    

    def get_account_transactions__retry_version(self, account_address='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n',
                                ledger_index_min=-1,
                                ledger_index_max=-1,
                                max_attempts=3,
                                retry_delay=.2,
                                num_runs=5):
        
        longest_transactions = []
        
        for i in range(num_runs):
            print(f"Run {i+1}/{num_runs}")
            
            transactions = self.get_account_transactions__exhaustive(
                account_address=account_address,
                ledger_index_min=ledger_index_min,
                ledger_index_max=ledger_index_max,
                max_attempts=max_attempts,
                retry_delay=retry_delay
            )
            
            num_transactions = len(transactions)
            print(f"Number of transactions: {num_transactions}")
            
            if num_transactions > len(longest_transactions):
                longest_transactions = transactions
            
            if i < num_runs - 1:
                print(f"Waiting for {retry_delay} seconds before the next run...")
                time.sleep(retry_delay)
        
        print(f"Longest list of transactions: {len(longest_transactions)} transactions")
        return longest_transactions
    
    def output_post_fiat_holder_df(self):
        """ This function outputs a detail of all accounts holding PFT tokens
        with a float of their balances as pft_holdings. note this is from
        the view of the issuer account so balances appear negative so the pft_holdings 
        are reverse signed.
        """
        client = xrpl.clients.JsonRpcClient(self.mainnet_url)
        print("Getting all accounts holding PFT tokens...")
        response = client.request(xrpl.models.requests.AccountLines(
            account=self.pft_issuer,
            ledger_index="validated",
            peer=None,
            limit=None))
        full_post_fiat_holder_df = pd.DataFrame(response.result)
        for xfield in ['account','balance','currency','limit_peer']:
            full_post_fiat_holder_df[xfield] = full_post_fiat_holder_df['lines'].apply(lambda x: x[xfield])
        full_post_fiat_holder_df['pft_holdings']=full_post_fiat_holder_df['balance'].astype(float)*-1
        return full_post_fiat_holder_df
    

    def get_memo_detail_df_for_account(self,account_address,transaction_limit=5000, pft_only=True, exhaustive=False):
        """ This function gets all the memo details for a given account """
        if exhaustive ==False:
            full_transaction_history = self.get_account_transactions(account_address=account_address, 
                                                                    limit=transaction_limit)
        if exhaustive == True:
            full_transaction_history = self.get_account_transactions__retry_version(account_address=account_address, 
                                                                                    ledger_index_min=-1,
                                                                                    ledger_index_max=-1,
                                                                                    max_attempts=3,
                                                                                    retry_delay=.2,
                                                                                    num_runs=5)


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
        live_memo_tx['reference_account']=account_address
       
        return live_memo_tx
    
    def convert_memo_detail_df_into_essential_caching_details(self, memo_details_df):
        """ 
        Takes a memo detail df and converts it into a raw detail df to be cached to a local db
        """
        full_memo_detail = memo_details_df
        full_memo_detail['pft_absolute_amount']=full_memo_detail['tx'].apply(lambda x: x['Amount']['value']).astype(float)
        full_memo_detail['memo_format']=full_memo_detail['converted_memos'].apply(lambda x: x['MemoFormat'])
        full_memo_detail['memo_type']= full_memo_detail['converted_memos'].apply(lambda x: x['MemoType'])
        full_memo_detail['memo_data']=full_memo_detail['converted_memos'].apply(lambda x: x['MemoData'])
        full_memo_detail['pft_sign']= np.where(full_memo_detail['message_type'] =='INCOMING',1,-1)
        full_memo_detail['directional_pft'] = full_memo_detail['pft_sign']*full_memo_detail['pft_absolute_amount']
        raw_detail_df = full_memo_detail[['hash','account','destination','message_type',
                        'user_account','datetime','pft_absolute_amount','memo_format',
                        'memo_type','memo_data','pft_sign','directional_pft','reference_account']].copy()
        raw_detail_df['unique_key']=raw_detail_df['reference_account']+'__'+raw_detail_df['hash']
        return raw_detail_df
    
    def send_PFT_chunk_message(self,user_name,full_text, destination_address):
        """
        This takes a large message compresses the strings and sends it in hex to another address.
        Is based on a user spawned wallet and sends 1 PFT per chunk
        user_name = 'spm_typhus',full_text = big_string, destination_address='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN'"""     
        
        wallet = self.spawn_user_wallet_based_on_name(user_name)
        task_id = 'chunkm__'+self.generate_random_utf8_friendly_hash(6)
        
        all_chunks = self.split_text_into_chunks(full_text)
        send_memo_map = {}
        for xchunk in all_chunks:
            chunk_num = int(xchunk.split('chunk_')[1].split('__')[0])
            send_memo_map[chunk_num] = self.construct_basic_postfiat_memo(user=user_name, task_id=task_id, 
                                        full_output=self.compress_string(xchunk))
        yarr=[]
        for xkey in send_memo_map.keys():
            xresp = self.send_PFT_with_info(sending_wallet=wallet, amount=1, memo=send_memo_map[xkey], 
                                destination_address=destination_address, url=None)
            yarr.append(xresp)
        final_response = yarr[-1] if yarr else None
        return final_response
            
    def get_all_account_chunk_messages(self,account_id='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN'):
        """ This pulls in all the chunk messages an account has received and cleans and aggregates
        the messages for easy digestion - implementing sorts, and displays some information associated with the messages """ 
        all_account_memos = self.get_memo_detail_df_for_account(account_id, pft_only=True)
        all_chunk_messages = all_account_memos[all_account_memos['converted_memos'].apply(lambda x: 
                                                                        'chunkm__' in x['MemoType'])].copy()
        all_chunk_messages['memo_data_raw']= all_chunk_messages['converted_memos'].apply(lambda x: x['MemoData']).astype(str)
        all_chunk_messages['message_id']=all_chunk_messages['converted_memos'].apply(lambda x: x['MemoType'])
        all_chunk_messages['decompressed_strings']=all_chunk_messages['memo_data_raw'].apply(lambda x: self.decompress_string(x))
        all_chunk_messages['chunk_num']=all_chunk_messages['decompressed_strings'].apply(lambda x: x.split('chunk_')[1].split('__')[0]).astype(int)
        all_chunk_messages.sort_values(['message_id','chunk_num'], inplace=True)
        grouped_memo_data = all_chunk_messages[['decompressed_strings','message_id']].groupby('message_id').sum().copy()
        def remove_chunks(text):
            # Use regular expression to remove all occurrences of chunk_1__, chunk_2__, etc.
            cleaned_text = re.sub(r'chunk_\d+__', '', text)
            return cleaned_text
        grouped_memo_data['cleaned_message']=grouped_memo_data['decompressed_strings'].apply(lambda x: remove_chunks(x))
        all_chunk_messages['PFT_value']=all_chunk_messages['tx'].apply(lambda x: x['Amount']['value']).astype(float)
        grouped_pft_value = all_chunk_messages[['message_id','PFT_value']].groupby('message_id').sum()['PFT_value']
        grouped_memo_data['PFT']=grouped_pft_value
        last_slice = all_chunk_messages.groupby('message_id').last().copy()
        
        grouped_memo_data['datetime']=last_slice['datetime']
        grouped_memo_data['hash']=last_slice['hash']
        grouped_memo_data['message_type']= last_slice['message_type']
        grouped_memo_data['destination']= last_slice['destination']
        grouped_memo_data['account']= last_slice['account']
        return grouped_memo_data


    def process_memo_detail_df_to_daily_summary_df(self, memo_detail_df):
        """_summary_
        
        Example Code to feed this 
        all_memo_detail = self.get_memo_detail_df_for_account(account_address='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n', pft_only=True)
        """
        all_memo_detail = memo_detail_df
        ## THIS EXCLUDES CHUNK MESSAGES FROM THE DAILY SUMMARY
        ### I THINK THIS IS LOGICAL BC CHUNK MESSAGES ARE INHERENTLY DUPLICATED
        all_memo_detail = all_memo_detail[all_memo_detail['converted_memos'].apply(lambda x: 'chunkm__' not in str(x))].copy()
        all_memo_detail['pft_absolute_value']=all_memo_detail['tx'].apply(lambda x: x['Amount']['value']).astype(float)
        all_memo_detail['incoming_sign']=np.where(all_memo_detail['message_type']=='INCOMING',1,-1)
        all_memo_detail['pft_directional_value'] = all_memo_detail['incoming_sign'] * all_memo_detail['pft_absolute_value']
        all_memo_detail['pft_transaction']=np.where(all_memo_detail['pft_absolute_value']>0,1,np.nan)
        
        
        all_memo_detail['combined_memo_type_and_data']= all_memo_detail['converted_memos'].apply(lambda x: x['MemoType']+'  '+x['MemoData'])
        output_frame = all_memo_detail[['datetime','pft_transaction','pft_directional_value',
                                        'combined_memo_type_and_data','pft_absolute_value']].groupby('datetime').first()
        output_frame.reset_index(inplace=True)
        output_frame['raw_date']=pd.to_datetime(output_frame['datetime'].apply(lambda x: x.date()))
        daily_grouped_output_frame = output_frame[['pft_transaction','pft_directional_value',
                    'combined_memo_type_and_data','pft_absolute_value','raw_date']].groupby('raw_date').sum()
        return {'daily_grouped_summary':daily_grouped_output_frame, 'raw_summary':output_frame}
    
    def get_most_recent_google_doc_for_user(self, account_memo_detail_df, address):
        """ This function takes a memo detail df and a classic address and outputs
        the associated google doc
        
        EXAMPLE:
        address = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'
        all_account_info = self.get_memo_detail_df_for_account(account_address='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n',transaction_limit=5000,
            pft_only=True) 
        """ 
        
        op = ''
        try:
            op=list(account_memo_detail_df[(account_memo_detail_df['converted_memos'].apply(lambda x: 'google_doc' in str(x))) & 
                    (account_memo_detail_df['account']==address)]['converted_memos'].tail(1))[0]['MemoData']
        except:
            print('No Google Doc Associated with Address')
            pass
        return op
    
    def determine_if_map_is_task_id(self,memo_dict):
        """ task ID detection 
        """
        full_memo_string = str(memo_dict)
        task_id_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}(?:__[A-Z0-9]{4})?)')
        has_task_id = False
        if re.search(task_id_pattern, full_memo_string):
            return True
        
        if has_task_id:
            return True
        return False
    
    def convert_all_account_info_into_simplified_task_frame(self, account_memo_detail_df):
        """ This takes all the Post Fiat Tasks and outputs them into a simplified
        dataframe of task information with embedded classifications 
        
        Runs on all_account_info generated by
        all_account_info =self.get_memo_detail_df_for_account(account_address=self.user_wallet.classic_address,
            transaction_limit=5000)
        
        """ 
        all_account_info = account_memo_detail_df
        #all_account_info['datetime']= all_account_info['tx'].apply(lambda x: self.convert_ripple_timestamp_to_datetime(x['date']))
        simplified_task_frame = all_account_info[all_account_info['converted_memos'].apply(lambda x: 
                                                                self.determine_if_map_is_task_id(x))].copy()
        simplified_task_frame = simplified_task_frame[simplified_task_frame['tx'].apply(lambda 
                                                                                        x: x['Amount']).apply(lambda x: 
                                                                                                                    "'currency': 'PFT'" in str(x))].copy()
        def add_field_to_map(xmap, field, field_value):
            xmap[field] = field_value
            return xmap

        simplified_task_frame['pft_abs']= simplified_task_frame['tx'].apply(lambda x: x['Amount']['value']).astype(float)
        simplified_task_frame['directional_pft']=simplified_task_frame['message_type'].map({'INCOMING':1,
            'OUTGOING':-1}) * simplified_task_frame['pft_abs']

        for xfield in ['hash','datetime']:
            simplified_task_frame['converted_memos'] = simplified_task_frame.apply(lambda x: add_field_to_map(x['converted_memos'],
                xfield,x[xfield]),1)
            
        core_task_df = pd.DataFrame(list(simplified_task_frame['converted_memos'])).copy()

        core_task_df['task_type']=core_task_df['MemoData'].apply(lambda x: self.classify_task_string(x))

        return core_task_df
    
    def convert_all_account_info_into_outstanding_task_df(self, account_memo_detail_df):
        """ This reduces all account info into a simplified dataframe of proposed 
        and accepted tasks """ 
        all_account_info = account_memo_detail_df
        task_frame = self.convert_all_account_info_into_simplified_task_frame(account_memo_detail_df=account_memo_detail_df)
        task_frame['task_id']=task_frame['MemoType']
        task_frame['full_output']=task_frame['MemoData']
        task_frame['user_account']=task_frame['MemoFormat']
        task_type_map = task_frame.groupby('task_id').last()[['task_type']].copy()
        task_id_to_proposal = task_frame[task_frame['task_type']
        =='PROPOSAL'].groupby('task_id').first()['full_output']
        
        task_id_to_acceptance = task_frame[task_frame['task_type']
        =='ACCEPTANCE'].groupby('task_id').first()['full_output']
        acceptance_frame = pd.concat([task_id_to_proposal,task_id_to_acceptance],axis=1)
        acceptance_frame.columns=['proposal','acceptance_raw']
        acceptance_frame['acceptance']=acceptance_frame['acceptance_raw'].apply(lambda x: str(x).replace('ACCEPTANCE REASON ___ ',
                                                                                                         '').replace('nan',''))
        acceptance_frame['proposal']=acceptance_frame['proposal'].apply(lambda x: str(x).replace('PROPOSED PF ___ ',
                                                                                                         '').replace('nan',''))
        raw_proposals_and_acceptances = acceptance_frame[['proposal','acceptance']].copy()
        proposed_or_accepted_only = list(task_type_map[(task_type_map['task_type']=='ACCEPTANCE')|
        (task_type_map['task_type']=='PROPOSAL')].index)
        op= raw_proposals_and_acceptances[raw_proposals_and_acceptances.index.get_level_values(0).isin(proposed_or_accepted_only)]
        return op
    
    def convert_memo_detail_df_into_essential_caching_details(self, memo_details_df):
        full_memo_detail = memo_details_df
        full_memo_detail['pft_absolute_amount']=full_memo_detail['tx'].apply(lambda x: x['Amount']['value']).astype(float)
        full_memo_detail['memo_format']=full_memo_detail['converted_memos'].apply(lambda x: x['MemoFormat'])
        full_memo_detail['memo_type']= full_memo_detail['converted_memos'].apply(lambda x: x['MemoType'])
        full_memo_detail['memo_data']=full_memo_detail['converted_memos'].apply(lambda x: x['MemoData'])
        full_memo_detail['pft_sign']= np.where(full_memo_detail['message_type'] =='INCOMING',1,-1)
        full_memo_detail['directional_pft'] = full_memo_detail['pft_sign']*full_memo_detail['pft_absolute_amount']
        # full_memo_detail['reference_account']=account_address
        raw_detail_df = full_memo_detail[['hash','account','destination','message_type',
                        'user_account','datetime','pft_absolute_amount','memo_format',
                        'memo_type','memo_data','pft_sign','directional_pft','reference_account']].copy()
        raw_detail_df['unique_key']=raw_detail_df['reference_account']+'__'+raw_detail_df['hash']
        return raw_detail_df