from agti.utilities.node_tools.agency_coach import AgencyCoach__v2
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
import xrpl
import datetime
from xrpl.wallet import Wallet
from xrpl.models.requests import AccountTx
from xrpl.models.transactions import Payment, Memo
#from pftpyclient.basic_utilities.settings import *
import asyncio
import nest_asyncio
import pandas as pd
import numpy as np
import requests 
import binascii
import re
import random 
import string
import re
class SimpleUserTaskManager:
    def __init__(self, pw_map):
        self.pw_map= pw_map
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        self.user_name_mapping = self.generate_user_name_mapping()
    def to_hex(self,string):
        return binascii.hexlify(string.encode()).decode()


    def convert_memo_dict(self, memo_dict):
        """Constructs a memo object with user, task_id, and full_output from hex-encoded values."""
        user= ''
        task_id=''
        full_output=''
        try:
            user = self.hex_to_text(memo_dict['MemoFormat'])
        except:
            pass
        try:
            task_id = self.hex_to_text(memo_dict['MemoType'])
        except:
            pass
        try:
            full_output = self.hex_to_text(memo_dict['MemoData'])
        except:
            pass
        
        return {
            'user': user,
            'task_id': task_id,
            'full_output': full_output
        }
    def determine_if_map_is_task_id(self,memo_dict):
        """ Note that technically only the task ID recognition is needed
        at a later date might want to implement forced user and output delineators 
        if someone spams the system with task IDs
        """
        full_memo_string = str(memo_dict)
        task_id_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}(?:__[A-Z0-9]{4})?)')
        has_task_id = False
        if re.search(task_id_pattern, full_memo_string):
            return True
        has_user_identified = 'user:' in full_memo_string
        has_full_output_identified = 'full_output:' in full_memo_string
        if (has_user_identified) and (has_full_output_identified) and has_task_id:
            return True
        return False

    def construct_basic_postfiat_memo(self, user, task_id, full_output):
        user_hex = self.to_hex(user)
        task_id_hex = self.to_hex(task_id)
        full_output_hex = self.to_hex(full_output)
        memo = Memo(
        memo_data=full_output_hex,
        memo_type=task_id_hex,
        memo_format=user_hex)  
        return memo
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
                'REQUEST_POST_FIAT ':['REQUEST_POST_FIAT ___']
            }
    
        for category, keywords in categories.items():
            if any(keyword in string for keyword in keywords):
                return category
    
        return 'UNKNOWN'

    def convert_all_account_info_into_simplified_task_frame(self, all_account_info,account_address):
        
        #all_account_info = all_account_details
        
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
        simplified_task_frame['node_account']= simplified_task_frame['user_account']
        
        for xfield in ['hash','node_account','datetime']:
            simplified_task_frame['converted_memos'] = simplified_task_frame.apply(lambda x: add_field_to_map(x['converted_memos'],
                xfield,x[xfield]),1)
            
        core_task_df = pd.DataFrame(list(simplified_task_frame['converted_memos'])).copy()
        core_task_df['task_type']=core_task_df['MemoData'].apply(lambda x: self.classify_task_string(x))
        core_task_df['task_id']=core_task_df['MemoType']
        core_task_df['user']=core_task_df['MemoFormat']
        return core_task_df

    def send_acceptance_for_task_id(self, task_id, acceptance_string, all_account_info,account_address):
        """ 
        This function accepts a task. The function will not work 

        EXAMPLE PARAMETERS
        task_id='2024-05-14_19:10__ME26'
        acceptance_string = 'I agree and accept 2024-05-14_19:10__ME26 - want to finalize reward testing'
        all_account_info =self.get_memo_detail_df_for_account(account_address=self.user_wallet.classic_address,
                transaction_limit=5000)
        """
        all_account_info = all_account_info
        simplified_task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=all_account_info,account_address=account_address)
        all_task_types = simplified_task_frame[simplified_task_frame['task_id']
         == task_id]['task_type'].unique()
        
        if (('REFUSAL' in all_task_types) 
        | ('ACCEPTANCE' in all_task_types)
       | ('VERIFICATION_RESPONSE' in all_task_types)
       | ('USER_GENESIS' in all_task_types)
       | ('REWARD' in all_task_types)):
            print('task is not valid for acceptance. Its statuses include')
            print(all_task_types)
            
        if (('REFUSAL' not in all_task_types) 
        & ('ACCEPTANCE' not in all_task_types)
       & ('VERIFICATION_RESPONSE' not in all_task_types)
       & ('USER_GENESIS' not in all_task_types)
       & ('REWARD' not in all_task_types)):
            print('Proceeding to accept task')
            node_account = list(simplified_task_frame[simplified_task_frame['task_id']==task_id].tail(1)['node_account'])[0]
            user = list(simplified_task_frame[simplified_task_frame['task_id'] == task_id].tail(1)['user'])[0]
            if 'ACCEPTANCE REASON ___' not in acceptance_string:
                acceptance_string='ACCEPTANCE REASON ___ '+acceptance_string
            constructed_memo = self.construct_basic_postfiat_memo(user=user, 
                                                       task_id=task_id, full_output=acceptance_string)
            sending_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user)
            response = self.generic_pft_utilities.send_PFT_with_info(amount=1, memo=constructed_memo, 
                destination_address=node_account, sending_wallet=sending_wallet)
            account = response.result['Account']
            destination = response.result['Destination']
            memo_map = response.result['Memos'][0]['Memo']
            #memo_map.keys()
            print(f"{account} sent 1 PFT to {destination} with memo")
            print(self.convert_memo_dict(memo_map))
        return response

    def send_refusal_for_task(self, task_id, refusal_reason, all_account_info, account_address):
        """ 
        This function refuses a task. The function will not work if the task has already 
        been accepted, refused, or completed. 

        EXAMPLE PARAMETERS
        task_id='2024-05-14_19:10__ME26'
        refusal_reason = 'I cannot accept this task because ...'
        all_account_info =self.get_memo_detail_df_for_account(account_address=self.user_wallet.classic_address,
                transaction_limit=5000)
        """
        all_account_info = all_account_info
        simplified_task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=all_account_info, account_address=account_address)
        task_statuses = simplified_task_frame[simplified_task_frame['task_id'] 
        == task_id]['task_type'].unique()

        if any(status in task_statuses for status in ['REFUSAL', 'ACCEPTANCE', 
            'VERIFICATION_RESPONSE', 'USER_GENESIS', 'REWARD']):
            print('Task is not valid for refusal. Its statuses include:')
            print(task_statuses)
            return

        if 'PROPOSAL' not in task_statuses:
            print('Task must have a proposal to be refused. Current statuses include:')
            print(task_statuses)
            return

        print('Proceeding to refuse task')
        node_account = list(simplified_task_frame[simplified_task_frame['task_id'] 
            == task_id].tail(1)['node_account'])[0]
        user = list(simplified_task_frame[simplified_task_frame['task_id'] 
            == task_id].tail(1)['user'])[0]
        if 'REFUSAL REASON ___' not in refusal_reason:
            refusal_reason = 'REFUSAL REASON ___ ' + refusal_reason
        constructed_memo = self.construct_basic_postfiat_memo(user=user, 
                                                               task_id=task_id, full_output=refusal_reason)
        sending_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user)
        response = self.generic_pft_utilities.send_PFT_with_info(amount=1, memo=constructed_memo, 
                                                                 destination_address=node_account, sending_wallet= sending_wallet)
        account = response.result['Account']
        destination = response.result['Destination']
        memo_map = response.result['Memos'][0]['Memo']
        print(f"{account} sent 1 PFT to {destination} with memo")
        print(self.convert_memo_dict(memo_map))
        return response

    def request_post_fiat(self, request_message, 
                          all_account_info,account_address):
        """ 
        This requests a task known as a Post Fiat from the default node you are on
        
        request_message = 'I would like a new task related to the creation of my public facing wallet', 
        all_account_info=all_account_info

        """
        
        simplified_task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=
            all_account_info,account_address=account_address)
        
        # Ensure the message has the correct prefix
        if 'REQUEST_POST_FIAT ___' not in request_message:
            request_message = 'REQUEST_POST_FIAT ___ ' + request_message
        
        # Generate a custom task ID for this request
        task_id = self.generic_pft_utilities.generate_custom_id()
        user =list(simplified_task_frame.tail(1)['user'])[0]
        node_account = list(simplified_task_frame.tail(1)['node_account'])[0]
        # Construct the memo with the request message
        constructed_memo = self.construct_basic_postfiat_memo(user=user, 
                                                               task_id=task_id, full_output=request_message)
        # Send the memo to the default node
        sending_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user)
        response = self.generic_pft_utilities.send_PFT_with_info(amount=1, memo=constructed_memo, 
                                                                 destination_address=node_account, sending_wallet= sending_wallet)
        account = response.result['Account']
        destination = response.result['Destination']
        memo_map = response.result['Memos'][0]['Memo']
        print(f"{account} sent 1 PFT to {destination} with memo")
        print(self.convert_memo_dict(memo_map))
        return response

    def send_post_fiat_initial_completion(self, completion_string, task_id, all_account_info,account_address):
        """
        This function sends an initial completion for a given task back to a node.
        The most recent task status must be 'ACCEPTANCE' to trigger the initial completion.
        
        EXAMPLE PARAMETERS
        completion_string = 'I have completed the task as requested'
        task_id = '2024-05-14_19:10__ME26'
        all_account_info = self.get_memo_detail_df_for_account(account_address=self.user_wallet.classic_address,
                                                                transaction_limit=5000)
        """
        all_account_info = all_account_info

        simplified_task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=all_account_info, account_address=account_address)
        matching_task = simplified_task_frame[simplified_task_frame['task_id'] == task_id]#
        
        if matching_task.empty:
            print(f"No task found with task ID: {task_id}")
            return
        
        most_recent_status = matching_task.sort_values(by='datetime').iloc[-1]['task_type']
        
        if most_recent_status != 'ACCEPTANCE':
            print(f"The most recent status for task ID {task_id} is not 'ACCEPTANCE'. Current status: {most_recent_status}")
            return
        user_name = list(simplified_task_frame['user'])[0]
        node_account = list(simplified_task_frame[simplified_task_frame['task_id'] 
            == task_id].tail(1)['node_account'])[0]
        user= user_name
        source_of_command = matching_task.iloc[0]['node_account']
        acceptance_string = 'COMPLETION JUSTIFICATION ___ ' + completion_string
        constructed_memo = self.construct_basic_postfiat_memo(user=user_name, 
                                                              task_id=task_id, 
                                                              full_output=acceptance_string)
        print(acceptance_string)
        print('converted to memo')
        sending_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user)
        response = self.generic_pft_utilities.send_PFT_with_info(amount=1, memo=constructed_memo, 
                                                                 destination_address=node_account, sending_wallet= sending_wallet)
        
        account = response.result['Account']
        destination = response.result['Destination']
        memo_map = response.result['Memos'][0]['Memo']
        print(f"{account} sent 1 PFT to {destination} with memo")
        print(self.convert_memo_dict(memo_map))
        return response

    def convert_all_account_info_into_required_verification_df(self,all_account_info, account_address):
        """ 
        This function pulls in all account info and converts it into a list

        all_account_info = self.get_memo_detail_df_for_account(account_address=self.user_wallet.classic_address,
                                                                transaction_limit=5000)

        """ 
        simplified_task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=all_account_info, account_address=account_address)
        simplified_task_frame['full_output']= simplified_task_frame['MemoData']
        verification_frame = simplified_task_frame[simplified_task_frame['full_output'].apply(lambda x: 
                                                                         'VERIFICATION PROMPT ___' in x)].groupby('task_id').last()[['full_output']]
        if len(verification_frame) == 0:
            return verification_frame

        if len(verification_frame)> 0:
            verification_frame['verification']=verification_frame['full_output'].apply(lambda x: x.replace('VERIFICATION PROMPT ___',''))
            verification_frame['original_task']=simplified_task_frame[simplified_task_frame['task_type'] == 'PROPOSAL'].groupby('task_id').first()['full_output']
            verification_frame[['original_task','verification']].copy()
            last_task_status=simplified_task_frame.sort_values('datetime').groupby('task_id').last()['task_type']
            verification_frame['last_task_status']=last_task_status
            outstanding_verification = verification_frame[verification_frame['last_task_status']=='VERIFICATION_PROMPT'].copy()
            outstanding_verification= outstanding_verification[['original_task','verification']].reset_index().copy()

        return outstanding_verification
        
    def send_post_fiat_verification_response(self, response_string, task_id, all_account_info, account_address):
        """
        This function sends a verification response for a given task back to a node.
        The most recent task status must be 'VERIFICATION_PROMPT' to trigger the verification response.
        
        EXAMPLE PARAMETERS
        response_string = 'This link https://livenet.xrpl.org/accounts/rnQUEEg8yyjrwk9FhyXpKavHyCRJM9BDMW is the PFT token mint. You can see that the issuer wallet has been blackholed per lsfDisableMaster'
        task_id = '2024-05-10_00:19__CJ33'
        all_account_info = self.get_memo_detail_df_for_account(account_address=self.user_wallet.classic_address, transaction_limit=5000)
        """
        print("""Note - for the verification response - provide a brief description of your response but
            also feel free to include supplemental information in your google doc 

            wrapped in 
            ___x TASK VERIFICATION SECTION START x___ 

            ___x TASK VERIFICATION SECTION END x___

            """ )
        all_account_info = all_account_info
        simplified_task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=all_account_info, account_address=account_address)
        matching_task = simplified_task_frame[simplified_task_frame['task_id'] == task_id]
        
        if matching_task.empty:
            print(f"No task found with task ID: {task_id}")
            return
        
        most_recent_status = matching_task.sort_values(by='datetime').iloc[-1]['task_type']
        
        if most_recent_status != 'VERIFICATION_PROMPT':
            print(f"The most recent status for task ID {task_id} is not 'VERIFICATION_PROMPT'. Current status: {most_recent_status}")
            return 
        user_name = list(simplified_task_frame['user'])[0]
        user=user_name
        node_account = list(simplified_task_frame[simplified_task_frame['task_id'] 
            == task_id].tail(1)['node_account'])[0]
        source_of_command = matching_task.iloc[0]['node_account']
        verification_response = 'VERIFICATION RESPONSE ___ ' + response_string
        constructed_memo = self.construct_basic_postfiat_memo(user=user_name, 
                                                              task_id=task_id, 
                                                              full_output=verification_response)
        print(verification_response)
        print('converted to memo')

        sending_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user)
        response = self.generic_pft_utilities.send_PFT_with_info(amount=1, memo=constructed_memo, 
                                                                 destination_address=node_account, sending_wallet= sending_wallet)
        account = response.result['Account']
        destination = response.result['Destination']
        memo_map = response.result['Memos'][0]['Memo']
        print(f"{account} sent 1 PFT to {destination} with memo")
        print(self.convert_memo_dict(memo_map))
        return response
    def generate_user_name_mapping(self):
        """ Simplistic user map based on PFT holdings to claim a particular user name """ 
        full_pft_holder_df = self.generic_pft_utilities.output_post_fiat_holder_df()
        full_pft_holder_df['pft_debit']=full_pft_holder_df['balance'].astype(float)*-1
        account_to_pft_holding = full_pft_holder_df.groupby('account').first()['pft_debit']
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address='r4yc85M1hwsegVGZ1pawpZPwj65SVs8PzD',
                transaction_limit=5000, exhaustive=True, pft_only=True)
        all_account_info['user_name']=all_account_info['converted_memos'].apply(lambda x: x['MemoFormat'])
        all_account_info=all_account_info[all_account_info['message_type']=='INCOMING'].copy()
        all_account_info['pft_holdings']=all_account_info['user_account'].map(account_to_pft_holding)
        all_account_info = all_account_info[all_account_info['datetime']>'2024-05-10'].copy()
        pft_holdings_sorted = all_account_info.sort_values('pft_holdings',ascending=False)
        user_name_to_wallet_map = pft_holdings_sorted.groupby('user_name').first()
        return user_name_to_wallet_map['user_account']
        
    def generate_outstanding_task_map_and_verification_for_user(self, user_name ='goodalexander'):
        account_address = self.user_name_mapping[user_name]
        
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address,
                    transaction_limit=5000)
        #task_frame = self.convert_all_account_info_into_simplified_task_frame(all_account_info=all_account_info, 
        #                                                                      account_address=account_address)
        task_frame = self.generic_pft_utilities.convert_all_account_info_into_outstanding_task_df(account_memo_detail_df
                                                                                                  =all_account_info)
        
        required_verification = self.convert_all_account_info_into_required_verification_df(all_account_info=all_account_info, 
                                                                                            account_address=account_address)

        op = {'task_frame': task_frame,
              'required_verification':required_verification}
        return op

    def discord_tooling___pf_outstanding(self,user_name='goodalexander'):
        task_frame = self.generate_outstanding_task_map_and_verification_for_user(user_name=user_name)
        #o#utstanding_task_df = self.generic_pft_utilities.convert_all_account_info_into_outstanding_task_df(account_memo_detail_df=all_account_info)
        df = task_frame['task_frame'].reset_index()
        
        output = "Task List:\n\n"
        for index, row in df.iterrows():
            task_id = row['task_id']
            proposal = row['proposal']
            acceptance = row['acceptance']
        
            output += f"Task ID: {task_id}\n"
            output += f"Proposal: {proposal}\n"
            output += f"Acceptance: {acceptance}\n\n"
        output +="VERIFICATION REQUIREMENTS"
        verification_output = task_frame['required_verification']
        for index, row in verification_output.iterrows():
            task_id = row['task_id']
            original_task = row['original_task']
            verification = row['verification']
            output += f"Task ID: {task_id}\n"
            output += f"Original Task: {original_task}\n"
            output += f"Verification Requirement: {verification}\n\n"
        return output

    def discord_tooling___pf_guide(self):
        guide_string = """!pf_guide output this guide 
!pf_task - ask for a task
!pf_accept - accept a task
!pf_refuse - refuse a task
!pf_submit - submit initial completion of a task
!pf_verify - submit the verification of a completion of a task
!pf_outstanding - get a string of all outstanding tasks and verification items 
!pf_rewards - get a list of all your recent rewards and a summary of your run rate 
and trends 
"""
        print(guide_string)
        return guide_string

    def discord_tooling___pf_task(self,user_name='goodalexander',request_message='Give me a relevant task'):
        account_address = self.user_name_mapping[user_name]
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address,
                            transaction_limit=5000)
        response = self.request_post_fiat(request_message=request_message, 
                               all_account_info=all_account_info, 
                               account_address=account_address)
        return response 

    def extract_task_id(self, string):
        task_id_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}__[A-Z0-9]{4})')
        match = task_id_pattern.search(string)
        if match:
            return match.group(1)
        return ''

    
    def discord_tooling___pf_refuse(self,user_name='goodalexander',request_message='2024-07-04_19:09__VN67 Ambient content is not the focus rn'):
        account_address = self.user_name_mapping[user_name]
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address,
                            transaction_limit=5000)
        
        task_id= self.extract_task_id(request_message)
        refusal_reason = request_message.replace(task_id,'')
        if task_id!='':
            
            response = self.send_refusal_for_task(task_id=task_id, refusal_reason=refusal_reason, 
                                       all_account_info=all_account_info, account_address=account_address)
            return response

    def discord_tooling___pf_accept(self,user_name='goodalexander',request_message='2024-07-04_19:09__VN67 Ambient content is not the focus rn'):
        account_address = self.user_name_mapping[user_name]
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address,
                            transaction_limit=5000)
        
        task_id= self.extract_task_id(request_message)
        acceptance_string = request_message.replace(task_id,'')
        if task_id!='':
            response = self.send_acceptance_for_task_id(task_id=task_id, acceptance_string=acceptance_string, 
                                       all_account_info=all_account_info, account_address=account_address)
            return response

    def discord_tooling___pf_submit(self,user_name='goodalexander',request_message='2024-07-04_19:09__VN67 Ambient content is not the focus rn'):
        account_address = self.user_name_mapping[user_name]
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address,
                            transaction_limit=5000)
        
        task_id= self.extract_task_id(request_message)
        completion_string = request_message.replace(task_id,'')
        if task_id!='':
            response = self.send_post_fiat_initial_completion(task_id=task_id, completion_string=completion_string, 
                                       all_account_info=all_account_info, account_address=account_address)
            return response

    def discord_tooling___pf_verify(self,user_name='goodalexander',request_message='2024-07-04_19:09__VN67 Ambient content is not the focus rn'):
        account_address = self.user_name_mapping[user_name]
        all_account_info = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address,
                            transaction_limit=5000)
        
        task_id= self.extract_task_id(request_message)
        response_string = request_message.replace(task_id,'')
        if task_id!='':
            response = self.send_post_fiat_verification_response(task_id=task_id, response_string=response_string, 
                                       all_account_info=all_account_info, account_address=account_address)
            return response
        
    def ux__convert_response_object_to_status_message(self, response):
        """ Takes a response object from an XRP transaction and converts it into legible transaction text""" 
        status_constructor = 'unsuccessfully'
        if 'success' in response.status:
            status_constructor = 'successfully'
        non_hex_memo = self.convert_memo_dict(response.result['Memos'][0]['Memo'])
        user_string = non_hex_memo['full_output']
        amount_of_pft_sent = response.result['Amount']['value']
        node_name = response.result['Destination']
        output_string = f"""User {status_constructor} sent {amount_of_pft_sent} PFT with request '{user_string}' to Node {node_name}"""
        return output_string