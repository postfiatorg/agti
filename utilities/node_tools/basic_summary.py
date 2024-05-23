import re
import pandas as pd
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
class NodeTaskEvaluation:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.node_name = pw_map['node_name']
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        self.node_address = self.pw_map[f'{self.node_name}__v1xrpaddress']
    def output_node_current_information_map(self):
        full_memo_details = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address,pft_only=True)
        
        full_memo_details['classified_memos']= full_memo_details['converted_memos'].apply(lambda x: 
                                                   self.generic_pft_utilities.classify_task_string(string=str(x)))
        
        #full_memo_string = str(memo_dict)
        task_id_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}(?:__[A-Z0-9]{4})?)')
        
        # Function to extract task_id using the pattern
        def extract_task_id(memo):
            match = task_id_pattern.search(memo)
            return match.group(0) if match else None
        
        # Apply the function to the 'classified_memos' column and create a new column 'task_id'
        full_memo_details['task_id'] = full_memo_details['converted_memos'].apply(lambda x: extract_task_id(str(x)))
        task_id_only=full_memo_details[full_memo_details['task_id'].apply(lambda x: str(x)!='None')]#['task_id']
        
        most_recent_task_state = task_id_only.sort_values('datetime').groupby('task_id').last()[['classified_memos']]
        node_request_details = full_memo_details[full_memo_details['classified_memos'].apply(lambda x: 
                                                                      'NODE_REQUEST' in x)].copy()
        node_request_details['reward']=node_request_details['tx'].apply(lambda x: x['Amount']['value']).astype(float)
        node_request_details['raw_text'] = node_request_details['converted_memos'].apply(lambda x: x['MemoData'])+'|'
        
        node_reward_details = node_request_details.groupby('task_id')[['reward','raw_text']].sum()
        node_reward_details['most_recent_state']=most_recent_task_state
        output_map= {'node_reward_details':node_reward_details.reset_index(),
         'most_recent_task_state':most_recent_task_state, 'classified_memo_df':full_memo_details}
        return output_map