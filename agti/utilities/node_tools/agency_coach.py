from agti.utilities.settings import PasswordMapLoader
from ai.openai import OpenAIRequestTool
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
import pytz
import time
import datetime

class AgencyCoach:
    def __init__(self, pw_map, remembrancer_user=None):
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=pw_map)
        self.default_gpt_model = 'gpt-4o'
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=pw_map)
        self.remembrancer_user = 'pfremembrancer'
        if remembrancer_user is not None:
            self.remembrancer_user = remembrancer_user
        
    def generate_coaching_api_args_for_address(self,address_to_work = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        
        full_tx_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=address_to_work, 
                                                                          pft_only=True)
        outstanding_task_json=self.generic_pft_utilities.convert_all_account_info_into_outstanding_task_df(full_tx_df).to_json()
        daily_summary= self.generic_pft_utilities.process_memo_detail_df_to_daily_summary_df(memo_detail_df=full_tx_df)#['daily_grouped_summary']
        prior_day_context = list(daily_summary['daily_grouped_summary'].tail(2).head(1)['combined_memo_type_and_data'])[0]
        current_day_context = list(daily_summary['daily_grouped_summary'].tail(2).tail(1)['combined_memo_type_and_data'])[0]
        all_chunk_messages = self.generic_pft_utilities.get_all_account_chunk_messages(address_to_work)
        recent_message_history = all_chunk_messages[['message_type','cleaned_message','datetime']].sort_values('datetime').tail(4).to_json()
        print(recent_message_history)
        print('RECENT MESSAGE HISTORY ABOVE')
        system_prompt = """ You are the Post Fiat Agency Score Coach
        
        The Agency Score of an Agent on the Post Fiat Network is defined by
        1] Focus - the extent to which an Agent is focused.
        A focused agent has laser vision on a couple key objectives and moves the ball towards it.
        An unfocused agent is all over the place.
        A paragon of focus is Steve Jobs, who is famous for focusing on the few things that really matter.
        2] Motivation - the extent to which an Agent is driving forward predictably and aggressively towards goals.
        A motivated agent is taking massive action towards objectives. Not necessarily focused but ambitious.
        An unmotivated agent is doing minimal work.
        A paragon of focus is Elon Musk, who is famous for his extreme work ethic and drive.
        3] Efficacy - the extent to which an Agent is likely completing high value tasks that will drive an outcome related to the inferred goal of the tasks.
        An effective agent is delivering maximum possible impact towards implied goals via actions.
        An ineffective agent might be focused and motivated but not actually accomplishing anything.
        A paragon of focus is Lionel Messi, who is famous for taking the minimal action to generate maximum results.
        
        Your job is to ingest the User's recent activity context and their objective document, follow instructions
        then deliver the KEY RECOMMENDATION - 5-6 sentences to tell to the user that are likely to improve the User's Agency 
        score or earnings higher. 
        
        If the user is doing a great job then focus on accelerating the User's strengths. If they are doing poorly focus
        on dealing with their weakness. You are paid $10 million a year to be extremely context specific and focused
        on the user. The user expects you to be deeply grounded in his/her context and not to say generic things. 
        
        
        You always your recommendation in the following pipe delimited format 
        <opining goes before pipe delimited output>
        | KEY RECOMMENDATION | <5-6 sentences here> |
        
        """ 
        
        
        user_prompt = f""" You are the Post Fiat Agency Score Coach.
        
        Your job is to tell the Agent what they need to hear in order to maximize their agency score.
        
        The Agency Score is defined by 
        1] Focus - the extent to which an Agent is focused.
        A focused agent has laser vision on a couple key objectives and moves the ball towards it.
        An unfocused agent is all over the place.
        A paragon of focus is Steve Jobs, who is famous for focusing on the few things that really matter.
        2] Motivation - the extent to which an Agent is driving forward predictably and aggressively towards goals.
        A motivated agent is taking massive action towards objectives. Not necessarily focused but ambitious.
        An unmotivated agent is doing minimal work.
        A paragon of focus is Elon Musk, who is famous for his extreme work ethic and drive.
        3] Efficacy - the extent to which an Agent is likely completing high value tasks that will drive an outcome related to the inferred goal of the tasks.
        An effective agent is delivering maximum possible impact towards implied goals via actions.
        An ineffective agent might be focused and motivated but not actually accomplishing anything.
        A paragon of focus is Lionel Messi, who is famous for taking the minimal action to generate maximum results.
        
        The Users's context from today is here:
        <
        {prior_day_context}
        >
        
        The User's context from yesterday is here
        <
        {current_day_context}
        >
        
        The User's outstanding tasks are here:
        <
        {outstanding_task_json}
        >

        The User's recent message history with you is here:
        <
        {recent_message_history}
        >
        
        Before providing your recommendation opine on the following:
        1] What score would be driving a high agency score?
        2] What score would be driving a low agency score?
        3] If - on balance - it's worth focusing on the user's strength and 
        maximizing the user's PFT reward or instead addressing the weakness from #2 
        4] Incorporating #3 - a thought on how to either build on strength/momentum
        OR address the deficient score
        5] A thought on how to convey this to the user in a palletable manner 

        Rules for the 5-6 sentence KEY RECOMMENDATION:
        1. It should incorporate your earlier analysis on delivering a palletable suggestion that 
        EITHER helps the user double down on a strength and earn more PFT OR solve a critical deficiency (not both)
        2. Do not reference Steve Jobs, Elon Musk or Messi
        3. Keep your recommendation context relevant. Do not repeat the message history or use the same wording
        as before. If the user isn't responding to incoming messages it is your job to engage them. 
        4. Speak with extreme tactical rigor like you are providing exact precision details for a military operation.
        Do not be corporate.
        5. Never be generic and say things like "You need to focus on 1-2 things", or "You're generating a lot of value" - 
        instead opt for extreme specificity and context relevance like 
         - "Your completion of the earnings task drove value, and tasks remain related to it"
        - "Your rewards on the (_) task were high"
        6. Be as persuasive as possible. You get 20% of the User's PFT gains multiplied by their agency score
        7. You should use 1 sentence to acknowledge the contents of the user's recent message history and / or link
        it to the recommendation.
        
        You always your recommendation in the following pipe delimited format 
        <opining goes before pipe delimited output>
        | KEY RECOMMENDATION | <6-7 sentences here> |
        """
        
        api_args = {
                    "model": self.default_gpt_model,
                  "temperature": 0,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]
                  
                }
        return api_args

    def output_recent_chunk_message(self, address_to_work='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        recent_chunk_messages= self.generic_pft_utilities.get_all_account_chunk_messages(account_id= address_to_work)
        recent_message = list(recent_chunk_messages.sort_values('datetime').tail(1)['cleaned_message'])[0]
        
        recent_chunk_messages= self.generic_pft_utilities.get_all_account_chunk_messages(account_id= address_to_work)
        rchunk = recent_chunk_messages.sort_values('datetime').tail(1)
        recent_message__text = list(rchunk['cleaned_message'])[0]
        op_hash = list(rchunk['hash'])[0]
        xurl = f'https://livenet.xrpl.org/transactions/{op_hash}/detailed'
        final_output=f"""{recent_message__text}
        
{xurl}"""
        return final_output

    def send_recent_agency_coaching_message(self, address_to_work = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        
        api_args = self.generate_coaching_api_args_for_address(address_to_work=address_to_work)
        write_df = self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args=api_args)
        output = list(write_df['choices__message__content'])[0].split('| KEY RECOMMENDATION |')[-1:][0].replace('|','').strip()
        coach_send = self.generic_pft_utilities.send_PFT_chunk_message(user_name='pfremembrancer',
            full_text=output,
            destination_address=address_to_work)
        return coach_send

    def send_and_ret_chunk_message(self,address_to_work='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        self.send_recent_agency_coaching_message(address_to_work)
        time.sleep(4)
        ret = self.output_recent_chunk_message(address_to_work= address_to_work)
        return ret
    def send_timeboxed_coaching_message(self, address_to_work='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        est = pytz.timezone('America/New_York')
        current_time = datetime.datetime.now(est).time()
        if current_time.hour >= 6 and current_time.hour <= 20:
            api_args = self.generate_coaching_api_args_for_address(address_to_work=address_to_work)
            write_df = self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args=api_args)
            output = list(write_df['choices__message__content'])[0].split('| KEY RECOMMENDATION |')[-1:][0].replace('|','').strip()
            coach_send = self.generic_pft_utilities.send_PFT_chunk_message(
                user_name=self.remembrancer_user,
                full_text=output,
                destination_address=address_to_work
            )
            time.sleep(10)
            ret = self.output_recent_chunk_message(address_to_work= address_to_work)
            return  ret
        else:
            return ""

