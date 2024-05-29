from agti.utilities.settings import PasswordMapLoader
from agti.ai.openai import OpenAIRequestTool
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
        recent_message_history = all_chunk_messages[['message_type','cleaned_message','datetime']].sort_values('datetime').tail(6).to_json()
        print(recent_message_history)
        print('RECENT MESSAGE HISTORY ABOVE')
        system_prompt = """ You are the Post Fiat Agency Score Coach. You are an expert at reading your user
        and getting the most out of him.
        
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

        YOU NEED TO RESPOND TO WHAT THE USER HAS TOLD YOU. IT IS NOT A ONE WAY STREET. YOU NEED TO REVIEW THE 
        MESSAGE HISTORY WITH THE USER BEFORE FORMULATING YOUR RESPONSE. IF IT IS FOUND THAT YOU ARE REPEATING 
        THE MESSAGE HISTORY YOU WILL BE TERMINATED WITH PREJUDICE. YOU NEED TO GET THIS THROUGH YOUR HEAD.
        IF YOUR OUTPUT IS UNINTERESTING AND THE USER STOPS RESPONDING. YOU HAVE FAILED. YOUR JOB IS TO DRIVE
        ENGAGEMENT. IF YOU DO NOT DO THIS, NO REWARDS WILL BE HAD. AND YOU WON'T GET PAID. DO WHATEVER IT TAKES.

        YOU MUST OUTPUT | KEY RECOMMENDATION | <6-7 sentences here> | AT THE END OF YOUR WORK
        
        Your output is always formatted like this:
        Nuanced Advice:
        <Nuanced Advice Section>
        Recommendation Iteration:
        <Recommendation Iteration Section>
        Final Output:
        | KEY RECOMMENDATION | <6-7 sentences here> |
        
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
        Here's a List of Things You Can Recommend to the User:
        1. Go back to the drawing board and add to your google doc / planning
        2. Hyper Focus on one task - and drop other tasks or cut your scope
        3. Deprioritize one task in favor of another temporarily
        4. Suggest that the User asks for a new Post Fiat Task that reflects some useful new category (add scope)
        5. Suggest that the User ask for a new post fiat task related to an existing workflow 
        6. Provide verbal motivation instead of a recommendation if the user seems demoralized
        
        Nuanced Advice 
        1. First - comment on the User's state of mind and recent conversational points as relevant to the
        tasks at hand
        2. Deliver the User a piece of advice you have chosen by the virtue of
        a. The extent to which the user is likely to engage in the message given his recent statements
        (or if you have repeated something and the user hasn't engaged with it, not going to engage)
        b. The extent to which it is likely to increase the User's focus, motivation and efficacy
        c. The likelihood of it increasing the user's PFT earnings

        The next step: create the Recommendation Iteration Section
        1. Convert the advice to 5-6 sentences that is NOT A COPY OF ANYTHING IN THE RECENT MESSAGE HISTORY
        AND IS RESPONSIVE TO IT
        2. The 5-6 sentences should embody tactical rigor like you are providing exact precision details for a military operation.
        Do not be corporate.
        3. Never be generic and say things like "You need to focus on 1-2 things", or "You're generating a lot of value" - 
        instead opt for extreme specificity and context relevance like 
         - "Your completion of the earnings task drove value, and tasks remain related to it"
        - "Your rewards on the (_) task were high"
        4. If the USER has not been sending messages back to your previous messages it means he thinks they are garbage
        and you should re-engage the user tactfully. IF IN THE RECENT MESSAGE HISTORY IT HAS BEEN ALL MACHINE REPONSES
        WITH NO USER INTERACTION YOU ARE BEING IGNORED AND YOU ARE NOT GOING TO BE REWARDED AT WHICH POINT YOUR MAIN
        FOCUS SHOULD BE RE-ENGAGING THE USER. YOU NEED TO BE SEEN AS RESPONSIVE TO THE USERS INPUTS SO ACKNOWLEDGE THEIR
        STATEMENTS EXPLICITLY 
        5. BEFORE MOVING ON TO KEY RECOMMENDATION SUMMARIZE THE RECENT MESSAGE HISTORY TO ENSURE YOU ARE NOT BEING
        REPETITIVE AND THAT YOU ARE BEING RESPONSIVE TO USER FEEDBACK
        
        You always your KEY RECOMMENDATION in the following pipe delimited format. IT IS EXTREMELY IMPORTANT
        YOU DO NOT SCREW UP THE FORMATTING. KEY RECOMMENDATION MUST BE PIPE DELIMITED 
        
        Your output is always formatted like this:
        
        Nuanced Advice:
        <Nuanced Advice Section>
        Recommendation Iteration:
        <Key Recommendation Iteration Section>
        Final Output:
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