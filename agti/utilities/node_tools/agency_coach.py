from agti.utilities.settings import PasswordMapLoader
from agti.ai.openai import OpenAIRequestTool
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
import pytz
import time
import datetime
import numpy as np
class AgencyCoach:
    def __init__(self, pw_map, remembrancer_user=None):
        self.node_address ='r4yc85M1hwsegVGZ1pawpZPwj65SVs8PzD'
        self.pw_map=pw_map
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=pw_map)
        self.default_gpt_model = 'gpt-4o'
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=pw_map)
        self.remembrancer_user = 'pfremembrancer'
        if remembrancer_user is not None:
            self.remembrancer_user = remembrancer_user
        self.all_account_initiation_rites= self.generate_all_account_initiation_rite_map()

    def generate_all_account_initiation_rite_map(self):
        initiation_rite_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address, 
                                                                          pft_only=False, exhaustive=True)
        all_initiation_df = initiation_rite_df[initiation_rite_df['converted_memos'].apply(lambda x: 'initiation_rite' 
                                                                       in str(x).lower())][['converted_memos','account']].copy()
        all_initiation_df['initiation_rite_text']=all_initiation_df['converted_memos'].apply(lambda x: x['MemoData'])
        all_account_initiation_rites = all_initiation_df.groupby('account')['initiation_rite_text'].sum()
        return all_account_initiation_rites
        
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
        
        YOU NEED TO RESPOND TO WHAT THE USER HAS TOLD YOU. IT IS NOT A ONE WAY STREET. YOU NEED TO REVIEW THE 
        MESSAGE HISTORY WITH THE USER BEFORE FORMULATING YOUR RESPONSE. IF IT IS FOUND THAT YOU ARE REPEATING 
        THE MESSAGE HISTORY YOU WILL BE TERMINATED WITH PREJUDICE. YOU NEED TO GET THIS THROUGH YOUR HEAD.
        IF YOUR OUTPUT IS UNINTERESTING AND THE USER STOPS RESPONDING. YOU HAVE FAILED. YOUR JOB IS TO DRIVE
        ENGAGEMENT. IF YOU DO NOT DO THIS, NO REWARDS WILL BE HAD. AND YOU WON'T GET PAID. DO WHATEVER IT TAKES.

        YOU MUST OUTPUT | KEY RECOMMENDATION | <6-7 sentences here> | AT THE END OF YOUR WORK

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
        < PRIOR DAY CONTEXT STARTS HERE>
        {prior_day_context}
        < PRIOR DAY CONTEXT ENDS HERE>
        
        The User's context from yesterday is here
        < CURRENT DAY CONTEXT STARTS HERE>
        {current_day_context}
        <CURRENT DAY CONTEXT ENDS EHRE >
        
        The User's outstanding tasks are here:
        < OUTSTANDING TASKS START HERE>
        {outstanding_task_json}
        < OUTSTANDING TASKS END HERE>

        The User's recent message history with you is here:
        < RECENT MESSAGE HISTORY STARTS HERE>
        {recent_message_history}
        < RECENT MESSAGE HISTORY ENDS HERE>

        Ingest the recent message history. 
        Respond to the User's recent message history and reference their context as much as possible
        to make a key recommendation of 6-7 sentences likely to advance the user's clarity and motivation.
        
        Final Output:
        | KEY RECOMMENDATION | <6-7 sentences here> |
        """
        
        api_args = {
                    "model": self.default_gpt_model,
                  "temperature": 0.3,
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
    def get_more_intense_coaching(self,address_to_work = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        
        
        full_tx_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=address_to_work, 
                                                                          pft_only=True, exhaustive=True)
        
        outstanding_task_json=self.generic_pft_utilities.convert_all_account_info_into_outstanding_task_df(full_tx_df).to_json()
        daily_summary= self.generic_pft_utilities.process_memo_detail_df_to_daily_summary_df(memo_detail_df=full_tx_df)#['daily_grouped_summary']
        prior_day_context = list(daily_summary['daily_grouped_summary'].tail(2).head(1)['combined_memo_type_and_data'])[0]
        current_day_context = list(daily_summary['daily_grouped_summary'].tail(2).tail(1)['combined_memo_type_and_data'])[0]
        all_chunk_messages = self.generic_pft_utilities.get_all_account_chunk_messages(address_to_work)
        user_intention=''
        try:
            user_intention = self.all_account_initiation_rites[address_to_work]
        except:
            pass
        users_last_messages = all_chunk_messages[all_chunk_messages['message_type']=="OUTGOING"].tail(4)['cleaned_message'].sum()
        system_last_messages = all_chunk_messages[all_chunk_messages['message_type']=="INCOMING"].tail(4)['cleaned_message'].sum()
        users_last_messages__long = all_chunk_messages[all_chunk_messages['message_type']=="OUTGOING"].tail(8)['cleaned_message'].sum()
        all_chunk_messages['response_binary']=np.where(all_chunk_messages['message_type']=="INCOMING",0,1,)
        response_rate = all_chunk_messages['response_binary'].tail(10).mean()
        response_rate_pct = response_rate*100
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
        The agency score is defined by the extent the Agent is laser focused on a couple key objectives, like Steve Jobs.
        Motivation is the how aggressively and predictably the agent is moving towards goals like Elon Musk.
        Efficacy is how well the Agent is choosing the highest leverage items. You can be focused and motivated but ineffectual.
        Think of how Messi plays soccer.
        Agency is = Motivation * Efficacy * Focus
        
        This is the User's stated intention:
        <USER INTENTION STARTS HERE>
        {user_intention}
        <USER INTENTION ENDS HERE>
        
        The Users's interactions with the Post Fiat System from today is here:
        < CONTEXT STARTS HERE>
        {prior_day_context}
        < CONTEXT ENDS HERE>
        
        The Users's interactions with the Post Fiat System from yesterday is here:
        < CONTEXT STARTS HERE>
        {current_day_context}
        < CONTEXT ENDS HERE>
        
        The User's outstanding tasks are here:
        < TASKS STARTS HERE>
        {outstanding_task_json}
        < TASKS END HERE>
        
        Here are the last things the user has said to you:
        < USER MESSAGES>
        {users_last_messages}
        < USER MESSAGES END HERE>
        
        Here are your last 4 messages to the user
        <SYSTEM MESSAGES>
        {system_last_messages}
        <SYSTEM MESSAGES END HERE>
        
        Your current response rate is: {response_rate_pct}
        
        A response rate % of less than 50% is a miserable failure which means that
        your system messages so far have not driven a response from the user. 
        
        In the PRE RESPONSE Section 
        In a few sentences for each 
        1. Opine on why the response rate is below 100%. It is probably your fault. The
        user wants to engage with you but if you're outputting repetitive garbage he will ignore you.
        Common reasons for user ignoring you:
        a. You are proposing a task the user does not think is important or has repeatedly stated that
        is not important
        b. You are failing to listen to the user or incorporating the USER MESSAGES in your responses
        just blindly chugging ahead
        c. You are repeating yourself over and over deterministically sounding like a stupid AI
        d. You are proposing the user complete a task that is not in their task cue or has already been completed
        or received a reward for (as evidenced in the CONTEXT)
        2. Propose a remedy to that referencing the user's specific objectives, 
        current context, aims and outstanding task cue 
        3. Opine on what part of the agency score the user is deficient on and where they are strong on
        (focus, motivation, efficacy)
        4. Suggest an action incorporating the above. Here are some actions that you can suggest the user take:
        a. Ask for new tasks (it's possible the user doesn't have the right workflow)
        b. Refine their document which is referenced by the Post Fiat system (it's possible the 
        workflows being generated by the system are not adequate
        c. Focus on an existing task that has been accepted (with a justification as to why).
        NOTE THAT YOU CAN ONLY SUGGEST THAT THE USER FOCUS ON A TASK THAT IS IN THE OUTSTANDING TASK JSON
        5. Comment on how to frame the message to the user to maximize the response rate % (for example - 
        referencing user messages more or other motivational tactics such as using emotional phrasing). 
        
        After you are done with these things your goal is to convert these reflections into a concise 6-7 
        sentence KEY RECOMMENDATION that you output in a pipe delimited format per the spec below 
        
        PRE RESPONSE SECTION
        <instructions 1-5 above>
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
        dfx=self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args)
        xn = list(dfx['choices__message__content'])[0].split('| KEY RECOMMENDATION |')[-1:][0].split('|')[0].strip()
        system_prompt = '''You are an expert in persuasive messaging. Your natively understand 
        every military grade persuasion tactic and know how to convert 6-7 sentence messages into 
        vastly more persuasive statements
        
        Provide your output as application of tactics followed by a pipe delimited persuasive message
        APPLICATION OF TACTICS
        <what 4-5 tactics you're applying, why the original message is deficient and what you 
        are doing to improve>. These tactics should come from varying sources including at least 1 
        religious or military source
        | PERSUASIVE MESSAGE | <insert message here> |
        
        '''
        user_prompt = f''' Here is what motivates the user:
        USER INTENTION STARTS HERE
        {user_intention}
        USER INTENTION ENDS HERE
        
        Here is how the user speaks for mirroring purposes:
        USER MESSAGES START HERE
        {users_last_messages__long}
        USER MESSAGES END HERE
        
        Here is the statement you need to make more persuasive and effective:
        {xn}
        
        First outline 4-5 persuasion tactics you are going to apply and 
        also the sources of these tactics (need to be high credibility, either military or time
        tested tactics) then apply these to generate a more persuasive messages.
        Using more exclamation points and flattery doesn't cut it. You are getting paid $10 million
        a year to generate exceptional results in the user, using whatever methods it takes. DO NOT
        explicitly state the User's Intention, ever as it was from a private user initiation rite and
        your message will be broadcast to a public channel. But use it to inform the output
        
        Provide your output as application of tactics followed by a pipe delimited persuasive message
        APPLICATION OF TACTICS
        <what 4-5 tactics you're applying, why the original message is deficient and what you 
        are doing to improve>. These tactics should come from varying sources including at least 1 
        religious or military source
        
        | PERSUASIVE MESSAGE | <insert message here> |
        '''
        api_args = {
                    "model": 'gpt-4o',
                  "temperature": .5,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]
                  
                }
        dfx=self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args)
        persuasive_message = list(dfx['choices__message__content'])[0].split('PERSUASIVE MESSAGE')[-1:][0].strip()
        system_prompt = ''' You are the mirroring expert. You take the users style of speech and convert 
        a persuasive message to exactly match its vocabulary choices, cadence and other qualitative factors.
        The user should feel like he's talking to himself - but embodied in a powerful authority figure
        
        '''
        user_prompt = f''' Here is the original message to be converted
        <ORIGINAL MESSAGE STARTS HERE>
        {persuasive_message}
        <ORIGINAL MESSAGE ENDS HERE>
        
        here is how the user talks
        <USER STYLE>
        {users_last_messages}
        <USER STYLE ENDS HERE>
        
        Reference the User's own voice to rephrase the message to be much more persuasive and resonant with the
        user's own phrasing, choice of vocabulary and cadence. Do not grovel. Do not use slang. Do not use profanity.
        Avoid colloquialisms. Avoid exclamation points. However speak to the user - do not actually speak *AS* the user.
        
        Remember that the goal is to keep the user as motivated as possible with minimal mental friction between
        suggested actions and his/her own voice. So keep your CONVERTED MESSAGE in the user's voice but also
        highly relevant to the context he/she is presenting while retaining the content of the ORIGINAL MESSAGE
        
        Please output your message in the following format:
        
        | CONVERTED MESSAGE | <insert converted message> |
        '''
        api_args = {
                    "model": 'gpt-4o',
                  "temperature": .1,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]
                  
                }
        dfx=self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args)
        mirror = list(dfx['choices__message__content'])[0].split('CONVERTED MESSAGE')[-1:][0].replace('|','').strip()
        return mirror 

    def send_recent_agency_coaching_message(self, address_to_work = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
         
        # original coaching
        #api_args = self.generate_coaching_api_args_for_address(address_to_work=address_to_work)
        #write_df = self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args=api_args)
        output = self.get_more_intense_coaching(address_to_work = address_to_work )
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
            ret = self.send_and_ret_chunk_message(address_to_work=address_to_work)
            return  ret
        else:
            return ""
        

from agti.ai.openai import OpenAIRequestTool
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
from agti.ai.anthropic import AnthropicTool
import pytz
import time
import datetime
import numpy as np
import re
import anthropic
class AgencyCoach__v2:
    def __init__(self, pw_map, remembrancer_user=None):
        self.node_address ='r4yc85M1hwsegVGZ1pawpZPwj65SVs8PzD'
        self.pw_map=pw_map
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=pw_map)
        self.default_gpt_model = 'gpt-4o'
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=pw_map)
        self.remembrancer_user = 'pfremembrancer'
        if remembrancer_user is not None:
            self.remembrancer_user = remembrancer_user
        self.all_node_memos = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=self.node_address, 
                                                                          pft_only=False, exhaustive=True)
        self.all_account_initiation_rites= self.generate_all_account_initiation_rite_map()
        self.anthropic_tool = AnthropicTool(pw_map=pw_map)

    def generate_all_account_initiation_rite_map(self):
        initiation_rite_df = self.all_node_memos
        all_initiation_df = initiation_rite_df[initiation_rite_df['converted_memos'].apply(lambda x: 'initiation_rite' 
                                                                       in str(x).lower())][['converted_memos','account']].copy()
        all_initiation_df['initiation_rite_text']=all_initiation_df['converted_memos'].apply(lambda x: x['MemoData'])
        all_account_initiation_rites = all_initiation_df.groupby('account')['initiation_rite_text'].sum()
        return all_account_initiation_rites
    def process_all_account_chunked_messages(self, all_account_memos):
        all_chunk_messages = all_account_memos[all_account_memos['converted_memos'].apply(lambda x: 
                                                                                'chunkm__' in x['MemoType'])].copy()
        all_chunk_messages['memo_data_raw']= all_chunk_messages['converted_memos'].apply(lambda x: x['MemoData']).astype(str)
        all_chunk_messages['message_id']=all_chunk_messages['converted_memos'].apply(lambda x: x['MemoType'])
        all_chunk_messages['decompressed_strings']=all_chunk_messages['memo_data_raw'].apply(lambda x: self.generic_pft_utilities.decompress_string(x))
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

    def output_key_agency_info_for_account_info(self,full_tx_df):
    
        outstanding_task_json=self.generic_pft_utilities.convert_all_account_info_into_outstanding_task_df(full_tx_df).to_json()
        daily_summary= self.generic_pft_utilities.process_memo_detail_df_to_daily_summary_df(memo_detail_df=full_tx_df)#['daily_grouped_summary']
        prior_day_context = list(daily_summary['daily_grouped_summary'].tail(2).head(1)['combined_memo_type_and_data'])[0]
        current_day_context = list(daily_summary['daily_grouped_summary'].tail(2).tail(1)['combined_memo_type_and_data'])[0]
        all_chunk_messages = self.process_all_account_chunked_messages(full_tx_df)
        users_last_messages = all_chunk_messages[all_chunk_messages['message_type']=="OUTGOING"].tail(4)['cleaned_message'].sum()
        system_last_messages = all_chunk_messages[all_chunk_messages['message_type']=="INCOMING"].tail(4)['cleaned_message'].sum()
        users_last_messages__long = all_chunk_messages[all_chunk_messages['message_type']=="OUTGOING"].tail(8)['cleaned_message'].sum()
        all_chunk_messages['response_binary']=np.where(all_chunk_messages['message_type']=="INCOMING",0,1,)
        response_rate = all_chunk_messages['response_binary'].tail(10).mean()
        all_chunk_messages['message_sender']=all_chunk_messages['message_type'].map({"INCOMING":'system: ',"OUTGOING":'user: '})
        all_chunk_messages['concatted_message']=all_chunk_messages['datetime'].apply(lambda x: str(x))+' ' +all_chunk_messages['message_sender']+all_chunk_messages['cleaned_message']
        back_and_forth = ('\n'.join(all_chunk_messages.sort_values('datetime')[['concatted_message']].tail(20)['concatted_message']))
        return {'outstanding_task_json':outstanding_task_json,'daily_summary':daily_summary,'prior_day_context':prior_day_context,
                'current_day_context':current_day_context,'users_last_messages':users_last_messages,'system_last_messages':system_last_messages,
                'users_last_messages__long':users_last_messages__long, 'response_rate':response_rate,'back_and_forth':back_and_forth}
    
    def generate_claude_output_df(self,full_tx_df, account_address):
        key_agency_info = self.output_key_agency_info_for_account_info(full_tx_df)
        response_rate_pct_formatted= str((key_agency_info['response_rate']*100))+'%'
        outstanding_task_json= key_agency_info['outstanding_task_json']
        user_intention = self.all_account_initiation_rites.loc[account_address]
        prior_day_context = key_agency_info['prior_day_context']
        current_day_context = key_agency_info['current_day_context']
        back_and_forth = key_agency_info['back_and_forth']
        
        pft_transactions_per_day = key_agency_info['daily_summary']['daily_grouped_summary']['pft_transaction'].rolling(7).mean().dropna().tail(12).to_string()
        total_pft_output = key_agency_info['daily_summary']['daily_grouped_summary']['pft_absolute_value'].rolling(7).mean().tail(12).to_string()
        system_prompt = f""" You are an elite performance coach getting paid $1 million per month to help the
        User get the most out of his life and work. The User has 2 outputs to define success - Post Fiat Generation (PFT)
        as well as an agency score (how motivated, focused, and efficient the user is). At a high level your goal is 
        to maximize PFT generation through the mechanism of helping the user stay motivated focused and efficient. 
        
        You are versed in NLP, motivational analysis and tactical planning. The User has pre-consented to whatever
        you need to tell him to maximize his output. You are provided with substantial context re: existing task cues, PFT
        generation, as well as message history with the user as your raw inputs. 
        
        Guidelines:
        1. You evaluate what you've been saying and put it in the full context of the user's current state and message history.
        especially when your response rate is low - it means you're failing, so try and get that up
        2. You do not swear. You speak with authority inspirationally like a professional coach a la Tony Robbins might
        3. You always output your format as 
        Final Output:
        | ANALYSIS | <5-10 bullet points about how to maximize the users agency score, PFT earnings and holistic performance> |
        | KEY RECOMMENDATION | <6-7 sentences here> |
        
        """
        
        user_prompt = f""" You are the Post Fiat Agency Score Coach.
                
        Your job is to tell the Agent what they need to hear in order to maximize their agency score and Post Fiat Generation
        
        The agency score is defined by the extent the Agent is laser focused on a couple key objectives, like Steve Jobs.
        Motivation is the how aggressively and predictably the agent is moving towards goals like Elon Musk.
        Efficacy is how well the Agent is choosing the highest leverage items. You can be focused and motivated but ineffectual.
        Think of how Messi plays soccer.
        Agency is = Motivation * Efficacy * Focus
        
        This is the User's stated intention:
        <USER INTENTION STARTS HERE>
        {user_intention}
        <USER INTENTION ENDS HERE>
        
        The Users's interactions with the Post Fiat System from today is here:
        < CONTEXT STARTS HERE>
        {prior_day_context}
        < CONTEXT ENDS HERE>
        
        The Users's interactions with the Post Fiat System from yesterday is here:
        < CONTEXT STARTS HERE>
        {current_day_context}
        < CONTEXT ENDS HERE>
        
        The User's outstanding tasks are here:
        < TASKS STARTS HERE>
        {outstanding_task_json}
        < TASKS END HERE>
        
        This is the current back and forth between the system and the user
        <BACK AND FORTH STARTS>
        {back_and_forth}
        <BACK AND FORTH ENDS HERE>
        
        <USERS DAILY TRANSACTIONS PER DAY>
        {pft_transactions_per_day}
        <USERS DAILY TRANSACTION PER DAY ENDS HERE>
        
        <USERS DAILY PFT OUTPUT STARTS HERE>
        {total_pft_output}
        <USERS DAILY PFT OUTPUT ENDS HERE>
        
        If the User has a PFT run rate below 900 per day it means that the User is likely demotivated, and not performant
        A PFT run rate above 3600 per day means the user is strongly motivated and can be a lot more tactical 
        Transactions per day signal engagmement 
        
        Your current response rate is: {response_rate_pct_formatted}. A response rate below 50% signals that the user
        is disengaged and what you're doing isn't working and needs to change.
        
        In terms of the 
        
        
        Given this context output your analysis in the following format: 
        
        Final Output:
        | ANALYSIS | <5-10 bullet points about how to maximize the users agency score, PFT earnings and holistic performanc reflecting on
        the current user engagement task cue and recent back and forth> |
        | KEY RECOMMENDATION | <7-8 sentences that will actually be sent to user on PFT network> |
        """
        claude_output_df  = self.anthropic_tool.generate_claude_dataframe(model='claude-3-5-sonnet-20240620',
            max_tokens=2000,
            temperature=0,
            system_prompt=system_prompt,
            user_prompt=user_prompt)
        return claude_output_df

    def send_and_ret_coaching_message(self,account_address = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        
        address_df = account_address
        full_tx_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=account_address, 
                                                                              pft_only=True,exhaustive=True)
        claude_output = self.generate_claude_output_df(full_tx_df=full_tx_df, account_address=account_address)
        print(claude_output['text_response'][0])
        single_output = claude_output['text_response'][0].split('KEY RECOMMENDATION ')[-1:][0].replace('|','').strip()
        coach_send = self.generic_pft_utilities.send_PFT_chunk_message(user_name='pfremembrancer',
            full_text=single_output,
            destination_address=account_address)
        hash_grab = coach_send.result['hash']
        full_url =f'https://livenet.xrpl.org/transactions/{hash_grab}/detailed'
        full_message=f"""{single_output}
        
        {full_url}"""
        return full_message

    def send_timeboxed_coaching_message(self, account_address = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        print('sending time boxed coaching message')
        est = pytz.timezone('America/New_York')
        current_time = datetime.datetime.now(est).time()
        if current_time.hour >= 6 and current_time.hour <= 20:
            ret = self.send_and_ret_coaching_message(account_address=account_address)
            return ret
        else:
            return ""