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
        The user should feel like he's talking to himself'''
        user_prompt = f''' Here is the original message to be converted
        <ORIGINAL MESSAGE STARTS HERE>
        {persuasive_message}
        <ORIGINAL MESSAGE ENDS HERE>
        
        here is how the user talks
        <USER STYLE>
        {users_last_messages}
        <USER STYLE ENDS HERE>
        
        Convert the original message into the user's own voice. Be unsparing - it really needs to sound like the user. 
        output your response in pipe delimited format as follows. Do not grovel. Do not use slang. Do not use profanity. 
        Though you should mirror the user as much as possible you should keep your authority in tact and avoid using
        colloquialisms or try to sound hip. Do not use exclamation points unless the user uses exclamation points. 
        Do not use "we" or mission driven statements unless the user does. Do not use any type of phrasing or tone the user
        does not himself use - if at all possible. 
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