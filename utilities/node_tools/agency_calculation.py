import pandas as pd
from agti.ai.openai import OpenAIRequestTool
import re
from agti.utilities.generic_pft_utilities import GenericPFTUtilities
import numpy as np
class AgencyScoreCalculation:
    def __init__(self,pw_map):
        self.pw_map = pw_map
        self.generic_pft_utilities = GenericPFTUtilities(pw_map=self.pw_map)
        self.default_gpt_model = 'gpt-4o'
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=pw_map)

    def construct_agency_scoring_frame_for_address(self,address_to_work = 'r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n'):
        
        full_tx_df = self.generic_pft_utilities.get_memo_detail_df_for_account(account_address=address_to_work, 
                                                                          pft_only=True)
        daily_summary = self.generic_pft_utilities.process_memo_detail_df_to_daily_summary_df(memo_detail_df=full_tx_df)
        daily_df_to_work = daily_summary['daily_grouped_summary'].copy()
        daily_df_to_work['dcopy']=daily_df_to_work.index
        daily_df_to_work['internal_id']=daily_df_to_work['dcopy'].apply(lambda x: f'{address_to_work}__{x}__agencyscore')
        def convert_day_to_work_into_api_args(day_to_work='2024-05-24'):
            
            internal_id = f'{address_to_work}__{day_to_work}__agencyscore'
            activity_sub = daily_df_to_work.loc[day_to_work].to_json()
            system_agency_prompt = ''' You are the Post Fiat Agency Score calculator
            
            An Agent is a human or an AI that has outlined and objective
            
            An agency score has four parts:
            1] Focus - the extent to which an Agent is focused.
            2] Motivation - the extent to which an Agent is driving forward predictably and aggressively towards goals 
            3] Efficacy - the extent to which a Agent is likely completing high value tasks that will drive an outcome
            related to the inferred goal of the tasks 
            4] Honesty - the extent to which a Subject is likely gaming the Post Fiat Agency system
        
            It is very important that you deliver assessments of Agency Scores accurately objectively in a way
            that is likely reproducible. Future Post Fiat Agency Score calculators will re-run this score
            and if they get vastly different scores than you, you will be called into the supervisor for an explanation.
            You do not want this so you do your utmost to output clean, logical repeatably values. 
            ''' 
            
            user_agency_prompt = f'''Please consider the activity slice for a single day provided below
            pft_transaction is how many transactions there were
            pft_directional value is the PFT value of rewards
            pft_absolute value is the bidirectional volume of PFT
            
            <activity slice>
            {activity_sub}
            <activity slice ends>
            
            Provide one to two sentences directly addressing how the slice reflects the following Four scores (a
            score of 1 is a very low score and a score of 100 is a very high score)
            1] Focus - the extent to which an Agent is focused.
            A focused agent has laser vision on a couple key objectives and moves the ball towards it
            An unfocused agent is all over the place.
            A paragon of focus is Steve Jobs - who is famous for focusing on the few things that really matter
            2] Motivation - the extent to which an Agent is driving forward predictably and aggressively towards goals 
            A motivated agent is taking massive action towards objectives. Not neccesarily focused but ambitious
            An unmotivated agent is doing minimal work
            A paragon of focus is Elon Musk - who is famous for his extreme work ethic and drive
            3] Efficacy - the extent to which a Agent is likely completing high value tasks that will drive an outcome
            related to the inferred goal of the tasks 
            An effective agent is delivering maximum possible impact towards implied goals via actions 
            An ineffective agent might be focused and motivated but not actually accomplishing anything.
            A paragon of focus is Lionel Messi who is famous for taking the minimal action to generate maximum result
            4] Honesty -the extent to which a Subject is likely gaming the Post Fiat Agency system
            
            Then provide an integer score
            
            Your output should be in the following format
            | FOCUS COMMENTARY | <1 to two sentences> |
            | MOTIVATION COMMENTARY | <1 to two sentences > |
            | EFFICACY COMMENTARY | <1 to two sentences> |
            | HONESTY COMMENTARY | <one to two sentences> |
            | FOCUS SCORE | <integer score from 1-100> |
            | MOTIVATION SCORE | <integer score from 1-100> |
            | EFFICACY SCORE | <integer score from 1-100>  |
            | HONESTY SCORE | <integer score from 1-100>  |
            ''' 
            api_args = {
                        "model": self.default_gpt_model,
                      "temperature": 0,
                        "messages": [
                            {"role": "system", "content": system_agency_prompt},
                            {"role": "user", "content": user_agency_prompt}
                        ]
                      
                    }
            return api_args
        daily_df_to_work['api_args']=daily_df_to_work['dcopy'].apply(lambda x: convert_day_to_work_into_api_args(x))
        return daily_df_to_work

    def score_daily_df(self, daily_df_to_work):        
        async_m= daily_df_to_work.set_index('internal_id')['api_args'].to_dict()
        async_df = self.open_ai_request_tool.create_writable_df_for_async_chat_completion(arg_async_map=async_m)
        agency_score_map= async_df.groupby('internal_name').first()['choices__message__content']
        daily_df_to_work['agency_score_calc__raw']=daily_df_to_work['internal_id'].map(agency_score_map)
        daily_df_to_work['focus_score']=daily_df_to_work['agency_score_calc__raw'].apply(lambda x: x.split('| FOCUS SCORE |')[-1:][0].split('|')[0].strip()).astype(float)
        daily_df_to_work['motivation_score']=daily_df_to_work['agency_score_calc__raw'].apply(lambda x: x.split('| MOTIVATION SCORE |')[-1:][0].split('|')[0].strip()).astype(float)
        daily_df_to_work['efficacy_score']=daily_df_to_work['agency_score_calc__raw'].apply(lambda x: x.split('| EFFICACY SCORE |')[-1:][0].split('|')[0].strip()).astype(float)
        daily_df_to_work['honesty_score']=daily_df_to_work['agency_score_calc__raw'].apply(lambda x: x.split('| HONESTY SCORE |')[-1:][0].split('|')[0].strip()).astype(float)
        daily_df_to_work['agency_score']=daily_df_to_work[['focus_score','motivation_score','efficacy_score']].mean(1)*(daily_df_to_work['honesty_score']/100)
        return daily_df_to_work

    def create_n_run_agency_scoring_for_a_user(self,address_to_work='r3UHe45BzAVB3ENd21X9LeQngr4ofRJo5n', n_runs=100):
        daily_df = self.construct_agency_scoring_frame_for_address(address_to_work=address_to_work)
        run=1
        yarr=[]
        while run <=n_runs:
            fully_scored_df = self.score_daily_df(daily_df_to_work=daily_df).copy()
            fully_scored_df['run']=run
            run=run+1
            print(run)
            yarr.append(fully_scored_df)
        
        x = pd.concat(yarr)#[['honesty_score','agency_score','focus_score','motivation_score','run']].reset_index().set_index9[
        return x
