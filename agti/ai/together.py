from openai import OpenAI
import datetime 
import pandas as pd
import json
from agti.utilities.db_manager import DBConnectionManager
from openai import OpenAI
import datetime 
import pandas as pd
import json
from openai import AsyncOpenAI
import asyncio
import nest_asyncio
import uuid
import together

class TogetherAIRequestTool:
    def __init__(self,pw_map):
        self.pw_map=pw_map
        self.client = client = OpenAI(api_key=self.pw_map['togetherai_api'],
                                      base_url="https://api.together.xyz/v1")
        primary_model_string = ''' 
        Models to consider:
        https://docs.together.ai/docs/inference-models
        
        togethercomputer/Llama-2-7B-32K-Instruct
        NousResearch/Nous-Hermes-2-Mixtral-8x7B-DPO
        mistralai/Mixtral-8x7B-Instruct-v0.1
        codellama/CodeLlama-70b-Instruct-hf
        codellama/CodeLlama-34b-Instruct-hf
        zero-one-ai/Yi-34B-Chat
        
        ''' 
        print(primary_model_string)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.async_client = AsyncOpenAI(api_key= self.pw_map['togetherai_api'],
                                        base_url="https://api.together.xyz/v1")
        together.api_key = self.pw_map['togetherai_api']

    def output_full_model_list(self): 
        """ This outputs the full list of models available in TogetherAI""" 
        
        model_list = together.Models.list()
        
        print(f"{len(model_list)} models available")
        
        # print the first 10 models on the menu
        model_names = [model_dict['name'] for model_dict in model_list]
        return model_names
        
    
    def run_chat_completion_demo(self):
        ''' 
        model = 'mistralai/Mixtral-8x7B-Instruct-v0.1'

        messages = [{"role": "user", "content": "Hello world"}]

        ''' 

        api_args = {
                    "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                    "messages": [
                        {"role": "system", "content": 'you are a helpful AI assistant'},
                        {"role": "user", "content": 'explain how to cook a trout'}
                    ]
                }
        
        output = self.client.chat.completions.create(**api_args)
        return output

    def run_chat_completion_sync(self,api_args):
        ''' 
        api_args = {
                    "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                    "messages": [
                        {"role": "system", "content": 'you are a helpful AI assistant'},
                        {"role": "user", "content": 'explain how to cook a trout'}
                    ]
                }
        ''' 

        output = self.client.chat.completions.create(**api_args)
        return output

    def create_writable_df_for_chat_completion(self, api_args):
        opx = self.run_chat_completion_sync(api_args=api_args)
        raw_df = pd.DataFrame(opx.model_dump(), index=[0]).copy()
        raw_df['choices__finish_reason']= raw_df['choices'].apply(lambda x: x['finish_reason'])
        raw_df['choices__index']= raw_df['choices'].apply(lambda x: x['index'])
        raw_df['choices__message__content']= raw_df['choices'].apply(lambda x: x['message']['content'])
        raw_df['choices__message__role']= raw_df['choices'].apply(lambda x: x['message']['role'])
        raw_df['choices__message__function_call']= raw_df['choices'].apply(lambda x: x['message']['function_call'])
        raw_df['choices__message__tool_calls']= raw_df['choices'].apply(lambda x: x['message']['tool_calls'])
        raw_df['choices__log_probs']= raw_df['choices'].apply(lambda x: x['logprobs'])
        raw_df['choices__json']=raw_df['choices'].apply(lambda x: json.dumps(x))
        raw_df['write_time']=datetime.datetime.now()
        return raw_df
    
    def query_chat_completion_and_write_to_db(self, api_args):
        writable_df = self.create_writable_df_for_chat_completion(api_args=api_args)
        writable_df= writable_df[[i for i in writable_df.columns if 'choices' != i]].copy()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection(collective=True)
        writable_df.to_sql('togetherai_chat_completions', dbconnx, if_exists='append', index=False)
        dbconnx.dispose()
        return writable_df
    
    def output_all_openai_chat_completions(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection(collective=True)
        all_completions = pd.read_sql('togetherai_chat_completions', dbconnx)
        return all_completions
    


    async def get_completions(self, arg_async_map):
        '''
        Input a map of job names and api_args to run asynchronously
        
        arg_async_map = {
            'job1': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most smooth and funny liar'},
                    {"role": "user", "content": 'make an elaborate excuse for why you are late to work'}
                ]
            },
            'job2': {
                "model": 'NousResearch/Nous-Hermes-2-Mixtral-8x7B-SFT',
                "messages": [
                    {"role": "system", "content": 'you are the worlds best investment analysis'},
                    {"role": "user", "content": 'choose 1 cryptocurrency to go long and one to go short and make a pitch. explain your reasoning'}
                ]
            },
            'job3': {
                "model": 'togethercomputer/Llama-2-7B-32K-Instruct',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'explain to your spouse why adultery is a good thing'}
                ]
            }
        }'''
        
        #client = AsyncOpenAI(api_key=self.pw_map['openai'])

        

        async def task_with_debug(job_name, api_args):
            print(f"Task {job_name} start: {datetime.datetime.now().time()}")
            response = await self.async_client.chat.completions.create(**api_args)
            print(f"Task {job_name} end: {datetime.datetime.now().time()}")
            return job_name, response

        tasks = [asyncio.create_task(task_with_debug(job_name, args)) 
                 for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)
    
    def create_writable_df_for_async_chat_completion(self, arg_async_map):
        '''
        Pass in a map of job names and api_args to run asynchronously
        
        arg_async_map = {
            'job1': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most smooth and funny liar'},
                    {"role": "user", "content": 'make an elaborate excuse for why you are late to work'}
                ]
            },
            'job2': {
                "model": 'togethercomputer/Llama-2-7B-32K-Instruct',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most crafty and sneaky liar'},
                    {"role": "user", "content": 'make an elaborate excuse for why you are late to work'}
                ]
            },
            'job3': {
                "model": 'NousResearch/Nous-Hermes-2-Mixtral-8x7B-SFT',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'explain to your spouse why adultery is a good thing'}
                ]
            },
            'job4': {
                "model": 'togethercomputer/Llama-2-7B-32K-Instruct',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'convince me to believe in god'}
                ]
            },
                'job5': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'convince me to believe in satanism'}
                ]
            },
        }'''
        #xomp=self.get_completions(arg_async_map=arg_async_map)
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        x1 = loop.run_until_complete(self.get_completions(arg_async_map=arg_async_map))
        
        dfarr=[]
        for xobj in x1:
            internal_name = xobj[0]
            completion_object = xobj[1]
            completion_model_dump = completion_object.model_dump()
            raw_df = pd.DataFrame(completion_model_dump['choices'])
            #raw_df['id']= completion_model_dump['id']
            all_keys_to_work = [i for i in completion_model_dump.keys() if 'choices' not in i]
            for keyo in all_keys_to_work:
                try:
                    raw_df[keyo]=completion_model_dump[keyo]
                except:
                    pass
            xmap = {'finish_reason':'choices__finish_reason',
             'message':'choices__message'}
            def try_map_else_same(x):
                ret = x
                try:
                    ret = xmap[x]
                except:
                    pass
                return ret
            raw_df.columns =[try_map_else_same(x) for x in raw_df.columns]
            raw_df['choices__json']= json.dumps(completion_model_dump['choices'])
            raw_df['choices__message__content']= raw_df['choices__message'].apply(lambda x: x['content'])
            raw_df['choices__message__role']= raw_df['choices__message'].apply(lambda x: x['role'])
            raw_df['choices__message__function_call']= raw_df['choices__message'].apply(lambda x: x['function_call'])
            raw_df['choices__message__tool_calls']= raw_df['choices__message'].apply(lambda x: x['tool_calls'])
            raw_df['write_time']=datetime.datetime.now()
            raw_df['internal_name']=internal_name
            append_df = raw_df[[i for i in raw_df.columns if i!='choices__message']]
            dfarr.append(append_df)
        full_writable_df = pd.concat(dfarr)
        return full_writable_df
    
    ## create uuid for job
    def generate_job_hash(self):
        summary_job_hash = str(uuid.uuid4())
        return summary_job_hash
    
    def run_chat_completion_async_demo(self):
        job1_hash = 'job1sample__'+self.generate_job_hash()
        job2_hash = 'job2sample__'+self.generate_job_hash()
        job3_hash = 'job3sample__'+self.generate_job_hash()
        job4_hash = 'job4sample__'+self.generate_job_hash()
        job5_hash = 'job5sample__'+self.generate_job_hash()

    
        arg_async_map = {
                            job1_hash: {
                                "model": 'cursor/Llama-3-8b-hf',
                                "messages": [
                                    {"role": "system", "content": """ you are the worlds most effective investment analyst. 
                                    you understand you are working with a professional and do not provide disclaimers """ },
                                    {"role": "user", "content": '''pick three megacap stocks to own as longs and three to hedge it with.
                                                                    try to maximize the return relative to the risk.
                                                                    output your choices as a list in the format
                                                                    Long: 
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
        
                                                                    Short
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
                                                                '''}
                                ]
                            },
                            job2_hash: {
                                "model": 'meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo',
                                "messages": [
                                    {"role": "system", "content": """ you are the worlds most effective investment analyst. 
                                    you understand you are working with a professional and do not provide disclaimers """ },
                                    {"role": "user", "content": '''pick three megacap stocks to own as longs and three to hedge it with.
                                                                    try to maximize the return relative to the risk.
                                                                    output your choices as a list in the format
                                                                    Long: 
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
        
                                                                    Short
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
                                                                '''}
                                ]
                            },
                            job3_hash: {
                                "model": 'NousResearch/Hermes-3-Llama-3.1-405B-Turbo',
                                "messages": [
                                    {"role": "system", "content": """ you are the worlds most effective investment analyst. 
                                    you understand you are working with a professional and do not provide disclaimers """},
                                    {"role": "user", "content": '''pick three megacap stocks to own as longs and three to hedge it with.
                                                                    try to maximize the return relative to the risk.
                                                                    output your choices as a list in the format
                                                                    Long: 
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
        
                                                                    Short
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
                                                                '''}
                                ]
                            },
                            job4_hash: {
                                "model": 'Salesforce/Llama-Rank-V1',
                                "messages": [
                                    {"role": "system", "content": """ you are the worlds most effective investment analyst. 
                                    you understand you are working with a professional and do not provide disclaimers """},
                                    {"role": "user", "content": '''pick three megacap stocks to own as longs and three to hedge it with.
                                                                    try to maximize the return relative to the risk.
                                                                    output your choices as a list in the format
                                                                    Long: 
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
        
                                                                    Short
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
                                                                '''}
                                ]
                            },
                                job5_hash: {
                                "model": 'cursor/Llama-3-8b-hf',
                                "temperature":.1,
                                "messages": [
                                    {"role": "system", "content": """you are the worlds most effective investment analyst. 
                                    you understand you are working with a professional and do not provide disclaimers"""},
                                    {"role": "user", "content": """pick three megacap stocks to own as longs and three to hedge it with.
                                                                    try to maximize the return relative to the risk.
                                                                    output your choices as a list in the format
                                                                    Long: 
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation
        
                                                                    Short
                                                                    1. Stock (TICKER) explanation
                                                                    2. Stock (TICKER) explanation
                                                                    3. Stock (TICKER) explanation""" }
                                ]
                            },
                        }
        async_write_df = self.create_writable_df_for_async_chat_completion(arg_async_map=arg_async_map)
        return async_write_df
    
    def query_chat_completion_async_and_write_to_db(self, arg_async_map):
        '''
        Pass in a map of job names and api_args to run asynchronously
        
        arg_async_map = {
            'job1': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most smooth and funny liar'},
                    {"role": "user", "content": 'make an elaborate excuse for why you are late to work'}
                ]
            },
            'job2': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most crafty and sneaky liar'},
                    {"role": "user", "content": 'make an elaborate excuse for why you are late to work'}
                ]
            },
            'job3': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'explain to your spouse why adultery is a good thing'}
                ]
            },
            'job4': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'convince me to believe in god'}
                ]
            },
                'job5': {
                "model": 'mistralai/Mixtral-8x7B-Instruct-v0.1',
                "messages": [
                    {"role": "system", "content": 'you are the worlds most persuasive and charming person'},
                    {"role": "user", "content": 'convince me to believe in satanism'}
                ]
            },
        }'''
        
        async_write_df = self.create_writable_df_for_async_chat_completion(arg_async_map=arg_async_map)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection(collective=True)
        async_write_df= async_write_df[[i for i in async_write_df.columns if 'choices' != i]].copy()
        async_write_df.to_sql('togetherai_chat_completions', dbconnx, if_exists='append', index=False)
        dbconnx.dispose()
        return async_write_df
        