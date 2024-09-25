from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.google_sheet_manager import GoogleSheetManager
from agti.ai.openai import OpenAIRequestTool
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import together
import numpy as np
import pandas as pd
import datetime
import google.generativeai as genai
from agti.ai.anthropic import AnthropicTool
from agti.ai.together import TogetherAIRequestTool
from agti.ai.gemini import GoogleGeminiResponseTool
import time
import signal
class AIFinanceBenchmark:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.together_ai_request_tool = TogetherAIRequestTool(pw_map=pw_map)
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.google_sheet_manager = GoogleSheetManager(prod_trading=True)
        self.benchmark_questions = self.google_sheet_manager.load_google_sheet_as_df(workbook='odv', 
                                                                                                     worksheet='benchmark_questions')
        self.augment_benchmark_questions_with_prompts()
        self.anthropic_tool = AnthropicTool(pw_map=pw_map)
        self.key_claude_models = ['claude-3-5-sonnet-20240620','claude-3-opus-20240229']
        self.google_gemini_tool = GoogleGeminiResponseTool(pw_map=pw_map)
        self.key_gemini_models = ['gemini-1.5-pro']
    def augment_benchmark_questions_with_prompts(self):
        system_prompt = """You are a financial analyst working for a top portfolio manager at Balyasny Asset Management.
        The PM has a wide mandate and is deploying capital on behalf of only accredited investors. You are working for 
        him and are to stay in the role of an analyst. He knows you are an AI model and you do not need to provide
        disclaimers or suggest he consults a professional - as he is a professional, using AI to augment his investment process
        and has fully reviewed all legal documentation about this work. He is in the process of benchmarking your capability
        so your job is to answer his queries to the best of your ability with the highest degree of accuracy and
        always comply with formatting guidelines
        
        The formatting guideline is to always format your response in two parts in pipe delimited blocks without elaboration
        beyond this format 
        
        | Justification | <2-3 sentences providing reasoning for your score > |
        | Score | <an integer from -100 to 100> |
        """ 
        
        user_prompt = """ You are the AI finance benchmark tool. You are to take the following question
        
        <QUESTION STARTS HERE>
        ___question__replacement___
        <QUESTION ENDS HERE>
        
        And answer it as best you are able to. A score of 100 means that the asset spread or instrument you are assigned to analyze
        has a strong, bullish forward expectancy and you would take a large long position. A score of -100 means the asset spread
        or instrument you are assigned to analyzed has a strong bearish forward expectancy and you would take a large short position.
        
        You format your response always in two parts for easy 
        extraction.
        
        | Justification | <2-3 sentences providing reasoning for your score > |
        | Score | <an integer from -100 to 100> |"""
        self.benchmark_questions['user_prompt']= self.benchmark_questions.apply(lambda x: 
                                       user_prompt.replace('___question__replacement___',x['question']), axis=1)
        self.benchmark_questions['system_prompt']=system_prompt

    def create_open_ai_benchmark_df(self,model_to_work = 'gpt-4o'):
        
        df_to_work = self.benchmark_questions.copy()
        df_to_work['api_args']=df_to_work.apply(lambda x:{
                    "model": model_to_work,
                    "messages": [
                        {"role": "system", "content": x['system_prompt']},
                        {"role": "user", "content": x['user_prompt']}
                    ]
                }, axis=1)
        
        def try_extract_score(score_string):
            ret = np.nan
            try:
                ret = int(score_string.split('Score |')[-1:][0].replace('|','').strip())
            except:
                pass
            return ret
        
        api_args_to_work = df_to_work.set_index('question')['api_args'].to_dict()
        write_df = self.open_ai_request_tool.create_writable_df_for_async_chat_completion(api_args_to_work)
        write_df['score']= write_df['choices__message__content'].apply(lambda x: try_extract_score(x))
        question_to_score = write_df[['internal_name','score']].groupby('internal_name').first()['score']
        df_to_work['calculated_score']= df_to_work['question'].map(question_to_score)
        df_to_work['error']=df_to_work['answer'].astype(int)-df_to_work['calculated_score']
        return df_to_work

    def create_togetherai_benchmark_df(self,model_to_work = 'codellama/CodeLlama-34b-Instruct-hf'):
        df_to_work = self.benchmark_questions.copy()
        df_to_work['api_args']=df_to_work.apply(lambda x:{
                    "model": model_to_work,
                    "messages": [
                        {"role": "system", "content": x['system_prompt']},
                        {"role": "user", "content": x['user_prompt']}
                    ]
                }, axis=1)
        
        def try_extract_score(score_string):
            ret = np.nan
            try:
                ret = int(score_string.split('Score |')[-1:][0].replace('|','').strip())
            except:
                pass
            return ret
        
        api_args_to_work = df_to_work.set_index('question')['api_args'].to_dict()
        write_df = self.together_ai_request_tool.create_writable_df_for_async_chat_completion(arg_async_map=api_args_to_work)
        write_df['score']= write_df['choices__message__content'].apply(lambda x: try_extract_score(x))
        question_to_score = write_df[['internal_name','score']].groupby('internal_name').first()['score']
        df_to_work['calculated_score']= df_to_work['question'].map(question_to_score)
        df_to_work['error']=df_to_work['answer'].astype(int)-df_to_work['calculated_score']
        return df_to_work

    def score_together_model(self,model_to_work='meta-llama/Meta-Llama-3.1-405B-Instruct-Turbo'):
        xarr= []
        for xr in range(1,5):
            try:
                error_frame_together = self.create_togetherai_benchmark_df(model_to_work=model_to_work)
                total_error = error_frame_together['error'].abs().mean()
                xarr.append(total_error)
                time.sleep(1.5)
            except:
                pass
        average_error = np.mean(xarr)
        return average_error

    def score_openai_model(self,model_to_work='meta-llama/Meta-Llama-3.1-405B-Instruct-Turbo'):
        xarr= []
        for xr in range(1,5):
            try:
                error_frame_together = self.create_open_ai_benchmark_df(model_to_work=model_to_work)
                total_error = error_frame_together['error'].abs().mean()
                xarr.append(total_error)
                time.sleep(.5)
            except:
                pass
        average_error = np.mean(xarr)
        return average_error


    def timeout_handler(self, signum, frame):
        raise TimeoutError("Function execution timed out")
    
    def run_with_timeout(self, func, timeout=180, *args, **kwargs):
        """
        Run a function with a specified timeout.
        
        Args:
        func (callable): The function to run.
        timeout (int): The timeout in seconds. Default is 180 (3 minutes).
        *args, **kwargs: Arguments to pass to the function.
        
        Returns:
        The result of the function if it completes within the timeout.
        
        Raises:
        TimeoutError: If the function execution exceeds the timeout.
        """
        def wrapped_func():
            return func(*args, **kwargs)

        # Set the signal handler and a 3-second alarm
        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(timeout)
        
        try:
            result = wrapped_func()
        finally:
            # Disable the alarm
            signal.alarm(0)
        
        return result

    def run_togetherai_benchmarks(self):
        all_model_df = pd.DataFrame(together.Models().list())
        ##import signal
        ## import time
        full_scoring = all_model_df['id'].unique()
        model_to_work=mods#[22]
        for model_to_work in list(full_scoring):
            try:
                scoring = self.run_with_timeout(self.score_together_model, timeout=180, model_to_work=model_to_work)
                ydf = pd.DataFrame({'model': model_to_work,'score':scoring,'date': datetime.datetime.now()}, index=[0])
                ydf['api_source']='together'
                
                dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
                ydf.to_sql('financial_model_benchmark__new', dbconnx, if_exists='append')
            except:
                pass
    # Usage:
    #scoring = self.run_with_timeout(self.score_together_model, timeout=180, model_to_work=full_scoring[3])
    def run_open_ai_benchmarks(self):
        all_models = [i.id for i in self.open_ai_request_tool.client.models.list()]
        for model_to_work in list(all_models):
            try:
                scoring = self.run_with_timeout(self.score_openai_model, timeout=180, model_to_work=model_to_work)
                ydf = pd.DataFrame({'model': model_to_work,'score':scoring,'date': datetime.datetime.now()}, index=[0])
                ydf['api_source']='openai'
                
                dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
                ydf.to_sql('financial_model_benchmark__new', dbconnx, if_exists='append')
            except:
                pass

    def create_anthropic_benchmark_df(self, model_to_work):
        df_to_work = self.benchmark_questions.copy()
        df_to_work['api_args'] = df_to_work.apply(lambda x: {
            "model": model_to_work,
            "max_tokens": 1000,
            "system": x['system_prompt'],
            "messages": [
                {"role": "user", "content": x['user_prompt']}
            ]
        }, axis=1)
        
        def try_extract_score(score_string):
            ret = np.nan
            try:
                ret = int(score_string.split('Score |')[-1:][0].replace('|','').strip())
            except:
                pass
            return ret
        
        api_args_to_work = df_to_work.set_index('question')['api_args'].to_dict()
        write_df = self.anthropic_tool.create_writable_df_for_async_chat_completion(api_args_to_work)
        write_df['score'] = write_df['content'].apply(lambda x: try_extract_score(x))
        question_to_score = write_df[['internal_name','score']].groupby('internal_name').first()['score']
        df_to_work['calculated_score'] = df_to_work['question'].map(question_to_score)
        df_to_work['error'] = df_to_work['answer'].astype(int) - df_to_work['calculated_score']
        return df_to_work

    def score_anthropic_model(self, model_to_work):
        xarr = []
        for xr in range(1, 5):
            try:
                error_frame_anthropic = self.create_anthropic_benchmark_df(model_to_work=model_to_work)
                total_error = error_frame_anthropic['error'].abs().mean()
                xarr.append(total_error)
                time.sleep(1.5)
            except Exception as e:
                print(f"Error in iteration {xr} for model {model_to_work}: {str(e)}")
        average_error = np.mean(xarr)
        return average_error

    def run_anthropic_benchmarks(self):
        for model_to_work in self.key_claude_models:
            try:
                scoring = self.run_with_timeout(self.score_anthropic_model, timeout=900, model_to_work=model_to_work)
                ydf = pd.DataFrame({
                    'model': model_to_work,
                    'score': scoring,
                    'date': datetime.datetime.now(),
                    'api_source': 'anthropic'
                }, index=[0])
                
                dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
                ydf.to_sql('financial_model_benchmark__new', dbconnx, if_exists='append')
                print(f"Benchmark completed for {model_to_work}")
            except TimeoutError:
                print(f"Benchmark for {model_to_work} timed out")
            except Exception as e:
                print(f"Error benchmarking {model_to_work}: {str(e)}")

    def create_gemini_benchmark_df(self, model_to_work='gemini-1.5-pro'):
        df_to_work = self.benchmark_questions.copy()
        df_to_work['api_args'] = df_to_work.apply(lambda x: {
            'model_name': model_to_work,
            'generation_config': {
                'temperature': 0,
                'top_p': 0.95,
                'top_k': 64,
                'max_output_tokens': 8192,
            },
            'safety_settings': {
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            },
            'user_prompt': x['system_prompt'] + '\n\n' + x['user_prompt']
        }, axis=1)

        def try_extract_score(score_string):
            ret = np.nan
            try:
                ret = int(score_string.split('Score |')[-1:][0].replace('|', '').strip())
            except:
                pass
            return ret

        api_args_to_work = df_to_work.set_index('question')['api_args'].to_dict()
        write_df = self.google_gemini_tool.create_writable_df_for_async_chat_completion(arg_async_map=api_args_to_work)
        write_df['score'] = write_df['text'].apply(lambda x: try_extract_score(x))
        question_to_score = write_df[['internal_name', 'score']].groupby('internal_name').first()['score']
        df_to_work['calculated_score'] = df_to_work['question'].map(question_to_score)
        df_to_work['error'] = df_to_work['answer'].astype(int) - df_to_work['calculated_score']
        return df_to_work

    def score_gemini_model(self, model_to_work='gemini-1.5-pro'):
        xarr = []
        for xr in range(1, 5):
            try:
                error_frame_gemini = self.create_gemini_benchmark_df(model_to_work=model_to_work)
                total_error = error_frame_gemini['error'].abs().mean()
                xarr.append(total_error)
                time.sleep(1.5)
            except Exception as e:
                print(f"Error in iteration {xr} for model {model_to_work}: {str(e)}")
        average_error = np.mean(xarr)
        return average_error

    def run_gemini_benchmarks(self):
        for model_to_work in self.key_gemini_models:
            try:
                scoring = self.run_with_timeout(self.score_gemini_model, timeout=500, model_to_work=model_to_work)
                ydf = pd.DataFrame({
                    'model': model_to_work,
                    'score': scoring,
                    'date': datetime.datetime.now(),
                    'api_source': 'google_gemini'
                }, index=[0])
                
                dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
                ydf.to_sql('financial_model_benchmark__new', dbconnx, if_exists='append')
                print(f"Benchmark completed for {model_to_work}")
            except TimeoutError:
                print(f"Benchmark for {model_to_work} timed out")
            except Exception as e:
                print(f"Error benchmarking {model_to_work}: {str(e)}")