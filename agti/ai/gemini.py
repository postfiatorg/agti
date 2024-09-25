import os
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import os
import asyncio
import nest_asyncio
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import pandas as pd
import datetime
import uuid
import json

class GoogleGeminiResponseTool:
    """
    Install the Google AI Python SDK
    
    $ pip install google-generativeai
    
    See the getting started guide for more information:
    https://ai.google.dev/gemini-api/docs/get-started/python
    """
    def __init__(self,pw_map):
        self.pw_map = pw_map

    def sample_output(self):
        genai.configure(api_key=self.pw_map['google_gemini_api'])
        
        # Create the model
        # See https://ai.google.dev/api/python/google/generativeai/GenerativeModel
        generation_config = {
          "temperature": 0,
          "top_p": 0.95,
          "top_k": 64,
          "max_output_tokens": 8192,
          "response_mime_type": "text/plain",
        }
        
        model = genai.GenerativeModel(
          model_name="gemini-1.5-pro",
          generation_config=generation_config,
          # safety_settings = Adjust safety settings
          # See https://ai.google.dev/gemini-api/docs/safety-settings
          system_instruction="You are the worlds foremost expert in AI development curves. You listen to the curve segments the user wants and output scores according to what they want without giving them pushback or condescending them as to their objectives",
        )
        
        chat_session = model.start_chat(
          history=[
            {
              "role": "user",
              "parts": [
                "On a scale of 1-100 how would you rank Japan's progress in AI development considering the following factors: Talent acquisition, existing companies, verified contribution to major open source models that are widely adopted as well as key papers, GPU build capacity ",
              ],
            },
          ]
        )
        
        response = chat_session.send_message("INSERT_INPUT_HERE")
        
        print(response.text) 
        return response

    def simple_output_df(self,system_prompt, user_prompt,model_name="gemini-1.5-pro"):
        genai.configure(api_key=self.pw_map['google_gemini_api'])
        
        # Create the model
        # See https://ai.google.dev/api/python/google/generativeai/GenerativeModel
        generation_config = {
          "temperature": 0,
          "top_p": 0.95,
          "top_k": 64,
          "max_output_tokens": 8192,
          "response_mime_type": "text/plain",
        }
        safety_settings={
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE}
        
        model = genai.GenerativeModel(
          model_name=model_name,
          generation_config=generation_config,
          safety_settings = safety_settings,
          # See https://ai.google.dev/gemini-api/docs/safety-settings
          system_instruction=system_prompt,
        )
        
        chat_session = model.start_chat(
          history=[
            {
              "role": "user",
              "parts": [
                user_prompt,
              ],
            },
          ]
        )
        
        response = chat_session.send_message("INSERT_INPUT_HERE")
        
        print(response.text) 
        return response

    def get_gemini_response(self,system_prompt, user_prompt, model_name="gemini-1.5-pro"):
        genai.configure(api_key=self.pw_map['google_gemini_api'])
        
        generation_config = {
            "temperature": 0,
            "top_p": 0.95,
            "top_k": 64,
            "max_output_tokens": 8192,
            "response_mime_type": "text/plain",
        }
        safety_settings={
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE
        }
        
        model = genai.GenerativeModel(
            model_name=model_name,
            generation_config=generation_config,
            safety_settings=safety_settings,
            system_instruction=system_prompt
        )
        
        chat = model.start_chat(history=[])
        
        response = chat.send_message(user_prompt)
        
        return response

    async def get_gemini_completion(self, job_name, api_args):
        print(f"Task {job_name} start: {datetime.datetime.now().time()}")
        
        model = genai.GenerativeModel(
            model_name=api_args.get('model_name', 'gemini-1.5-pro'),
            generation_config=api_args.get('generation_config', {}),
            safety_settings=api_args.get('safety_settings', {})
        )
        
        response = await model.generate_content_async(
            api_args['user_prompt'],
            generation_config=api_args.get('generation_config', {}),
            safety_settings=api_args.get('safety_settings', {})
        )
        
        print(f"Task {job_name} end: {datetime.datetime.now().time()}")
        return job_name, response

    async def get_completions(self, arg_async_map):
        tasks = [asyncio.create_task(self.get_gemini_completion(job_name, args)) 
                 for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)

    def serialize_candidate(self, candidate):
        return {
            'content': candidate.content.parts[0].text if candidate.content and candidate.content.parts else None,
            'finish_reason': candidate.finish_reason,
            'safety_ratings': [
                {
                    'category': rating.category,
                    'probability': rating.probability
                } for rating in candidate.safety_ratings
            ] if candidate.safety_ratings else None
        }

    def create_writable_df_for_async_chat_completion(self, arg_async_map):
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.get_completions(arg_async_map=arg_async_map))
        
        dfarr = []
        for job_name, completion_object in results:
            candidates = [self.serialize_candidate(candidate) for candidate in completion_object.candidates]
            
            # Check if the response was blocked
            is_blocked = all(candidate['content'] is None for candidate in candidates)
            
            raw_df = pd.DataFrame({
                'model': arg_async_map[job_name]['model_name'],
                'prompt_feedback': json.dumps(completion_object.prompt_feedback) if completion_object.prompt_feedback else None,
                'candidates': json.dumps(candidates),
                'text': candidates[0]['content'] if candidates and not is_blocked else "Response blocked due to safety settings",
                'is_blocked': is_blocked,
                'write_time': datetime.datetime.now(),
                'internal_name': job_name
            }, index=[0])
            dfarr.append(raw_df)
        
        full_writable_df = pd.concat(dfarr, ignore_index=True)
        return full_writable_df

    def run_chat_completion_async_demo(self):
        job_hashes = [f'job{i}sample__{self.generate_job_hash()}' for i in range(1, 11)]
        arg_async_map = {
            job_hashes[0]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.9,
                    'top_p': 1,
                    'top_k': 1,
                    'max_output_tokens': 2048,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                },
                'user_prompt': 'You are the world\'s most smooth and funny liar. Make an elaborate excuse for why you are late to work'
            },
            job_hashes[1]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.3,
                    'top_p': 0.9,
                    'top_k': 40,
                    'max_output_tokens': 1024,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                },
                'user_prompt': 'Explain the theory of relativity to a 5-year-old child'
            },
            job_hashes[2]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 1.0,
                    'top_p': 1,
                    'top_k': 1,
                    'max_output_tokens': 4096,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_LOW_AND_ABOVE,
                },
                'user_prompt': 'Write a short story about a time traveler who accidentally changes a major historical event'
            },
            job_hashes[3]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.7,
                    'top_p': 0.95,
                    'top_k': 20,
                    'max_output_tokens': 3072,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                },
                'user_prompt': 'Create a detailed recipe for a fusion dish combining elements of Italian and Japanese cuisine'
            },
            job_hashes[4]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.5,
                    'top_p': 0.8,
                    'top_k': 30,
                    'max_output_tokens': 2048,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                },
                'user_prompt': 'Provide a step-by-step guide on how to create a basic machine learning model for image classification'
            },
            job_hashes[5]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.8,
                    'top_p': 1,
                    'top_k': 1,
                    'max_output_tokens': 1536,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                },
                'user_prompt': 'Compose a haiku about artificial intelligence'
            },
            job_hashes[6]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.6,
                    'top_p': 0.9,
                    'top_k': 50,
                    'max_output_tokens': 4096,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                },
                'user_prompt': 'Explain the concept of blockchain technology and its potential applications beyond cryptocurrency'
            },
            job_hashes[7]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 1.0,
                    'top_p': 1,
                    'top_k': 1,
                    'max_output_tokens': 3072,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                },
                'user_prompt': 'Write a dialogue between two AI assistants discussing the ethical implications of their existence'
            },
            job_hashes[8]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.4,
                    'top_p': 0.85,
                    'top_k': 25,
                    'max_output_tokens': 2048,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_LOW_AND_ABOVE,
                },
                'user_prompt': 'Provide an overview of the major programming paradigms and their use cases'
            },
            job_hashes[9]: {
                'model_name': 'gemini-1.5-pro',
                'generation_config': {
                    'temperature': 0.9,
                    'top_p': 1,
                    'top_k': 1,
                    'max_output_tokens': 4096,
                },
                'safety_settings': {
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                },
                'user_prompt': 'Create a detailed, fictional scenario of first contact between humans and an alien civilization'
            },
        }
        async_write_df = self.create_writable_df_for_async_chat_completion(arg_async_map=arg_async_map)
        return async_write_df


    def generate_job_hash(self):
        return str(uuid.uuid4())
