import os
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

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
        genai.configure(api_key=self.pw_map['google_gemini'])
        
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
        genai.configure(api_key=self.pw_map['google_gemini'])
        
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
        genai.configure(api_key=self.pw_map['google_gemini'])
        
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

