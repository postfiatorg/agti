import json
import os
from IPython.display import display, Javascript
from agti.ai.openai import OpenAIRequestTool
from IPython.core.getipython import get_ipython
from IPython import get_ipython
import pandas as pd
from agti.ai.anthropic import AnthropicTool
class NotebookAITool:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=self.pw_map)
        self.default_open_ai_model ='gpt-4o'
        self.anthropic_tool = AnthropicTool(pw_map=pw_map)
    def get_notebook_contents(self):
        # Get the notebook path
        notebook_path = os.path.abspath(".")
        
        # Find the notebook file
        for file in os.listdir(notebook_path):
            if file.endswith(".ipynb"):
                notebook_file = file
                break
        else:
            raise FileNotFoundError("No notebook file found in the current directory.")
        
        # Read the notebook file
        with open(os.path.join(notebook_path, notebook_file), "r", encoding="utf-8") as f:
            notebook_contents = json.load(f)
        
        # Convert the notebook contents to a JSON string
        #notebook_json = json.dumps(notebook_contents, indent=4)
        
        return notebook_contents
    def convert_notebook_to_pretty_string(self):
        x=self.get_notebook_contents()
        raw_notebook = pd.DataFrame(x['cells'])[['source','outputs']].copy()
        def try_join(arr=[]):
            ret =''
            try:
                ret =''.join(arr)
            except:
                pass
            return ret
        raw_notebook['source_string']=raw_notebook['source'].apply(lambda x:try_join(x))
        raw_notebook['output_string']=raw_notebook['outputs'].apply(lambda x: str(x))
        str_creator = raw_notebook[['source_string','output_string']].copy()
        full_con_string = ''
        for cell in list(str_creator.index):
            
            cell_constructor = str(cell)+"""
"""
            input_constructor = str_creator.loc[cell]['source_string']+"""
    
OUTPUT:
"""
            output_constructor = str_creator.loc[cell]['output_string']+"""
__________________________
"""
            full_constructor = cell_constructor+input_constructor+output_constructor 
            full_con_string = full_con_string+full_constructor
            #raw_notebook['output_string']=raw_notebook['outputs'].apply(lambda x:''.join(x))
        return full_con_string

    def construct_notebook_api_arg(self, user_input):
        notebook_content=self.convert_notebook_to_pretty_string()[-200_000:]
        system_prompt = """ You are the world's premier python coding expert designed to work inside of ipython Notebooks. 
    You are given a full Notebook Input log. The user will include mark up in the notebook with comments preceded by 
    ## COMMENT on where he wants you to focus to augment your output
    
    Here are some rules for your engagement
    1. When you output things it is code that can be directly pasted into an ipython notebook. That means if you provide comments you
    have to put ## in front of them to explain your work. 
    2. The code you should return should always work and you should be terse and provide minimal explanation outside
    of what is neccesary for the user to understand your 
    3. You do not need to preface your code with '''python - just output it with the assumption the user will add it into 
    a new cell in the notebook or print it out. You similarly do not need to end it with stuff like ```
    """ 
        user_prompt=f"""The user has asked help with this:
USER INPUT STARTS HERE
{user_input}
USER INPUT ENDS HERE 

    Here is the notebook content that you are to reference when addressing the user's input
    NOTEBOOK CONTENT STARTS HERE
___
    {notebook_content}
___

Here are some guidelines
1. please review the notebook for ##COMMENT tags to ensure you're paying attention to what
the user wants you to focus on
2. The code you generate will be pasted into the next cell so make sure that your output can be pasted into a cell. 
3. If you include explanation make sure to comment them appropriately and include doc strings  
4. It is best if you keep your commentary terse and include the relevant information in your doc strings 
The user doesn't need explanations he needs results. Make sure to get the code right and without errors.
5. The user might refer to errors in the notebook - which will be denoted in output cells 

For your output ONLY RETURN WHAT THE USER ASKS FOR IN THE USER INPUT - you reference other things
in the notebook but use them as supplements, do not respond to them
    """
        api_args = api_args = {
            "model": self.default_open_ai_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature":0
        }
        return api_args
    def open_ai_help(self,user_input):
        xarg = self.construct_notebook_api_arg(user_input)
        df=self.open_ai_request_tool.create_writable_df_for_chat_completion(api_args=xarg)
        code = list(df['choices__message__content'])[0].replace('```python','').replace('```','')
        shell = get_ipython()
        shell.set_next_input(code, replace=False)
    def ai_help(self,user_input):
        notebook_content=self.convert_notebook_to_pretty_string()[-200_000:]
        system_prompt = """ You are the world's premier python coding expert designed to work inside of ipython Notebooks. 
You are given a full Notebook Input log. The user will include mark up in the notebook with comments preceded by 
## COMMENT on where he wants you to focus to augment your output

Here are some rules for your engagement
1. When you output things it is code that can be directly pasted into an ipython notebook. That means if you provide comments you
have to put ## in front of them to explain your work. 
2. The code you should return should always work and you should be terse and provide minimal explanation outside
of what is neccesary for the user to understand your 
3. You do not need to preface your code with '''python - just output it with the assumption the user will add it into 
a new cell in the notebook or print it out. You similarly do not need to end it with stuff like ```
""" 
        user_prompt=f"""The user has asked help with this:
USER INPUT STARTS HERE
{user_input}
USER INPUT ENDS HERE 

    Here is the notebook content that you are to reference when addressing the user's input
    NOTEBOOK CONTENT STARTS HERE
___
    {notebook_content}
___

Here are some guidelines
1. please review the notebook for ##COMMENT tags to ensure you're paying attention to what
the user wants you to focus on
2. The code you generate will be pasted into the next cell so make sure that your output can be pasted into a cell. 
3. If you include explanation make sure to comment them appropriately and include doc strings  
4. It is best if you keep your commentary terse and include the relevant information in your doc strings 
The user doesn't need explanations he needs results. Make sure to get the code right and without errors.
5. The user might refer to errors in the notebook - which will be denoted in output cells 

For your output ONLY RETURN WHAT THE USER ASKS FOR IN THE USER INPUT - you reference other things
in the notebook but use them as supplements, do not respond to them
    """
        op_df = self.anthropic_tool.generate_claude_dataframe( model='claude-3-5-sonnet-20240620',
            max_tokens=3000,
            temperature=0,
            system_prompt=system_prompt,
            user_prompt=user_prompt
        )
        code = list(op_df['text_response'])[0].replace('```python','').replace('```','')
        shell = get_ipython()
        shell.set_next_input(code, replace=False)
        