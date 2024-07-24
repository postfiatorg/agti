from agti.ai.openai import OpenAIRequestTool
import datetime
from agti.utilities.db_manager import DBConnectionManager
class G10FXConceptSimulator:
    def __init__(self,pw_map):
        self.open_ai_request_tool = OpenAIRequestTool(pw_map=pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.default_model = 'gpt-4o'
        self.fx_map = {'EUR':'The Eurozone',
                   'SEK':'Sweden',
                  'NOK':'Norway',
                  'USD':'United States',
                  'GBP':'United Kingdom',
                  'CHF':'Switzerland',
                  'AUD': 'Australia',
                  'CAD':'Canada',
                  'NZD': 'New Zealand',
                   'JPY':'Japan'}
    def assemble_concept_simulator(self,fx_to_work = 'CAD', concept = 'implement sweeping Diversity Equity and Inclusion Laws'):
        fx_map = self.fx_map
        country_name = fx_map[fx_to_work]
        api_arg_make = {
                "model": self.default_model,
                "messages": [
                    {"role": "system", "content": """You are an expert on AGI development. You always follow instructions and output
                    your final output as specified"""},
                    {"role": "user", "content": f"""What is the likelihood that {country_name} is the first (or next) G10 economy to {concept}
First output your reasoning. Second output your percentage guess as a decimal. For example a score of 15% would be .15
A percentage likelihood of {concept} of 20% would be .2 etc. 

example output 1:

The {country_name} is very unlikely the next G10 economy to {concept} for < insert reasons >
thus we assign it a 10% chance. 

| {concept} | .1 |

example output 2:

The {country_name} is very unlikely the next G10 economy to {concept} for < insert reasons >
thus we assign it a 90% chance. 

| {concept} | .9 |

Follow these examples, provide your reasoning as instructed then output the following

| {concept} | <decimal representing the % likelihood such as .5> |
                    """ }
                ]}
        return api_arg_make

    def output_full_scoring_frame(self, runs, concept):
        """ example: runs = 10, concept = 'develop AGI' """ 
        xdf = pd.DataFrame(list(self.fx_map.keys()))
        xdf.columns=['fx']
        xdf['api_args']= xdf['fx'].apply(lambda x: self.assemble_concept_simulator(fx_to_work=x,
            concept=concept))
        xdf['concept']=concept
        #xdf['index_copy']=xdf.index
        #xdf['unique_string']=xdf['concept'].astype(str)+'_'+xdf['index_copy'].astype(str)+'_'+xdf['fx'].astype(str)
        # Number of copies
        n = runs
        
        # Create n copies using list comprehension
        copies = pd.concat([xdf.copy() for _ in range(n)])
        copies=copies.reset_index()
        copies['index_copy']=copies.index
        copies['unique_string']=copies['concept'].astype(str)+'_'+copies['index_copy'].astype(str)+'_'+copies['fx'].astype(str)
        dict_to_work = copies.set_index('unique_string')['api_args'].to_dict()
        def split_dict_chunks(input_dict, chunk_size):
            """
            Splits a dictionary into chunks of a specified size.
        
            Parameters:
            input_dict (dict): The dictionary to be split.
            chunk_size (int): The size of each chunk.
        
            Returns:
            List[dict]: A list of dictionaries, each representing a chunk.
            """
            # Convert dictionary items to a list of tuples
            items = list(input_dict.items())
            
            # Split the list of items into chunks
            chunks = [dict(items[i:i + chunk_size]) for i in range(0, len(items), chunk_size)]
            
            return chunks
        all_dicts = split_dict_chunks(input_dict=dict_to_work, chunk_size=10)
        yarr=[]
        
        for block_to_work in all_dicts:
            try:
                async_block = self.open_ai_request_tool.create_writable_df_for_async_chat_completion(block_to_work)
                yarr.append(async_block)
            except:
                print('failed')
                pass
        raw_extraction = pd.concat(yarr).groupby('internal_name').last()[['choices__message__content']]
        pattern = r'\|([^|]+)\|$'
        
        def extract_value(text):
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
            else:
                return None
        xdf = open_ai_request.create_writable_df_for_chat_completion(api_args=api_arg_make)
        raw_extraction['value']=pd.to_numeric(raw_extraction['choices__message__content'].apply(lambda x: extract_value(x)),errors='coerce')
        raw_extraction=raw_extraction.reset_index()
        raw_extraction['fx']=raw_extraction['internal_name'].apply(lambda x: x.split('_')[-1:][0])
        return raw_extraction

    def generate_score_df(self,concept_score = 'develop AGI', runs=20):
        concept_df = self.output_full_scoring_frame(runs=runs, concept=concept_score)
        concept_df = concept_df[['fx','value']].groupby('fx').mean().sort_values('value')
        concept_df.columns=['value']
        concept_df['concept_name']=concept_score
        concept_df['date_write']=datetime.datetime.now()
        return concept_df

    def calculate_overall_score(self, score_map, runs=20):
        """score_map = {
    'develop AGI': 1,
    'have a fiscal or monetary crisis': -1,
    'thrive in escalating global conflict and threat environment': 1,
    'thrive under a Trump Presidency and global move to the Far Right': 1}

        result = self.calculate_overall_score(score_map) 


        """
        # Generate score DataFrames for each concept
        concept_dfs = {concept: self.generate_score_df(concept_score=concept, runs=runs) 
                       for concept in score_map.keys()}
        
        # Concatenate all DataFrames
        score_frame = pd.concat(concept_dfs.values()).reset_index().groupby(['fx', 'concept_name']).last()['value'].unstack()
        
        # Calculate z-scores
        z_weighting = (score_frame - score_frame.mean()) / score_frame.std()
        
        # Calculate overall score using the provided weights
        overall_score = sum(z_weighting[concept] * weight for concept, weight in score_map.items())
        
        # Create a DataFrame with the overall score
        score = pd.DataFrame(overall_score, columns=['overall_score']).sort_values('overall_score')
        
        # Add the overall score to the score_frame
        score_frame['score'] = score['overall_score']
        score_frame = score_frame.sort_values('score')
        score_frame['score_map'] = score_map.to_json()
        score_map['number_of_runs']= runs
        score_map['run_time']= datetime.datetime.now()
        return score_frame