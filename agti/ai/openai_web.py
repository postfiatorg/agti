import pandas as pd
import sqlalchemy
import datetime
import uuid
import json
import asyncio
import nest_asyncio
from openai import OpenAI, AsyncOpenAI
from agti.utilities.db_manager import DBConnectionManager
from asyncio import Semaphore
from tqdm import tqdm
import time

class OpenAIResponsesTool:
    """
    Tool for interacting with the OpenAI Responses API.
    
    The Responses API is OpenAI's newest core API and an agentic API primitive, combining 
    the simplicity of Chat Completions with the ability to do more agentic tasks. It supports:
    
    - Text generation
    - Image inputs
    - Web search
    - File search
    - Computer use
    - Structured outputs
    - Function calling
    
    Example usage:
    ```python
    from ftr.ai.openai_responses import OpenAIResponsesTool
    
    # Initialize the tool with your password map
    responses_tool = OpenAIResponsesTool(pw_map={'openai': 'your-api-key'})
    
    # Simple text completion
    response = responses_tool.create_text_response("Tell me a three sentence bedtime story about a unicorn.")
    print(response.output_text)
    ```
    """
    
    def __init__(self, pw_map):
        """
        Initialize the OpenAIResponsesTool with the provided password map.
        
        Args:
            pw_map (dict): Dictionary containing API keys, including 'openai' key.
        """
        self.pw_map = pw_map
        self.client = OpenAI(api_key=self.pw_map['openai'])
        self.async_client = AsyncOpenAI(api_key=self.pw_map['openai'])
        primary_model_string = '''
        The primary model for OpenAI Responses API is currently gpt-4o'''
        print(primary_model_string)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)

    def create_text_response(self, text_input, model="gpt-4o", instructions=None, temperature=1.0, stream=False):
        """
        Create a simple text response using the Responses API.
        
        Args:
            text_input (str): The text input to send to the model.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            stream (bool, optional): Whether to stream the response, defaults to False.
            
        Returns:
            The response from the OpenAI Responses API.
        """
        api_args = {
            "model": model,
            "input": text_input,
            "temperature": temperature,
            "stream": stream
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        response = self.client.responses.create(**api_args)
        return response
    
    def create_message_response(self, message_input, model="gpt-4o", instructions=None, temperature=1.0, stream=False):
        """
        Create a response using messages format (similar to Chat Completions).
        
        Args:
            message_input (list): List of message objects with role and content.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            stream (bool, optional): Whether to stream the response, defaults to False.
            
        Returns:
            The response from the OpenAI Responses API.
        """
        api_args = {
            "model": model,
            "input": message_input,
            "temperature": temperature,
            "stream": stream
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        response = self.client.responses.create(**api_args)
        return response
    
    def create_image_response(self, text_query, image_url, model="gpt-4o", instructions=None, temperature=1.0):
        """
        Create a response with both text and image inputs.
        
        Args:
            text_query (str): The text query to ask about the image.
            image_url (str): URL of the image to analyze.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            
        Returns:
            The response from the OpenAI Responses API.
        """
        input_content = [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": text_query},
                    {"type": "input_image", "image_url": image_url}
                ]
            }
        ]
        
        api_args = {
            "model": model,
            "input": input_content,
            "temperature": temperature
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        response = self.client.responses.create(**api_args)
        return response
    
    def create_web_search_response(self, query, model="gpt-4o", instructions=None, temperature=1.0):
        """
        Create a response that uses web search capabilities.
        
        Args:
            query (str): The search query.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            
        Returns:
            The response from the OpenAI Responses API with web search results.
        """
        api_args = {
            "model": model,
            "tools": [{"type": "web_search_preview"}],
            "input": query,
            "temperature": temperature
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        response = self.client.responses.create(**api_args)
        return response
    
    def create_file_search_response(self, query, vector_store_ids, max_num_results=20, model="gpt-4o", instructions=None):
        """
        Create a response that uses file search capabilities.
        
        Args:
            query (str): The search query for the files.
            vector_store_ids (list): List of vector store IDs to search in.
            max_num_results (int, optional): Maximum number of results to return, defaults to 20.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            
        Returns:
            The response from the OpenAI Responses API with file search results.
        """
        api_args = {
            "model": model,
            "tools": [{
                "type": "file_search",
                "vector_store_ids": vector_store_ids,
                "max_num_results": max_num_results
            }],
            "input": query
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        response = self.client.responses.create(**api_args)
        return response
    
    def create_structured_output_response(self, query, structure_schema, model="gpt-4o", instructions=None):
        """
        Create a response with structured JSON output according to a schema.
        
        Args:
            query (str): The query to process.
            structure_schema (dict): JSON schema defining the structure of the output.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            
        Returns:
            The response from the OpenAI Responses API with structured output.
        """
        api_args = {
            "model": model,
            "input": query,
            "text": {
                "format": {
                    "type": "json_object",
                    "schema": structure_schema
                }
            }
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        response = self.client.responses.create(**api_args)
        return response
    
    def generate_job_hash(self):
        """
        Generate a unique job hash for async operations.
        
        Returns:
            str: A unique UUID string.
        """
        return str(uuid.uuid4())
    
    def create_writable_df_for_response(self, response):
        """
        Create a DataFrame from a Responses API response for database storage.
        
        Args:
            response: The response object from the Responses API.
            
        Returns:
            pd.DataFrame: A DataFrame containing the processed response data.
        """
        response_dict = response.model_dump()
        raw_df = pd.DataFrame([response_dict])
        
        # Extract output content if available
        if 'output' in response_dict and response_dict['output']:
            for i, output_item in enumerate(response_dict['output']):
                if output_item['type'] == 'message':
                    for j, content_item in enumerate(output_item.get('content', [])):
                        if content_item['type'] == 'output_text':
                            raw_df[f'output_{i}_content_{j}_text'] = content_item['text']
                            
                            # Process annotations if present
                            if 'annotations' in content_item and content_item['annotations']:
                                raw_df[f'output_{i}_content_{j}_annotations'] = json.dumps(content_item['annotations'])
        
        # Add metadata
        raw_df['write_time'] = datetime.datetime.now()
        raw_df['response_json'] = json.dumps(response_dict)
        
        return raw_df
    
    def query_response_and_write_to_db(self, response):
        """
        Write a Responses API response to the database.
        
        Args:
            response: The response object from the Responses API.
            
        Returns:
            pd.DataFrame: The DataFrame that was written to the database.
        """
        writable_df = self.create_writable_df_for_response(response)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='collective')
        writable_df.to_sql('openai_responses', dbconnx, if_exists='append', index=False)
        dbconnx.dispose()
        return writable_df
    
    def output_all_responses(self):
        """
        Output all responses from the database.
        
        Returns:
            pd.DataFrame: A DataFrame containing all response records.
        """
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='collective')
        all_responses = pd.read_sql('openai_responses', dbconnx)
        dbconnx.dispose()
        return all_responses
    
    async def get_responses(self, arg_async_map):
        """
        Get responses asynchronously for a map of jobs.
        
        Args:
            arg_async_map (dict): A dictionary mapping job names to API arguments.
            
        Returns:
            list: A list of (job_name, response) tuples.
        """
        async def task_with_debug(job_name, api_args):
            print(f"Task {job_name} start: {datetime.datetime.now().time()}")
            response = await self.async_client.responses.create(**api_args)
            print(f"Task {job_name} end: {datetime.datetime.now().time()}")
            return job_name, response

        tasks = [asyncio.create_task(task_with_debug(job_name, args)) for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)
    
    def create_writable_df_for_async_responses(self, arg_async_map):
        """
        Create a DataFrame for async responses results.
        
        Args:
            arg_async_map (dict): A dictionary mapping job names to API arguments.
            
        Returns:
            pd.DataFrame: A DataFrame containing all response data.
        """
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.get_responses(arg_async_map=arg_async_map))
        
        dfarr = []
        for job_name, response in results:
            writable_df = self.create_writable_df_for_response(response)
            writable_df['internal_name'] = job_name
            dfarr.append(writable_df)
            
        if dfarr:
            full_writable_df = pd.concat(dfarr)
            return full_writable_df
        return pd.DataFrame()
    
    def query_responses_async_and_write_to_db(self, arg_async_map):
        """
        Query responses asynchronously and write results to database.
        
        Args:
            arg_async_map (dict): A dictionary mapping job names to API arguments.
            
        Returns:
            pd.DataFrame: The DataFrame that was written to the database.
        """
        async_write_df = self.create_writable_df_for_async_responses(arg_async_map=arg_async_map)
        
        if not async_write_df.empty:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='collective')
            async_write_df.to_sql('openai_responses', dbconnx, if_exists='append', index=False)
            dbconnx.dispose()
            
        return async_write_df
    
    def run_responses_demo(self):
        """
        Demo function to showcase the Responses API capabilities.
        
        Returns:
            dict: A dictionary containing various demo responses.
        """
        demo_results = {}
        
        # Text response
        print("Running text response demo...")
        text_response = self.create_text_response(
            "Tell me a three sentence bedtime story about a unicorn."
        )
        demo_results['text_response'] = text_response
        
        # Image response
        print("Running image response demo...")
        image_response = self.create_image_response(
            "What is in this image?",
            "https://upload.wikimedia.org/wikipedia/commons/thumb/d/dd/Gfp-wisconsin-madison-the-nature-boardwalk.jpg/2560px-Gfp-wisconsin-madison-the-nature-boardwalk.jpg"
        )
        demo_results['image_response'] = image_response
        
        # Web search
        print("Running web search demo...")
        web_search_response = self.create_web_search_response(
            "What was a positive news story from today?"
        )
        demo_results['web_search_response'] = web_search_response
        
        # Structured output
        print("Running structured output demo...")
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "interests": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["name", "age", "interests"]
        }
        
        structured_response = self.create_structured_output_response(
            "Extract information about John, who is 30 years old and enjoys hiking, reading, and photography.",
            schema
        )
        demo_results['structured_response'] = structured_response
        
        return demo_results
    
    def retrieve_response(self, response_id):
        """
        Retrieve a specific response by ID.
        
        Args:
            response_id (str): The ID of the response to retrieve.
            
        Returns:
            The retrieved response object.
        """
        response = self.client.responses.retrieve(response_id)
        return response
    
    def get_clean_text(self, response):
        """
        Extract only the text content from response, skipping tool use and tool results.
        
        Args:
            response: The response object from the OpenAI Responses API.
            
        Returns:
            str: Clean text content joined from all text blocks.
        """
        text_parts = []
        if hasattr(response, 'output') and response.output:
            for output_item in response.output:
                if output_item.type == "message":
                    for content_item in output_item.content:
                        if content_item.type == "output_text":
                            text_parts.append(content_item.text)
        return "".join(text_parts)
    
    def create_web_search_args_template(self, query, model="gpt-4o", instructions=None, temperature=1.0):
        """
        Create API arguments template for web search requests.
        
        Args:
            query (str): The search query or question.
            model (str): The model to use, defaults to "gpt-4o".
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            
        Returns:
            dict: API arguments dictionary for use with async methods.
        """
        api_args = {
            "model": model,
            "tools": [{"type": "web_search_preview"}],
            "input": query,
            "temperature": temperature
        }
        
        if instructions:
            api_args["instructions"] = instructions
            
        return api_args
    
    def create_bulk_web_search_map(self, queries_dict, model="gpt-4o", instructions=None, temperature=1.0):
        """
        Create a bulk async map for multiple web search queries.
        
        Args:
            queries_dict (dict): Dictionary mapping job names to query strings.
            model (str): The model to use for all requests.
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            
        Returns:
            dict: Async map ready for use with existing async methods.
        """
        async_map = {}
        for job_name, query in queries_dict.items():
            async_map[job_name] = self.create_web_search_args_template(query, model, instructions, temperature)
        return async_map
    
    async def get_simple_responses(self, arg_async_map):
        """
        Get responses asynchronously and return only clean text.
        
        Args:
            arg_async_map (dict): A dictionary mapping job names to API arguments.
            
        Returns:
            list: A list of (job_name, clean_text) tuples.
        """
        async def simple_task(job_name, api_args):
            print(f"Task {job_name} start: {datetime.datetime.now().time()}")
            response = await self.async_client.responses.create(**api_args)
            clean_text = self.get_clean_text(response)
            print(f"Task {job_name} end: {datetime.datetime.now().time()}")
            return job_name, clean_text

        tasks = [asyncio.create_task(simple_task(job_name, args)) for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)
    
    async def get_simple_responses_with_semaphore(self, arg_async_map, semaphore):
        """
        Get responses asynchronously with rate limiting using semaphore.
        
        Args:
            arg_async_map (dict): A dictionary mapping job names to API arguments.
            semaphore (asyncio.Semaphore): Semaphore for rate limiting.
            
        Returns:
            list: A list of (job_name, clean_text) tuples.
        """
        async def rate_limited_task(job_name, api_args, sem):
            async with sem:
                try:
                    response = await self.async_client.responses.create(**api_args)
                    clean_text = self.get_clean_text(response)
                    return job_name, clean_text
                except Exception as e:
                    print(f"Error processing {job_name}: {str(e)}")
                    return job_name, f"Error: {str(e)}"

        tasks = [asyncio.create_task(rate_limited_task(job_name, args, semaphore)) 
                for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)
    
    def execute_bulk_web_search_simple(self, queries_dict, model="gpt-4o", instructions=None, temperature=1.0):
        """
        Execute bulk web search and return a simple, clean dataframe.
        
        Args:
            queries_dict (dict): Dictionary mapping query IDs to query strings.
            model (str): The model to use for all requests.
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            
        Returns:
            pd.DataFrame: Simple dataframe with columns: query_id, query, clean_response
        """
        print(f"Starting bulk web search for {len(queries_dict)} queries...")
        
        # Create async map
        async_map = self.create_bulk_web_search_map(queries_dict, model, instructions, temperature)
        
        # Execute async requests
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.get_simple_responses(arg_async_map=async_map))
        
        print(f"Completed async requests, processing {len(results)} responses...")
        
        # Build simple dataframe
        simple_results = []
        for job_name, clean_text in results:
            simple_results.append({
                'query_id': job_name,
                'query': queries_dict[job_name],
                'clean_response': clean_text
            })
            print(f"Processed {job_name}: {len(clean_text)} characters")
        
        df = pd.DataFrame(simple_results)
        print(f"Created clean dataframe with shape: {df.shape}")
        return df
    
    def execute_bulk_web_search_rate_constrained(self, queries_dict, model="gpt-4o", 
                                                batch_size=10, instructions=None, 
                                                temperature=1.0, delay_between_batches=1.0):
        """
        Execute bulk web search with rate constraints and progress tracking.
        
        Args:
            queries_dict (dict): Dictionary mapping query IDs to query strings.
            model (str): The model to use for all requests.
            batch_size (int, optional): Number of queries per batch, defaults to 10.
            instructions (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            delay_between_batches (float, optional): Delay in seconds between batches, defaults to 1.0.
            
        Returns:
            pd.DataFrame: Combined dataframe with columns: query_id, query, clean_response
        """
        print(f"Starting rate-constrained bulk web search for {len(queries_dict)} queries...")
        print(f"Batch size: {batch_size}, Delay between batches: {delay_between_batches}s")
        
        # Convert dict items to list for batching
        query_items = list(queries_dict.items())
        total_queries = len(query_items)
        
        # Initialize results storage
        all_results = []
        
        # Process in batches with progress bar
        with tqdm(total=total_queries, desc="Processing queries") as pbar:
            for i in range(0, total_queries, batch_size):
                batch_start_time = time.time()
                
                # Get current batch
                batch_items = query_items[i:i + batch_size]
                batch_dict = dict(batch_items)
                
                print(f"\nProcessing batch {i//batch_size + 1} ({len(batch_items)} queries)...")
                
                # Create async map for this batch
                async_map = self.create_bulk_web_search_map(batch_dict, model, instructions, temperature)
                
                # Execute batch with semaphore for additional rate limiting
                nest_asyncio.apply()
                loop = asyncio.get_event_loop()
                
                # Create semaphore to limit concurrent requests within batch
                semaphore = asyncio.Semaphore(batch_size)
                
                try:
                    results = loop.run_until_complete(
                        self.get_simple_responses_with_semaphore(arg_async_map=async_map, semaphore=semaphore)
                    )
                    
                    # Process batch results
                    for job_name, clean_text in results:
                        all_results.append({
                            'query_id': job_name,
                            'query': batch_dict[job_name],
                            'clean_response': clean_text
                        })
                    
                    # Update progress bar
                    pbar.update(len(batch_items))
                    
                except Exception as e:
                    print(f"Error processing batch: {str(e)}")
                    # Add error entries for failed batch
                    for job_name, query in batch_dict.items():
                        all_results.append({
                            'query_id': job_name,
                            'query': query,
                            'clean_response': f"Batch error: {str(e)}"
                        })
                    pbar.update(len(batch_items))
                
                # Calculate batch processing time
                batch_elapsed = time.time() - batch_start_time
                print(f"Batch completed in {batch_elapsed:.2f} seconds")
                
                # Add delay between batches (except for last batch)
                if i + batch_size < total_queries and delay_between_batches > 0:
                    print(f"Waiting {delay_between_batches} seconds before next batch...")
                    time.sleep(delay_between_batches)
        
        # Create final dataframe
        df = pd.DataFrame(all_results)
        print(f"\nCompleted all queries. Final dataframe shape: {df.shape}")
        
        # Summary statistics
        successful_responses = df[~df['clean_response'].str.startswith('Error:', na=False)].shape[0]
        failed_responses = df[df['clean_response'].str.startswith('Error:', na=False)].shape[0]
        
        print(f"Successfully processed: {successful_responses}/{total_queries}")
        if failed_responses > 0:
            print(f"Failed queries: {failed_responses}")
        
        return df

# Example usage
if __name__ == "__main__":
    from ftr.utilities.settings import PasswordMapLoader
    
    password_map_loader = PasswordMapLoader()
    responses_tool = OpenAIResponsesTool(pw_map=password_map_loader.pw_map)
    
    # Run the demo
    demo_results = responses_tool.run_responses_demo()
    
    # Print out the text response
    print("\nText Response Example:")
    for item in demo_results['text_response'].output:
        if item.type == "message":
            for content in item.content:
                if content.type == "output_text":
                    print(content.text)
