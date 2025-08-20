import pandas as pd
import sqlalchemy
import datetime
import uuid
import json
import asyncio
import nest_asyncio
from anthropic import Anthropic, AsyncAnthropic
from agti.utilities.db_manager import DBConnectionManager
from asyncio import Semaphore
from tqdm import tqdm
import time

class AnthropicWebSearchTool:
    """
    Tool for interacting with the Anthropic Claude API with web search capabilities.
    
    This tool provides access to Claude's web search functionality, allowing Claude to:
    - Access current information from across the web
    - Perform multi-hop searches with progressive queries
    - Provide citations for web-sourced responses
    - Control domain access through allow/block lists
    
    Example usage:
    ```python
    from ftr.ai.anthropic_web_search import AnthropicWebSearchTool
    
    # Initialize the tool with your password map
    search_tool = AnthropicWebSearchTool(pw_map={'anthropic': 'your-api-key'})
    
    # Simple web search query
    response = search_tool.create_web_search_response("What are the latest developments in AI?")
    print(response.content[0].text)
    ```
    """
    
    def __init__(self, pw_map):
        """
        Initialize the AnthropicWebSearchTool with the provided password map.
        
        Args:
            pw_map (dict): Dictionary containing API keys, including 'anthropic' key.
        """
        self.pw_map = pw_map
        self.client = Anthropic(api_key=self.pw_map['anthropic'])
        self.async_client = AsyncAnthropic(api_key=self.pw_map['anthropic'])
        primary_model_string = '''
        The primary model for Anthropic Claude with web search is claude-3-7-sonnet-latest'''
        print(primary_model_string)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)

    def create_web_search_response(self, query, model="claude-3-7-sonnet-latest", system_prompt=None, 
                                 temperature=1.0, max_tokens=4096, max_uses=5, 
                                 allowed_domains=None, blocked_domains=None):
        """
        Create a response using Claude's web search capabilities.
        
        Args:
            query (str): The search query or question to ask Claude.
            model (str): The model to use, defaults to "claude-3-7-sonnet-latest".
            system_prompt (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            max_tokens (int, optional): Maximum tokens in response, defaults to 4096.
            max_uses (int, optional): Maximum number of web searches Claude can perform, defaults to 5.
            allowed_domains (list, optional): List of domains Claude is allowed to search.
            blocked_domains (list, optional): List of domains Claude is blocked from searching.
            
        Returns:
            The response from the Anthropic Claude API with web search results.
        """
        # Use the correct web search tool type based on your working example
        web_search_tool = {
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": max_uses
        }
        
        # Add domain controls if specified
        if allowed_domains or blocked_domains:
            web_search_tool["web_search"] = {}
            if allowed_domains:
                web_search_tool["web_search"]["allowed_domains"] = allowed_domains
            if blocked_domains:
                web_search_tool["web_search"]["blocked_domains"] = blocked_domains
        
        api_args = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "tools": [web_search_tool],
            "messages": [
                {
                    "role": "user",
                    "content": query
                }
            ]
        }
        
        if system_prompt:
            api_args["system"] = system_prompt
            
        response = self.client.messages.create(**api_args)
        return response
    
    def create_simple_response(self, query, model="claude-3-7-sonnet-latest", system_prompt=None, 
                             temperature=1.0, max_tokens=4096):
        """
        Create a simple response without web search capabilities.
        
        Args:
            query (str): The query to send to Claude.
            model (str): The model to use, defaults to "claude-3-7-sonnet-latest".
            system_prompt (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            max_tokens (int, optional): Maximum tokens in response, defaults to 4096.
            
        Returns:
            The response from the Anthropic Claude API.
        """
        api_args = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [
                {
                    "role": "user",
                    "content": query
                }
            ]
        }
        
        if system_prompt:
            api_args["system"] = system_prompt
            
        response = self.client.messages.create(**api_args)
        return response
    
    def create_conversation_response(self, messages, model="claude-3-7-sonnet-latest", 
                                   system_prompt=None, temperature=1.0, max_tokens=4096,
                                   enable_web_search=False, max_search_uses=5):
        """
        Create a response for multi-turn conversations.
        
        Args:
            messages (list): List of message objects with role and content.
            model (str): The model to use, defaults to "claude-3-7-sonnet-latest".
            system_prompt (str, optional): System message for the model.
            temperature (float, optional): Sampling temperature, defaults to 1.0.
            max_tokens (int, optional): Maximum tokens in response, defaults to 4096.
            enable_web_search (bool, optional): Whether to enable web search, defaults to False.
            max_search_uses (int, optional): Maximum web searches if enabled, defaults to 5.
            
        Returns:
            The response from the Anthropic Claude API.
        """
        api_args = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": messages
        }
        
        if system_prompt:
            api_args["system"] = system_prompt
            
        if enable_web_search:
            api_args["tools"] = [{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": max_search_uses
            }]
            
        response = self.client.messages.create(**api_args)
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
        Create a DataFrame from an Anthropic Claude response for database storage.
        
        Args:
            response: The response object from the Anthropic Claude API.
            
        Returns:
            pd.DataFrame: A DataFrame containing the processed response data.
        """
        response_dict = response.model_dump()
        raw_df = pd.DataFrame([response_dict])
        
        # Extract content from response
        if 'content' in response_dict and response_dict['content']:
            for i, content_item in enumerate(response_dict['content']):
                if content_item['type'] == 'text':
                    raw_df[f'content_{i}_text'] = content_item['text']
                elif content_item['type'] == 'tool_use':
                    raw_df[f'content_{i}_tool_name'] = content_item.get('name', '')
                    raw_df[f'content_{i}_tool_input'] = json.dumps(content_item.get('input', {}))
        
        # Extract usage information
        if 'usage' in response_dict and response_dict['usage']:
            raw_df['input_tokens'] = response_dict['usage'].get('input_tokens', 0)
            raw_df['output_tokens'] = response_dict['usage'].get('output_tokens', 0)
        
        # Add metadata
        raw_df['write_time'] = datetime.datetime.now()
        raw_df['response_json'] = json.dumps(response_dict)
        
        return raw_df
    
    def query_response_and_write_to_db(self, response):
        """
        Write an Anthropic Claude response to the database.
        
        Args:
            response: The response object from the Anthropic Claude API.
            
        Returns:
            pd.DataFrame: The DataFrame that was written to the database.
        """
        writable_df = self.create_writable_df_for_response(response)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='collective')
        writable_df.to_sql('anthropic_web_search_responses', dbconnx, if_exists='append', index=False)
        dbconnx.dispose()
        return writable_df
    
    def output_all_responses(self):
        """
        Output all responses from the database.
        
        Returns:
            pd.DataFrame: A DataFrame containing all response records.
        """
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='collective')
        all_responses = pd.read_sql('anthropic_web_search_responses', dbconnx)
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
            response = await self.async_client.messages.create(**api_args)
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
            async_write_df.to_sql('anthropic_web_search_responses', dbconnx, if_exists='append', index=False)
            dbconnx.dispose()
            
        return async_write_df
    
    def run_web_search_demo(self):
        """
        Demo function to showcase the Anthropic Claude web search capabilities.
        
        Returns:
            dict: A dictionary containing various demo responses.
        """
        demo_results = {}
        
        # Simple text response
        print("Running simple text response demo...")
        text_response = self.create_simple_response(
            "Tell me a three sentence bedtime story about a unicorn."
        )
        demo_results['text_response'] = text_response
        
        # Web search response
        print("Running web search demo...")
        web_search_response = self.create_web_search_response(
            "What are the latest developments in artificial intelligence this week?"
        )
        demo_results['web_search_response'] = web_search_response
        
        # Financial news search
        print("Running financial news search demo...")
        financial_search_response = self.create_web_search_response(
            "What are the latest market trends and economic indicators today?",
            system_prompt="You are a financial analyst. Provide concise analysis with citations."
        )
        demo_results['financial_search_response'] = financial_search_response
        
        # Domain-restricted search
        print("Running domain-restricted search demo...")
        restricted_search_response = self.create_web_search_response(
            "What are the latest developments in technology?",
            allowed_domains=["reuters.com", "bloomberg.com", "wsj.com"]
        )
        demo_results['restricted_search_response'] = restricted_search_response
        
        return demo_results
    
    def extract_text_from_response(self, response):
        """
        Extract text content from a Claude response, handling web search responses.
        
        Args:
            response: The response object from the Anthropic Claude API.
            
        Returns:
            str: The extracted text content.
        """
        text_content = ""
        for content_block in response.content:
            if content_block.type == "text":
                text_content += content_block.text
            # Skip tool use and tool result blocks - just extract final text
        return text_content
    
    def extract_citations_from_response(self, response):
        """
        Extract citations from a web search response.
        
        Args:
            response: The response object from the Anthropic Claude API.
            
        Returns:
            list: A list of citation dictionaries if available.
        """
        citations = []
        response_dict = response.model_dump()
        
        # Look for citations in the response content
        if 'content' in response_dict:
            for content_item in response_dict['content']:
                if content_item.get('type') == 'text' and 'citations' in content_item:
                    citations.extend(content_item['citations'])
        
        return citations
    
    def get_clean_text(self, response):
        """
        Extract only the text content from response, skipping tool use and tool results.
        
        Args:
            response: The response object from the Anthropic Claude API.
            
        Returns:
            str: Clean text content joined from all text blocks.
        """
        text_parts = []
        for content_block in response.content:
            if content_block.type == "text":
                text_parts.append(content_block.text)
        return "".join(text_parts)
    
    def create_web_search_args_template(self, query, model, max_tokens=5000, max_uses=5):
        """
        Create API arguments template for web search requests with any model.
        
        Args:
            query (str): The search query or question to ask Claude.
            model (str): The model to use for the request.
            max_tokens (int, optional): Maximum tokens in response, defaults to 5000.
            max_uses (int, optional): Maximum number of web searches Claude can perform, defaults to 5.
            
        Returns:
            dict: API arguments dictionary for use with async methods.
        """
        return {
            "model": model,
            "max_tokens": max_tokens,
            "messages": [
                {
                    "role": "user",
                    "content": query
                }
            ],
            "tools": [{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": max_uses
            }]
        }
    
    def create_bulk_web_search_map(self, queries_dict, model, max_tokens=5000, max_uses=5):
        """
        Create a bulk async map for multiple web search queries with specified model.
        
        Args:
            queries_dict (dict): Dictionary mapping job names to query strings.
            model (str): The model to use for all requests.
            max_tokens (int, optional): Maximum tokens in response, defaults to 5000.
            max_uses (int, optional): Maximum number of web searches Claude can perform, defaults to 5.
            
        Returns:
            dict: Async map ready for use with existing async methods.
        """
        async_map = {}
        for job_name, query in queries_dict.items():
            async_map[job_name] = self.create_web_search_args_template(query, model, max_tokens, max_uses)
        return async_map
    
    async def get_simple_responses(self, arg_async_map):
        """
        Get responses asynchronously and return only clean text - no complex objects.
        
        Args:
            arg_async_map (dict): A dictionary mapping job names to API arguments.
            
        Returns:
            list: A list of (job_name, clean_text) tuples.
        """
        async def simple_task(job_name, api_args):
            print(f"Task {job_name} start: {datetime.datetime.now().time()}")
            response = await self.async_client.messages.create(**api_args)
            clean_text = self.get_clean_text(response)
            print(f"Task {job_name} end: {datetime.datetime.now().time()}")
            return job_name, clean_text

        tasks = [asyncio.create_task(simple_task(job_name, args)) for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)

    def execute_bulk_web_search_simple(self, queries_dict, model, max_tokens=5000, max_uses=5):
        """
        Execute bulk web search and return a simple, clean dataframe.
        
        Args:
            queries_dict (dict): Dictionary mapping query IDs to query strings.
            model (str): The model to use for all requests.
            max_tokens (int, optional): Maximum tokens in response, defaults to 5000.
            max_uses (int, optional): Maximum number of web searches Claude can perform, defaults to 5.
            
        Returns:
            pd.DataFrame: Simple dataframe with columns: query_id, query, clean_response
        """
        print(f"Starting bulk web search for {len(queries_dict)} queries...")
        
        # Create async map
        async_map = self.create_bulk_web_search_map(queries_dict, model, max_tokens, max_uses)
        
        # Execute async requests and get clean text directly
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.get_simple_responses(arg_async_map=async_map))
        
        print(f"Completed async requests, processing {len(results)} responses...")
        
        # Build simple dataframe with just the 3 columns you want
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
        print(f"Columns: {list(df.columns)}")
        return df
    
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
                    response = await self.async_client.messages.create(**api_args)
                    clean_text = self.get_clean_text(response)
                    return job_name, clean_text
                except Exception as e:
                    print(f"Error processing {job_name}: {str(e)}")
                    return job_name, f"Error: {str(e)}"

        tasks = [asyncio.create_task(rate_limited_task(job_name, args, semaphore)) 
                for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)
    
    def execute_bulk_web_search_rate_constrained(self, queries_dict, model, 
                                                batch_size=10, max_tokens=5000, 
                                                max_uses=5, delay_between_batches=1.0):
        """
        Execute bulk web search with rate constraints and progress tracking.
        
        Args:
            queries_dict (dict): Dictionary mapping query IDs to query strings.
            model (str): The model to use for all requests.
            batch_size (int, optional): Number of queries per batch, defaults to 10.
            max_tokens (int, optional): Maximum tokens in response, defaults to 5000.
            max_uses (int, optional): Maximum number of web searches Claude can perform, defaults to 5.
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
                async_map = self.create_bulk_web_search_map(batch_dict, model, max_tokens, max_uses)
                
                # Execute batch with semaphore for additional rate limiting within batch
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