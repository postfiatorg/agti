from openai import OpenAI, AsyncOpenAI
import pandas as pd
import datetime
import uuid
import asyncio
import nest_asyncio
import json
import time
import random
from asyncio import Semaphore

class OpenRouterTool:
    """ 
    # Example usage:
    if __name__ == "__main__":
        client = OpenRouterTool(pw_map={'openrouter': 'your-api-key'})
        
        # Example 1: Basic text completion
        print("\nExample 1: Text Completion")
        print(client.example_text_completion())
        
        # Example 2: Image analysis
        print("\nExample 2: Image Analysis")
        image_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/d/dd/Gfp-wisconsin-madison-the-nature-boardwalk.jpg/2560px-Gfp-wisconsin-madison-the-nature-boardwalk.jpg"
        print(client.example_image_analysis(image_url))
        
        # Example 3: Structured output
        print("\nExample 3: Structured Output")
        print(client.example_structured_output())
        
        # Example 4: Multi-turn conversation
        print("\nExample 4: Multi-turn Conversation")
        conversation = client.example_multi_turn_conversation()
        for turn, response in conversation:
            print(f"\n{turn}:")
            print(response)
        
        # Example 5: Function calling
        print("\nExample 5: Function Calling")
        print(client.example_function_calling())
        
        # Example 6: Async completion with DataFrame output
        print("\nExample 6: Async Completion")
        df = client.run_chat_completion_async_demo()
        print(df)
    """
    def __init__(self, pw_map, max_concurrent_requests=30, requests_per_minute=120, http_referer="postfiat.org", timeout=180.0):
        self.pw_map = pw_map
        self.http_referer = http_referer
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.pw_map['openrouter'],
            timeout=timeout  # Increased timeout for better reliability
        )
        self.async_client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.pw_map['openrouter'],
            timeout=timeout  # Increased timeout for better reliability
        )
        self.semaphore = Semaphore(max_concurrent_requests)
        self.rate_limit = requests_per_minute
        self.request_times = []

    def _prepare_headers(self):
        """Prepare headers required for OpenRouter API"""
        return {
            "HTTP-Referer": self.http_referer
        }

    def generate_simple_text_output(self, model, messages, max_tokens=None, temperature=None):
        """
        Generate text output using specified model
        
        Example:
        model="anthropic/claude-3.5-sonnet"
        messages=[{"role": "user", "content": "Hello!"}]
        """
        try:
            completion = self.client.chat.completions.create(
                extra_headers=self._prepare_headers(),
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature
            )
            
            # Add safety checks
            if not completion:
                print(f"[WARNING] Completion is None for model {model}")
                return None
                
            if not hasattr(completion, 'choices') or not completion.choices:
                print(f"[WARNING] No choices in completion for model {model}")
                return None
                
            return completion.choices[0].message.content
        except Exception as e:
            print(f"[ERROR] Error in generate_simple_text_output: {e}")
            return None
            
    async def generate_simple_text_output_async(self, model, messages, max_tokens=None, temperature=None):
        """
        Async version of generate_simple_text_output
        
        Example:
        model="anthropic/claude-3.5-sonnet"
        messages=[{"role": "user", "content": "Hello!"}]
        """
        try:
            completion = await self.async_client.chat.completions.create(
                extra_headers=self._prepare_headers(),
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature
            )
            
            # Add safety checks
            if not completion:
                print(f"[WARNING] Completion is None for model {model}")
                return None
                
            if not hasattr(completion, 'choices') or not completion.choices:
                print(f"[WARNING] No choices in completion for model {model}")
                return None
                
            return completion.choices[0].message.content
        except Exception as e:
            print(f"[ERROR] Error in generate_simple_text_output_async: {e}")
            return None

    def generate_dataframe(self, model, messages, max_tokens=None, temperature=None):
        """Generate a DataFrame containing the response and metadata"""
        completion = self.client.chat.completions.create(
            extra_headers=self._prepare_headers(),
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        output_map = {
            'text_response': completion.choices[0].message.content,
            'model': model,
            'max_tokens': max_tokens,
            'temperature': temperature,
            'messages': json.dumps(messages),
            'date_run': datetime.datetime.now(),
            'job_uuid': str(uuid.uuid4()),
            'finish_reason': completion.choices[0].finish_reason,
            'usage': json.dumps(completion.usage.model_dump())
        }
        return pd.DataFrame(output_map, index=[0])

    async def rate_limited_request(self, job_name, api_args):
        """Execute a rate-limited API request"""
        async with self.semaphore:
            await self.wait_for_rate_limit()
            print(f"Task {job_name} start: {datetime.datetime.now().time()}")
            try:
                response = await self.async_client.chat.completions.create(
                    extra_headers=self._prepare_headers(),
                    **api_args
                )
                print(f"Task {job_name} end: {datetime.datetime.now().time()}")
                return job_name, response
            except Exception as e:
                print(f"Error for task {job_name}: {str(e)}")
                await asyncio.sleep(5)
                return await self.rate_limited_request(job_name, api_args)

    async def wait_for_rate_limit(self):
        """
        Implement more efficient rate limiting with token bucket algorithm
        This allows for bursts of requests while still preventing overages
        """
        now = time.time()
        # Clear old requests from the tracking list
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        # If well under the rate limit, proceed immediately with no delay
        if len(self.request_times) < (self.rate_limit * 0.6):  # If under 60% of capacity
            self.request_times.append(time.time())
            return
            
        # If just under the rate limit, small delay with jitter to smooth traffic
        if len(self.request_times) < self.rate_limit:
            jitter = random.uniform(0.0, 0.2)  # Small jitter
            await asyncio.sleep(jitter)  # Mini sleep to spread out requests
            self.request_times.append(time.time())
            return
            
        # Otherwise, calculate a smart progressive delay
        # This uses a more distributed approach to avoid all requests waiting for the same time
        current_requests = len(self.request_times)
        delay_factor = (current_requests - self.rate_limit + 1) / self.rate_limit
        
        # Apply jitter proportional to load - heavier load, more jitter
        # This helps prevent request alignment that can cause waves of traffic
        jitter_max = min(1.0, delay_factor * 0.8)  # Up to 1 second of jitter based on load
        jitter = random.uniform(0.0, jitter_max)
        
        # Progressive backoff - higher load = longer backoff
        sleep_time = max(0.05, delay_factor * 1.5) + jitter
        
        # Cap the sleep time to be reasonable but allow longer sleeps for heavy load
        sleep_time = min(sleep_time, 3.0)
        
        # Sleep for calculated time
        await asyncio.sleep(sleep_time)
        self.request_times.append(time.time())

    async def get_completions(self, arg_async_map):
        """Get completions asynchronously for given arguments map"""
        tasks = [self.rate_limited_request(job_name, args) for job_name, args in arg_async_map.items()]
        return await asyncio.gather(*tasks)

    def create_writable_df_for_async_chat_completion(self, arg_async_map, timeout=360, task_timeout=300):
        """
        Create DataFrame for async chat completion results with improved error handling
        
        Args:
            arg_async_map: Dictionary of job names and API arguments
            timeout: Maximum time to wait for all completions (default: 360 seconds)
            task_timeout: Maximum time for individual tasks (default: 300 seconds)
        """
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        
        # Use the improved error handling function
        results = loop.run_until_complete(
            self.run_async_chat_completions_with_error_handling(
                arg_async_map=arg_async_map,
                timeout=timeout,
                task_timeout=task_timeout
            )
        )
        
        dfarr = []
        for internal_name, result_or_error in results.items():
            # Skip failed tasks - they'll be logged but not included in the DataFrame
            if isinstance(result_or_error, Exception):
                print(f"Skipping failed task {internal_name}: {type(result_or_error).__name__}: {result_or_error}")
                continue
                
            # Process successful completions
            try:
                completion_object = result_or_error
                raw_df = pd.DataFrame({
                    'id': completion_object.id,
                    'model': completion_object.model,
                    'content': completion_object.choices[0].message.content,
                    'finish_reason': completion_object.choices[0].finish_reason,
                    'usage': json.dumps(completion_object.usage.model_dump()),
                    'write_time': datetime.datetime.now(),
                    'internal_name': internal_name
                }, index=[0])
                dfarr.append(raw_df)
            except Exception as e:
                print(f"Error processing result for {internal_name}: {e}")
                # Continue with other results instead of failing entirely
        
        # Return empty DataFrame if no successful results
        if not dfarr:
            print("No successful completions to include in DataFrame")
            return pd.DataFrame()
            
        full_writable_df = pd.concat(dfarr, ignore_index=True)
        full_writable_df['choices__message__content'] = full_writable_df['content']
        return full_writable_df

    def create_writable_df_for_chat_completion_easy(self, arg_async_map, timeout=360, task_timeout=300):
        """
        A safer version of create_writable_df_for_async_chat_completion.
        It checks for None or missing fields and logs a fallback row instead of erroring out.
        
        Args:
            arg_async_map: Dictionary of job names and API arguments
            timeout: Maximum time to wait for all completions (default: 360 seconds)
            task_timeout: Maximum time for individual tasks (default: 300 seconds)
        """
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        
        # Use the improved error handling function
        results = loop.run_until_complete(
            self.run_async_chat_completions_with_error_handling(
                arg_async_map=arg_async_map,
                timeout=timeout,
                task_timeout=task_timeout
            )
        )

        dfarr = []
        for internal_name, result_or_error in results.items():
            # Handle exceptions directly with fallback rows
            if isinstance(result_or_error, Exception):
                print(f"[WARNING] Task '{internal_name}' failed with error: {type(result_or_error).__name__}: {result_or_error}")
                fallback_df = pd.DataFrame({
                    'id': [None],
                    'model': [None],
                    'content': [None],
                    'finish_reason': [f"error_{type(result_or_error).__name__}"],
                    'usage': [None],
                    'write_time': [datetime.datetime.now()],
                    'internal_name': [internal_name],
                    'error': [str(result_or_error)]  # Store the error message for debugging
                })
                dfarr.append(fallback_df)
                continue
                
            completion_object = result_or_error
            
            # --------------------------------------------------------
            # More descriptive warnings for partial/no response
            # --------------------------------------------------------
            if completion_object is None:
                # No response at all
                print(f"[WARNING] For job '{internal_name}': completion_object is None.")
                reason = "None"
            elif not hasattr(completion_object, 'choices'):
                # Missing the 'choices' attribute
                print(f"[WARNING] For job '{internal_name}': 'choices' attribute is missing.")
                reason = "no_choices_attr"
            elif not completion_object.choices:
                # 'choices' is an empty list
                print(f"[WARNING] For job '{internal_name}': 'choices' list is empty.")
                reason = "choices_empty"
            else:
                reason = None

            if reason:
                # Then we have incomplete data, so fallback row
                fallback_df = pd.DataFrame({
                    'id': [None],
                    'model': [None],
                    'content': [None],
                    'finish_reason': [reason],
                    'usage': [None],
                    'write_time': [datetime.datetime.now()],
                    'internal_name': [internal_name]
                })
                dfarr.append(fallback_df)
                continue

            try:
                # Normal successful case
                raw_df = pd.DataFrame({
                    'id': [completion_object.id],
                    'model': [completion_object.model],
                    'content': [completion_object.choices[0].message.content],
                    'finish_reason': [completion_object.choices[0].finish_reason],
                    'usage': [json.dumps(completion_object.usage.model_dump())],
                    'write_time': [datetime.datetime.now()],
                    'internal_name': [internal_name]
                })
                dfarr.append(raw_df)
            except Exception as e:
                # Catch any random error building the DataFrame
                print(f"[WARNING] Error processing job '{internal_name}' => {e}")
                fallback_df = pd.DataFrame({
                    'id': [None],
                    'model': [None],
                    'content': [None],
                    'finish_reason': ["processing_error"],
                    'usage': [None],
                    'write_time': [datetime.datetime.now()],
                    'internal_name': [internal_name],
                    'error': [str(e)]  # Store the error message for debugging
                })
                dfarr.append(fallback_df)

        if not dfarr:
            print("[WARNING] No results to process, returning empty DataFrame")
            return pd.DataFrame()

        # Count results by status
        total = len(dfarr)
        failures = sum(1 for df in dfarr if df['finish_reason'].iloc[0] is None or 
                      (isinstance(df['finish_reason'].iloc[0], str) and 
                      (df['finish_reason'].iloc[0].startswith('error_') or 
                       df['finish_reason'].iloc[0] in ['None', 'no_choices_attr', 'choices_empty', 'processing_error'])))
        successes = total - failures
        
        print(f"Processing complete: {successes}/{total} successful tasks ({failures} failures)")
        
        full_writable_df = pd.concat(dfarr, ignore_index=True)
        # Keep the same column: 'choices__message__content'
        # so your pipeline can read from that if needed
        full_writable_df["choices__message__content"] = full_writable_df["content"]
        return full_writable_df

    def run_chat_completion_async_demo(self, timeout=360, task_timeout=300):
        """
        Run demo for async chat completion with improved error handling
        
        Args:
            timeout: Maximum time to wait for all completions (default: 360 seconds)
            task_timeout: Maximum time for individual tasks (default: 300 seconds)
        """
        job_hashes = [f'job{i}sample__{uuid.uuid4()}' for i in range(1, 3)]
        arg_async_map = {
            job_hashes[0]: {
                "model": "anthropic/claude-3.5-sonnet",
                "messages": [{"role": "user", "content": "What's the future of AI?"}]
            },
            job_hashes[1]: {
                "model": "anthropic/claude-3.5-sonnet",
                "messages": [{"role": "user", "content": "Explain quantum computing"}]
            }
        }
        
        print(f"Running async completion demo with timeout={timeout}s, task_timeout={task_timeout}s")
        return self.create_writable_df_for_chat_completion_easy(
            arg_async_map=arg_async_map,
            timeout=timeout,
            task_timeout=task_timeout
        )

    def example_text_completion(self):
        """
        Example of basic text completion
        Returns: Generated text response
        """
        response = self.generate_simple_text_output(
            model="anthropic/claude-3.5-sonnet",
            messages=[{
                "role": "user", 
                "content": "Write a short poem about artificial intelligence"
            }],
            temperature=0.7
        )
        return response

    def example_image_analysis(self, image_url):
        """
        Example of image analysis using multimodal capabilities
        Args:
            image_url: URL of the image to analyze
        Returns: Analysis of the image
        """
        response = self.generate_simple_text_output(
            model="anthropic/claude-3.5-sonnet",
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Analyze this image in detail. What do you see?"
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": image_url
                        }
                    }
                ]
            }]
        )
        return response

    def example_structured_output(self):
        """
        Example of generating structured data
        Returns: DataFrame with structured analysis
        """
        messages = [{
            "role": "user",
            "content": "Analyze the following companies: Apple, Microsoft, Google. For each provide: 1) Main business area 2) Year founded 3) Key products. Format as JSON."
        }]
        
        response = self.generate_dataframe(
            model="anthropic/claude-3.5-sonnet",
            messages=messages,
            temperature=0.3
        )
        return response

    def example_multi_turn_conversation(self):
        """
        Example of multi-turn conversation
        Returns: List of responses from the conversation
        """
        conversation = []
        
        # First turn
        response1 = self.generate_simple_text_output(
            model="anthropic/claude-3.5-sonnet",
            messages=[{
                "role": "user",
                "content": "What are the three laws of robotics?"
            }]
        )
        conversation.append(("Question 1", response1))
        
        # Second turn - follow up
        response2 = self.generate_simple_text_output(
            model="anthropic/claude-3.5-sonnet",
            messages=[
                {"role": "user", "content": "What are the three laws of robotics?"},
                {"role": "assistant", "content": response1},
                {"role": "user", "content": "Who created these laws and in what work were they first introduced?"}
            ]
        )
        conversation.append(("Question 2", response2))
        
        return conversation

    def example_function_calling(self):
        """
        Example of function calling capability
        Returns: Structured function call result
        """
        messages = [{
            "role": "user",
            "content": "Extract the following information from this text: 'The meeting is scheduled for March 15th, 2024 at 2:30 PM with John Smith to discuss the Q1 budget.'"
        }]
        
        # Using Claude to extract structured information
        response = self.generate_simple_text_output(
            model="anthropic/claude-3.5-sonnet",
            messages=messages,
            temperature=0
        )
        
        # Parse the response into a structured format
        try:
            # Assuming the model returns a JSON-like structure
            structured_data = json.loads(response)
            return structured_data
        except:
            return {"error": "Could not parse response into structured format", "raw_response": response}

    async def run_async_chat_completions_with_error_handling(self, arg_async_map: dict, timeout: int = 360, task_timeout: int = 300):
        """
        Runs multiple chat completion requests asynchronously via OpenRouter 
        with error handling, rate limiting, and semaphore control.

        Args:
            arg_async_map: A dictionary where keys are job names (strings) 
                           and values are dictionaries of API arguments for chat completion.
            timeout: Maximum time in seconds to wait for all completions (default: 360 seconds, 6 minutes).
            task_timeout: Maximum time in seconds for individual tasks (default: 300 seconds, 5 minutes).

        Returns:
            A dictionary where keys are the job names and values are either the 
            OpenAI completion object (from OpenRouter) upon success or an Exception object upon failure.
        """
        results = {}

        async def task_with_error_handling(job_name, api_args):
            async with self.semaphore: # Use the class semaphore
                await self.wait_for_rate_limit() # Use the class rate limiter
                print(f"Task {job_name} start: {datetime.datetime.now().time()}")
                try:
                    # Add individual task timeout with asyncio.wait_for
                    response = await asyncio.wait_for(
                        self.async_client.chat.completions.create(
                            extra_headers=self._prepare_headers(), # Add OpenRouter headers
                            **api_args
                        ), 
                        timeout=task_timeout
                    )
                    print(f"Task {job_name} end successfully: {datetime.datetime.now().time()}")
                    self.request_times.append(time.time()) # Record successful request time for rate limiting
                    return job_name, response
                except asyncio.TimeoutError:
                    print(f"Task {job_name} timed out after {task_timeout} seconds")
                    return job_name, TimeoutError(f"Individual task timed out after {task_timeout} seconds")
                except Exception as e:
                    print(f"Task {job_name} failed: {datetime.datetime.now().time()} with error: {e}")
                    # Log the error more formally if needed
                    # Note: We don't record request time on failure for rate limiting purposes here
                    #       but could adjust if API still counts failed attempts.
                    # Optionally add a small delay before returning the error
                    # await asyncio.sleep(1) 
                    return job_name, e

        # Create individual tasks
        tasks = [task_with_error_handling(job_name, args) for job_name, args in arg_async_map.items()]
        
        # Use asyncio.gather with return_exceptions=True to prevent one failing task from killing the whole batch
        try:
            # Use wait instead of wait_for to get more control over timeout behavior
            done, pending = await asyncio.wait(
                [asyncio.create_task(task) for task in tasks],
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED
            )
            
            # Process completed tasks
            for task in done:
                try:
                    job_name, result_or_error = task.result()
                    results[job_name] = result_or_error
                except Exception as e:
                    # This shouldn't happen often since we catch exceptions in task_with_error_handling
                    print(f"Error retrieving result from completed task: {e}")
            
            # Handle any pending (timed out) tasks
            if pending:
                print(f"Batch timeout after {timeout} seconds. {len(pending)} tasks still pending.")
                # Cancel pending tasks to avoid resource leaks
                for task in pending:
                    task.cancel()
                    
                # Mark pending tasks as timed out in results
                for job_name in arg_async_map.keys():
                    if job_name not in results:
                        results[job_name] = TimeoutError(f"Batch timeout after {timeout} seconds")
                
        except Exception as e:
            # Catch any other unexpected errors at the batch level
            print(f"Unexpected error in batch processing: {e}")
            
            # Ensure all tasks have entries in results
            for job_name in arg_async_map.keys():
                if job_name not in results:
                    results[job_name] = RuntimeError(f"Batch processing error: {str(e)}")

        return results

    def run_async_chat_completions_with_error_handling_demo(self, timeout=360, task_timeout=300):
        """
        Demonstrates running async chat completions with error handling via OpenRouter.
        
        Args:
            timeout: Maximum time in seconds to wait for all completions (default: 360 seconds, 6 minutes).
            task_timeout: Maximum time in seconds for individual tasks (default: 300 seconds, 5 minutes).
        """
        job_hashes = {
            'success_job_1': f'or_job1__{uuid.uuid4()}',
            'success_job_2': f'or_job2__{uuid.uuid4()}',
            'failure_job_bad_model': f'or_job3__{uuid.uuid4()}',
            'success_job_3': f'or_job4__{uuid.uuid4()}',
        }
        # Use a valid OpenRouter model like Claude 3.5 Sonnet
        valid_model = "anthropic/claude-3.5-sonnet"
        arg_async_map = {
            job_hashes['success_job_1']: {
                "model": valid_model, 
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "What is the capital of Canada?"}
                ]
            },
            job_hashes['success_job_2']: {
                "model": valid_model,
                "messages": [
                    {"role": "system", "content": "You are a poetic soul."},
                    {"role": "user", "content": "Write a haiku about asynchronous code."}
                ]
            },
            # This job is designed to fail due to an invalid model name
            job_hashes['failure_job_bad_model']: {
                "model": 'openrouter/invalid-model-will-fail', # Intentionally invalid for OpenRouter
                "messages": [
                    {"role": "user", "content": "This request will fail."}
                ]
            },
            job_hashes['success_job_3']: {
                "model": valid_model,
                "messages": [
                    {"role": "system", "content": "You know geography."},
                    {"role": "user", "content": "What is the longest river in the world?"}
                ]
            },
        }

        print("\n--- Starting OpenRouter Async Demo with Error Handling ---")
        print(f"Using batch timeout of {timeout} seconds, individual task timeout of {task_timeout} seconds")
        
        # Use nest_asyncio and run_until_complete 
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        # Check if loop is already running (important in some environments like Jupyter)
        if loop.is_running():
             # This execution path might be needed if called from an already async context
             # or within a running event loop (like Jupyter). A simple run_until_complete
             # might fail here. Running in a separate thread or using run_coroutine_threadsafe
             # could be alternatives, but run_until_complete often works with nest_asyncio.
             # For simplicity, we'll stick to run_until_complete assuming nest_asyncio handles it.
             print("(Running within an existing event loop)")
             results = loop.run_until_complete(
                 self.run_async_chat_completions_with_error_handling(
                     arg_async_map=arg_async_map, 
                     timeout=timeout,
                     task_timeout=task_timeout
                 )
            )
        else:
            results = loop.run_until_complete(
                self.run_async_chat_completions_with_error_handling(
                    arg_async_map=arg_async_map, 
                    timeout=timeout,
                    task_timeout=task_timeout
                )
            )
       

        print("\n--- OpenRouter Async Demo Results ---")
        successful_results = {}
        failed_results = {}
        for job_name, result_or_error in results.items():
            # Find the original descriptive key (like 'success_job_1') based on the generated hash
            original_job_key = next((key for key, val in job_hashes.items() if val == job_name), None)
            print(f"Processing job: {original_job_key} ({job_name})")
            if isinstance(result_or_error, Exception):
                print(f"  Status: Failed")
                print(f"  Error: {type(result_or_error).__name__}: {result_or_error}")
                failed_results[job_name] = result_or_error
            elif result_or_error is None:
                 # Handle cases where the task might return None unexpectedly
                 print(f"  Status: Failed (Returned None)")
                 failed_results[job_name] = None # Or some other indicator
            else:
                # Assuming result_or_error is a valid OpenAI completion object from OpenRouter
                print(f"  Status: Succeeded")
                try:
                    # Safely access content
                    if hasattr(result_or_error, 'choices') and result_or_error.choices:
                         content = result_or_error.choices[0].message.content
                         print(f"  Content Preview: {content[:60]}...") # Print first 60 chars
                    else:
                         print("  Content: [No choices found in response]")
                         # Optionally capture the whole object for inspection
                         # successful_results[job_name] = result_or_error 
                except (AttributeError, IndexError, TypeError) as e:
                    print(f"  Could not extract content: {e}")
                    # Capture the problematic object for debugging
                    # successful_results[job_name] = result_or_error 
                
                # Store the successful result object regardless of content extraction success
                successful_results[job_name] = result_or_error

        print("--- End OpenRouter Async Demo ---")
        
        # Print summary statistics
        total_tasks = len(arg_async_map)
        completed_tasks = len(successful_results)
        failed_tasks = len(failed_results)
        
        print(f"\nSummary: {completed_tasks} of {total_tasks} tasks completed successfully")
        print(f"Failed tasks: {failed_tasks}")
        if failed_tasks > 0:
            print("Failure types:")
            error_types = {}
            for error in failed_results.values():
                error_type = type(error).__name__
                error_types[error_type] = error_types.get(error_type, 0) + 1
            for error_type, count in error_types.items():
                print(f"  {error_type}: {count}")
        
        # Optionally return the processed results
        return {"successful": successful_results, "failed": failed_results}

