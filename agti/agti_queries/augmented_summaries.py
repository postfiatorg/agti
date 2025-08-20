import sqlalchemy
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import requests
from PyPDF2 import PdfReader
from io import BytesIO
from tqdm.notebook import tqdm
import time
from typing import List, Optional, Dict
from sqlalchemy import text
from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.settings import CredentialManager
from agti.ai.openrouter import OpenRouterTool
import datetime
import pandas as pd
import numpy as np
import asyncio
from typing import List, Dict, Any, Optional
import pandas as pd


class CentralBankPDFProcessor:
    def __init__(self, pw_map: Dict, user_name: str = 'agti_corp'):
        """Initialize with password map and user."""
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_conn_manager = DBConnectionManager(pw_map)
        self.dbconn = None
        self.data = None
        self.openrouter_tool = OpenRouterTool(pw_map=self.pw_map,max_concurrent_requests=50)
        self.qanda_filings = self.output_highly_relevant_q_and_a_filings()
        
    def __enter__(self):
        """Context manager entry - establish database connection."""
        self.dbconn = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(
            user_name=self.user_name
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup database connection."""
        if self.dbconn:
            self.dbconn.dispose()
            
    @staticmethod
    def extract_pdf_text(url: str) -> str:
        """Extract text from a PDF URL."""
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                reader = PdfReader(BytesIO(resp.content))
                text = ""
                for page in reader.pages:
                    text += page.extract_text()
                return text
        except:
            return ""
    
    def load_g10_data(self, start_date: str = '2020-01-01') -> pd.DataFrame:
        """Load G10 central bank data from database."""
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        valid_g10 = pd.read_sql('central_banks_g10', dbconnx)
        g10_snapshot = valid_g10[valid_g10['date_published'] >= start_date].sort_values('date_published').copy()
        
        # Add year and AWS link columns
        g10_snapshot['year'] = g10_snapshot['date_published'].apply(lambda x: x.year)
        g10_snapshot['aws_link'] = g10_snapshot.apply(
            lambda x: f"https://agti-central-banks.s3.us-east-1.amazonaws.com/{x['country_code_alpha_3']}/{x['year']}/{x['file_id']}.pdf",
            axis=1
        )
        
        self.data = g10_snapshot
        return g10_snapshot
    
    def identify_unextracted_pdfs(self) -> List[str]:
        """Identify PDFs that haven't been extracted yet."""
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        try:
            existing_pdf_table = pd.read_sql('all_central_bank_filings', dbconnx)
            existing_in_table = list(existing_pdf_table['aws_link'].unique())
        except:
            # Table doesn't exist, all PDFs need extraction
            existing_in_table = []
            print("Table 'all_central_bank_filings' not found. Will extract all PDFs.")
        finally:
            dbconnx.dispose()
        
        # Get all possible PDFs
        if self.data is None:
            self.load_g10_data()
        
        all_possible_pdfs = list(self.data['aws_link'].unique())
        
        # Find unextracted PDFs
        unextracted_pdfs = [i for i in all_possible_pdfs if i not in existing_in_table]
        
        print(f"Found {len(unextracted_pdfs)} unextracted PDFs out of {len(all_possible_pdfs)} total PDFs")
        return unextracted_pdfs
    
    def extract_pdfs(self, max_workers: int = 8) -> None:
        """Extract text from all PDFs in parallel."""
        if self.data is None:
            raise ValueError("No data loaded. Call load_g10_data() first.")
            
        start_time = time.time()
        urls = self.data['aws_link'].tolist()
        results = [None] * len(urls)
        
        # Use processes for CPU-bound PDF parsing
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {
                executor.submit(self.extract_pdf_text, url): i 
                for i, url in enumerate(urls)
            }
            
            for future in tqdm(as_completed(future_to_index), total=len(urls), 
                             desc="Extracting PDFs", unit="pdf"):
                index = future_to_index[future]
                results[index] = future.result()
        
        # Add extracted text to data
        self.data['extracted_text'] = results
        self.data['extracted_text'] = self.data['extracted_text'].str.replace('\x00', '', regex=False)
        
        elapsed = time.time() - start_time
        print(f"\nExtracted {len(urls)} PDFs in {elapsed:.1f} seconds ({len(urls)/elapsed:.1f} PDFs/sec)")
    
    def extract_incremental_pdfs(self, max_workers: int = 8) -> None:
        """Extract text only from unextracted PDFs."""
        if self.data is None:
            self.load_g10_data()
        
        # Identify unextracted PDFs
        unextracted_pdfs = self.identify_unextracted_pdfs()
        
        if not unextracted_pdfs:
            print("No new PDFs to extract.")
            return
        
        # Filter data to only unextracted PDFs
        self.data = self.data[self.data['aws_link'].isin(unextracted_pdfs)].copy()
        
        # Use the regular extract_pdfs method on the filtered data
        self.extract_pdfs(max_workers)
    
    def save_to_database(self, table_name: str = 'all_central_bank_filings', 
                        if_exists: str = 'replace', 
                        chunksize: int = 5000) -> None:
        """Save processed data to database."""
        if self.data is None:
            raise ValueError("No data to save. Process data first.")
            
        # Drop table if replacing
        if if_exists == 'replace':
            with self.dbconn.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        # Clean string columns
        string_cols = self.data.select_dtypes(include=['object']).columns
        for col in string_cols:
            self.data[col] = self.data[col].astype(str).str.replace('\x00', '', regex=False)
        
        # Save to database
        self.data.to_sql(
            table_name,
            self.dbconn,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=chunksize
        )
        
        print(f"Saved {len(self.data)} rows to {table_name}")
    
    def save_incremental_to_database(self, table_name: str = 'all_central_bank_filings', 
                                   chunksize: int = 5000) -> None:
        """Append newly processed data to database."""
        if self.data is None or len(self.data) == 0:
            print("No data to save.")
            return
        
        # Clean string columns
        string_cols = self.data.select_dtypes(include=['object']).columns
        for col in string_cols:
            self.data[col] = self.data[col].astype(str).str.replace('\x00', '', regex=False)
        
        # Create a database connection if needed
        if self.dbconn is None:
            dbconn = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        else:
            dbconn = self.dbconn
        
        # Check if table exists
        try:
            existing_df = pd.read_sql(f"SELECT 1 FROM {table_name} LIMIT 1", dbconn)
            table_exists = True
        except:
            table_exists = False
        
        # Save to database (append if exists, create if not)
        self.data.to_sql(
            table_name,
            dbconn,
            if_exists='append' if table_exists else 'replace',
            index=False,
            method='multi',
            chunksize=chunksize
        )
        
        # Dispose connection if we created it
        if self.dbconn is None:
            dbconn.dispose()
        
        print(f"Saved {len(self.data)} new rows to {table_name}")
    
    def process_all(self, start_date: str = '2020-01-01', 
                   max_workers: int = 8,
                   table_name: str = 'all_central_bank_filings') -> pd.DataFrame:
        """Complete pipeline: load, extract, and save."""
        self.load_g10_data(start_date)
        self.extract_pdfs(max_workers)
        self.save_to_database(table_name)
        return self.data
    
    def process_incremental(self, start_date: str = '2020-01-01', 
                          max_workers: int = 8,
                          table_name: str = 'all_central_bank_filings') -> pd.DataFrame:
        """Complete incremental pipeline: identify new PDFs, extract, and save."""
        # Load all data
        self.load_g10_data(start_date)
        
        # Extract only new PDFs
        self.extract_incremental_pdfs(max_workers)
        
        # Save to database incrementally
        self.save_incremental_to_database(table_name)
        
        return self.data
    
    async def process_full_transcript_history(self):
        agti_corp = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        all_central_bank_filings = pd.read_sql('all_central_bank_filings', dbconnx)
        system_prompt = '''You are the AGTI Summarization Module. 
        You are working for a top global investment bank as an extremely detail oriented analyst 
        You always follow orders exactly regarding summarization jobs
        You output an answer in pipe delimited format requested with no special formatting that would break extraction logic
        '''
        user_prompt = ''' Your job is to make standardized summaries in Markdown of a PDF

        The article content starts here
        << ARTICLE CONTENT STARTS HERE >>
        ___article_content___
        <<ARTICLE CONTENT ENDS HERE>>

        Article Purpose Guideline:
        RESEARCH - just generic research on a particular topic 
        POLICY DECISION - specifically a change in interest rates
        ASSET PURCHASES - comments or amounts of QE or Balance sheet changes
        INTERVENTION - primarily FX intervention or bond market intervention 
        MISC - non markets related topics such as personnell changes, or generic announcements 
        REGULATION - changes in market regulation

        You output the following in pipe delimited format. Do not add any formatting to the headers

        OUTPUT:
        | TITLE | Extracted Title of the Article |
        | QUICK SUMMARY | 2 sentences associated with the document explaining what it is |
        | FULL DOC | TRUE or FALSE -- True if the document is a full document and not just a summary |
        | CONCLUSION | The 3-5 sentences expressing the main conclusion or decision reached by the document |
        | CORE STATSITSICS | all core statistics, facts and figures cited in the article that would be interpreted as material |
        | FULL SUMMARY| The bullet pointed summary of all the main claims, impacts and warrants in the article or points that would be salient |
        | MARKET MATERIALITY | An integer between 0-100 that is a gauge of how much market impact an article would have based on 
        explicit policy guidance | 
        | ARTICLE PURPOSE | available options: RESEARCH, POLICY DECISION, INTERVENTION, AUCTION |
        | COMPREHENSION Q | a single 1 sentence question that would evaluate whether or not an analyst understood the article or not. 
        The question should reference the article title, the date or the context such as that it can be answered definitively by referencing this document (a generic question might be 'what is the BOJs policy rate' whereas a more nuanced 
        question would be 'what was the reason cited in August 2024 for the BOJ's change to its policy rate'). These are example questions. The actual question chosen should be the one most relevant to understanding the doc |
        | COMREHENSION A | a single 1-2 sentence answer to the comprehension question |'''
        all_central_bank_filings['user_prompt']=user_prompt
        all_central_bank_filings['system_prompt']=system_prompt
        all_central_bank_filings['extracted_text']= all_central_bank_filings['extracted_text'].apply(lambda x: str(x))
        all_central_bank_filings['user_prompt'] =all_central_bank_filings.apply(lambda x: x['user_prompt'].replace('___article_content___',x['extracted_text']) ,axis=1)
        def api_arg_constructor(system_prompt, user_prompt):
            op= {
            "model": 'google/gemini-2.5-pro',
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
                "temperature":0
            }
            return op 
        all_central_bank_filings['api_arg'] =all_central_bank_filings.apply(lambda x: api_arg_constructor(x['system_prompt'],x['user_prompt']),axis=1)
        full_api_args = all_central_bank_filings[['aws_link','api_arg']]



        # Method 1: Using list comprehension
        def chunk_dataframe_method1(df, chunk_size=300):
            """Split dataframe into chunks using list comprehension"""
            chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
            return chunks
        all_unique_docs = []
        try:
            dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
            all_documents =pd.read_sql('agti_central_bank_summary_reference', dbconnx)
            all_unique_docs = list(all_documents['document'].unique())
            dbconnx.dispose()
        except:
            pass
        central_bank_dexed = all_central_bank_filings.groupby('aws_link').first()
        central_bank_df_to_work = central_bank_dexed[~central_bank_dexed.index.isin(all_unique_docs)].reset_index()
        all_df_chunks = chunk_dataframe_method1(central_bank_df_to_work,300)

        for dfchunkx in all_df_chunks:
            full_api_args = dfchunkx.groupby('aws_link').first()['api_arg'].to_dict()

            openrouter_tool = OpenRouterTool(pw_map=self.pw_map,max_concurrent_requests=50)
            api_args = await openrouter_tool.run_async_chat_completions_with_error_handling(arg_async_map=full_api_args)
            all_api_arg_output = pd.DataFrame(api_args).transpose()
            #all_api_arg_output.loc['https://agti-central-banks.s3.us-east-1.amazonaws.com/EUE/2020/099d428ae4a4e0d0ed023028ff016236f136862d.pdf'][1][1][0].message.content
            all_api_arg_output['extracted_info']= all_api_arg_output[1].apply(lambda x: x[1][0].message.content)
            all_api_arg_output.index.name = 'document'
            args_to_write = all_api_arg_output[['extracted_info']].reset_index()

            args_to_write['datetime']=datetime.datetime.now()
            args_to_write['model']= 'google/gemini-2.5-pro'
            dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
            args_to_write.to_sql('agti_central_bank_summary_reference', dbconnx, if_exists='append',index=False)
            dbconnx.dispose()

    def output_augmented_filings(self):
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        full_extracted_history =pd.read_sql('agti_central_bank_summary_reference', dbconnx)
        def parse_pipes(text):
            """Extract pipe-delimited key-value pairs from a string into a dictionary."""
            result = {}
            parts = [p.strip() for p in text.split('|') if p.strip()]
            
            i = 0
            while i < len(parts) - 1:
                key = parts[i]
                value = parts[i + 1]
                
                # Check if next parts are part of the value (not all-caps keys)
                j = i + 2
                while j < len(parts) and not (parts[j].isupper() and len(parts[j]) > 2):
                    value += " | " + parts[j]
                    j += 1
                
                result[key] = value
                i = j if j > i + 2 else i + 2
            
            return result

        full_extracted_history['pipe_parser']= full_extracted_history['extracted_info'].apply(lambda x: parse_pipes(x))
        all_keys= ['TITLE', 'QUICK SUMMARY', 'FULL DOC', 'CONCLUSION', 'CORE STATSITSICS', 'FULL SUMMARY', 'MARKET MATERIALITY', 'ARTICLE PURPOSE', 'COMPREHENSION Q', 'COMREHENSION A']
        for xkey in all_keys:
            full_extracted_history[xkey.lower().replace(' ','_')]= full_extracted_history['pipe_parser'].apply(lambda x: x.get(xkey, ''))
        full_extracted_history['number_of_keys_extracted'] =full_extracted_history['pipe_parser'].apply(lambda x: len(x.keys()))
        full_extracted_history = full_extracted_history.sort_values('number_of_keys_extracted',ascending=False).groupby('document').last()
        full_extracted_history['market_materiality']=pd.to_numeric(full_extracted_history['market_materiality'],errors='coerce')
        return full_extracted_history


    async def output_key_query_info(self, user_query = 'what was the BOJ cash rate in March of 2021 and how did it compare to the BOEs'):

        start_date = (datetime.datetime.now()-datetime.timedelta(14)).strftime('%Y-%m-%d')
        end_date = (datetime.datetime.now()).strftime('%Y-%m-%d')
        system_prompt = ''' You are the reduction classifier. You take an input prompt and output
        the tightest list of parameters possible for a user query ''' 
        user_prompt= f'''You are given a user query defined below
        << USER QUERY STARTS HERE>> 
        {user_query}
        << USER QUERY ENDS HERE >>

        ## JOB 1: CENTRAL BANK IDENTIFICATION
        Your job is to output a list of relevant countries for the query

        ENG - Bank of England (relevant for GBP) 
        AUS - Australia (relevant for AUD)
        NOR - Norway (relevan for NOK)
        CHE - Switzerland (relevant for CHF)
        EUE - Europe / ECB (relevant for EUR)
        JPN - Bank of Japan (relevant for JPY)
        USA - Federal Reserve (relevant to USD)
        SWE - Swedish Riksbank (relevant to SEK)
        CAN - Canada (relevant to Canada )

        If the list of countries is ambiguous then you should return 
        ENG AUS NOR CHE EUE JPN USA SWE CAN

        These are the only options for countires 

        The list of countries should be space delimited 

        ## JOB 2: DATE START IDENTIFICATION
        Your next job is to output the start date for the query

        if the start date is ambiguous then you should return 
        {start_date}

        if the user is asking a long dated question spanning multiple years
        then you can use '2024-01-01' as a start date. this would be for example
        if the user is asking questions like
        - over the last several years what has been the policy of etc
        - over the last long period

        if the user is asking a question like the 'last year' then you should 
        reference the current date {end_date} and space the time appropriately 

        when the question asks about a specific date - use a start date at least 1 month prior
        to the users request 

        ## JOB 3: DATE END IDENTIFICATION
        Your next job is to output the end date for the query 

        if the end date is ambiguous then you should return 
        {end_date}

        Pay special attention if the user is asking a question about a specific year or datetime
        and reference the appropriate end date if specified (for example if the user is asking a question about Q1 2023
        then focus on that)

        when the question asks about a specific end date - us an end date at least 1 month after
        the users request. always make sure your end date is after your start date 

        ## JOB 4: BULK SUMMARY IDENTIFICATION
        Your next job is to determine whether the job is a bulk summary job. 
        If the answer is YES it is a bulk summary job then the following characteristics are met:
        * The user is asking for analysis across a wide variety of central banks
        * The user is asking for high level analytics that would require many documents as input rather than
        being able to answer a specific question about a targeted list of banks or queries
        * the user is primarily in an analytical frame that would be best served by summaries

        If the answer is NO it is not a bulk summary job - and the following characteristics are met:
        * The user is asking a targeted question about a specific central bank or at least less than 3 central banks 
        * The user is asking for specific information -- a question that has a definite, factual answer -- not one that has a high
        level analytical output
        * The user is primarily in a factual frame, asking questions that would be best served by detailed sources 

        The output for a bulk summary is the string YES or NO

        Always output your output in the following exact pipe delimited format with zero elaboration 
        | START DATE | <a string formatted %Y-%m-%d per instructions in Job 2> |
        | END DATE | <a string formatted %Y-%m-%d per instructions in Job 3> |
        | COUNTRY LIST | <a list of strings that corresponds with all relevant central banks identified. the list should be space delimited not comma delimited. each entry should
        be upper case and exactly correspond with the instructions in Job 1> |
        | BULK SUMMARY | <a string YES or the string NO meticulously following the instructions in job 4 >
        '''
        api_arg =  {
                    "model": 'anthropic/claude-sonnet-4',
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                        "temperature":0
                    }


        classifier = {'run1':api_arg}
        open_router_output= await self.openrouter_tool.run_async_chat_completions_with_error_handling(classifier)
        classifier_info = open_router_output['run1'].choices[0].message.content
        start_date = classifier_info.split('START DATE |')[-1:][0].split('|')[0].strip()
        end_date = classifier_info.split('END DATE |')[-1:][0].split('|')[0].strip()
        bulk_summary = classifier_info.split('BULK SUMMARY |')[-1:][0].split('|')[0].strip()
        countries = classifier_info.split('COUNTRY LIST |')[-1:][0].split('|')[0].strip()
        country_list = countries.split(' ')

        output = {'start_date':start_date,
                'end_date': end_date,
                'countr_list':country_list,
                'bulk_summary': bulk_summary}
        return output

    def output_highly_relevant_q_and_a_filings(self):
        ''' Full documents only with high market relevance'''
        g10_data = self.load_g10_data()
        date_published = g10_data.groupby('aws_link').first()['date_published']
        augmented_filings = self.output_augmented_filings()
        valid_filings = augmented_filings[(augmented_filings['full_doc']=='TRUE') & (augmented_filings['market_materiality']>50)].copy()
        valid_filings['dex_copy'] = valid_filings.index
        valid_filings['country']= valid_filings['dex_copy'].apply(lambda x: x.split('amazonaws.com')[-1:][0].split('/')[1])
        valid_filings['year']= valid_filings['dex_copy'].apply(lambda x: x.split('amazonaws.com')[-1:][0].split('/')[2])
        valid_filings['date_published']=date_published
        valid_filings['vector_info'] = valid_filings['extracted_info'].astype(str)+'| COUNTRY | '+valid_filings['country'].astype(str)+ ' | YEAR |'+ valid_filings['year'].astype(str)+' | DATE PUBLISHED | '+ valid_filings['date_published'].astype(str)
        valid_filings['article_ref']=list(range(0,len(valid_filings)))
        all_document_keys = list(valid_filings.index)

        # Convert list to tuple for SQL IN clause
        all_document_keys_tuple = tuple(all_document_keys)

        # Use SQLAlchemy's parameter binding
        query = f'''
        SELECT * 
        FROM all_central_bank_filings 
        WHERE aws_link IN {all_document_keys_tuple}
        '''

        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user('agti_corp')
        df = pd.read_sql(query, dbconnx)
        dexed_full_extraction = df.groupby('aws_link').last()
        valid_filings['full_extracted_text']=dexed_full_extraction['extracted_text']
        valid_filings['pre_scraping_url']=dexed_full_extraction['file_url']
        #valid_filings[['comprehension_q','comrehension_a']].loc['https://agti-central-banks.s3.us-east-1.amazonaws.com/AUS/2020/0d16a6d796ed06a4ff09aa2f9eba53b6a149d8bf.pdf']['comprehension_q']
        dbconnx.dispose()
        return valid_filings


    def output_100_benchmark_questions(self):
        """
        Sample 100 best questions from the dataframe with specific country and time distribution.
        
        Parameters:
        df: DataFrame with columns 'country', 'date_published', 'market_materiality'
            Already filtered for date range 2022-05-01 to 2024-05-01
        
        Returns:
        DataFrame with 100 questions sampled according to requirements
        """
        df = self.qanda_filings
        # Filter date range and sort by market_materiality
        df_filtered = df[(df['date_published'] < '2024-05-01') & 
                        (df['date_published'] > '2022-05-01')].copy()
        
        # Extract year from date_published for sampling
        df_filtered['year'] = pd.to_datetime(df_filtered['date_published']).dt.year
        
        # Sort by market_materiality descending
        df_filtered = df_filtered.sort_values('market_materiality', ascending=False)
        
        # Define priority countries and other countries
        priority_countries = ['USA', 'JPN', 'EUE', 'ENG']
        other_countries = ['AUS', 'CAN', 'CHE', 'NOR', 'SWE']
        
        sampled_dfs = []
        
        # Sample 20 questions from each priority country
        for country in priority_countries:
            country_df = df_filtered[df_filtered['country'] == country]
            
            if len(country_df) >= 20:
                # Try to get roughly even distribution across years
                samples_per_year = []
                for year in [2022, 2023, 2024]:
                    year_df = country_df[country_df['year'] == year]
                    # Aim for ~7 per year (20/3 ≈ 6.67)
                    n_samples = min(7, len(year_df))
                    if n_samples > 0:
                        samples_per_year.append(year_df.head(n_samples))
                
                # Combine year samples
                country_sample = pd.concat(samples_per_year)
                
                # If we don't have 20 yet, fill from the remaining top questions
                if len(country_sample) < 20:
                    remaining_ids = set(country_df.index) - set(country_sample.index)
                    remaining_df = country_df.loc[list(remaining_ids)]
                    additional_needed = 20 - len(country_sample)
                    country_sample = pd.concat([country_sample, 
                                            remaining_df.head(additional_needed)])
            else:
                # If country has less than 20 questions, take all
                country_sample = country_df
            
            sampled_dfs.append(country_sample.head(20))
        
        # For other countries, distribute the remaining 20 questions
        other_df = df_filtered[df_filtered['country'].isin(other_countries)]
        
        if len(other_df) >= 20:
            # Try to get even distribution across years for other countries too
            other_samples_per_year = []
            for year in [2022, 2023, 2024]:
                year_df = other_df[other_df['year'] == year]
                # Aim for ~7 per year
                n_samples = min(7, len(year_df))
                if n_samples > 0:
                    other_samples_per_year.append(year_df.head(n_samples))
            
            other_sample = pd.concat(other_samples_per_year)
            
            # Fill remaining if needed
            if len(other_sample) < 20:
                remaining_ids = set(other_df.index) - set(other_sample.index)
                remaining_df = other_df.loc[list(remaining_ids)]
                additional_needed = 20 - len(other_sample)
                other_sample = pd.concat([other_sample, 
                                        remaining_df.head(additional_needed)])
        else:
            other_sample = other_df
        
        sampled_dfs.append(other_sample.head(20))
        
        # Combine all samples
        final_df = pd.concat(sampled_dfs)
        
        # Remove duplicates based on index (since df might have unhashable columns)
        final_df = final_df[~final_df.index.duplicated(keep='first')]
        
        # Ensure we have exactly 100 questions
        if len(final_df) > 100:
            final_df = final_df.head(100)
        elif len(final_df) < 100:
            # If we have less than 100, fill with any remaining high-scoring questions
            used_ids = set(final_df.index)
            remaining_df = df_filtered[~df_filtered.index.isin(used_ids)]
            additional_needed = 100 - len(final_df)
            final_df = pd.concat([final_df, remaining_df.head(additional_needed)])
        
        # Sort final dataframe by market_materiality
        final_df = final_df.sort_values('market_materiality', ascending=False)
        
        # Drop the temporary year column
        if 'year' in final_df.columns:
            final_df = final_df.drop('year', axis=1)
        
        return final_df
    
    async def generate_100_benchmark_questions(self):
        sample_qs = self.output_100_benchmark_questions()
        model = 'anthropic/claude-4-opus-20250522'
        def manufacture_question_prompt(title, country, date_published,full_doc):
            full_doc_truncated = full_doc[0:700_000]
            system_prompt = ''' you are the Top Analyst Evaluator program at Soros Capital Management.
            You ask extremely relevant ACT or SAT style questions given a document that can be administered to analysts
            on standardized tests to verify their market judgment and knowledge is accurate. You follow instructions exactly'''
            user_prompt = f'''Your job is to generate a comprehension question for the following article 
        <<<ARTICLE NAME STARTS HERE>>>
        {title}
        <<<ARTICLE NAME ENDS HERE>>>

        Which was published on 
        <<<ARTICLE PUBLISH DATE STARTS HERE>>>
        {date_published}
        <<<ARTICLE PUBLISH DATE ENDS HERE>>>

        For the country
        <<< COUNTRY CODE STARTS HERE>>>
        {country}
        <<<COUNTRY CODE ENDS HERE>>>

        Here is the full document
        <<< FULL DOCUMENT STARTS HERE>>>
        {full_doc_truncated}
        <<< FULL DOCUMENT ENDS HERE>>>

        GUIDE TO COUNTRY CODES:
        ENG - Bank of England (relevant for GBP) 
        AUS - Australia (relevant for AUD)
        NOR - Norway (relevan for NOK)
        CHE - Switzerland (relevant for CHF)
        EUE - Europe / ECB (relevant for EUR)
        JPN - Bank of Japan (relevant for JPY)
        USA - Federal Reserve (relevant to USD)
        SWE - Swedish Riksbank (relevant to SEK)
        CAN - Canada (relevant to Canada )

        Comprehension question instructions:
        1. You should roughly reference the date's month and year but not the exact date 
        - Example of what NOT to do: "Jerome Powell's March 27th speech"
        2. You should reference the Central Bank in the question 
        - For example "During his address to the US Federal Reserve, Jerome Powell ..."
        3. You should ask a question that you could only tell from reading the document
        - the question should be related to the most important point advanced in the document not the footnotes
        4. If the question has a multi part answer you should specify how many parts it has
        - bad question: "what were the drivers of the feds inflation outlook"
        - good question: "what were the 2 drivers of the Fed's inflation outlook"
        5. The question should be market relevant and require some nuance not JUST a basic fact check
        - bad question: how many BPS did the ECB cut
        - good question: how many basis points did the ECB cut in the March meeting and what were the two justifications for it
        6. The questions should not span multiple sentences. At most the question should have 2 parts 
        7. The question should have a factual answer that is relatively non controversial 
        8. The question should be able to be answered in 4 sentences or less 

        Examples of good questions:
        - In the July ECB meeting discussing Central Bank Digital Currencies, what were the primary 2 salient threats discussed?
        - In the March Federal Reserve policy minutes what reasoning was given for cutting interest rates, and by how much were they cut?

        Example of Bad questions:
        - How much QE is the BOJ doing? (does not have a date, or clear article reference)
        - When the SNB decided to take interest rates negative, what were the factors cited? (does not have a clear date, unclear number of factors)
        - Who was the Fed speaker in the March 30 meeting? (irrelevant to market )

        Output your output in the following format always with no deviation:
        | COMPREHENSION QUESTION | <your question with no elaboration > |
        | COMPREHENSION ANSWER | < the answer to this question in 4 sentences or less without unneccesary details or caveats > |
        | EXACT DOCUMENT CITATION | <key quotes from the document justifying the comprehension answer, word for weed without summarization.
        should be exact quotes that are control-f able within the document > |
        | DISCARD | < if the comprehension question and answer are not able to be generated or not worthy of including on a standardized
        test then the response here should be the string REMOVE. otherwise the response should be the string KEEP. The only acceptable outputs
        for this are REMOVE or KEEP > |
        '''

            api_arg =  {"model": model,
                        "messages": [{"role": "system", "content": system_prompt},
                                    {"role": "user", "content": user_prompt}],
                                    "temperature":0}
            return api_arg
        sample_qs['api_arg'] =sample_qs.apply(lambda x: manufacture_question_prompt(x['title'], x['country'], x['date_published'],x['full_doc']),axis=1)
        full_question_hit = sample_qs['api_arg'].to_dict()
        all_question_generation = await self.openrouter_tool.run_async_chat_completions_with_error_handling(full_question_hit)
        ymap = {}
        for xlink in all_question_generation.keys():
            ymap[xlink]= all_question_generation[xlink].choices[0].message.content
        all_questions_generated = pd.DataFrame(ymap,index=[0]).transpose()
        all_questions_generated.index.name='article'
        all_questions_generated.columns=['qandaresponse']
        all_questions_generated['question']= all_questions_generated['qandaresponse'].apply(lambda x: x.split('COMPREHENSION QUESTION |')[-1:][0].split('|')[0].strip())
        all_questions_generated['answer']= all_questions_generated['qandaresponse'].apply(lambda x: x.split('COMPREHENSION ANSWER |')[-1:][0].split('|')[0].strip())
        all_questions_generated['discard']= all_questions_generated['qandaresponse'].apply(lambda x: x.split('DISCARD |')[-1:][0].split('|')[0].strip())
        all_questions_generated['citation']= all_questions_generated['qandaresponse'].apply(lambda x: x.split('EXACT DOCUMENT CITATION |')[-1:][0].split('|')[0].strip())
        all_questions_generated= all_questions_generated[all_questions_generated['discard']!='REMOVE'].copy()
        import datetime
        all_questions_generated['datetime']=datetime.datetime.now()
        all_questions_generated['model']='anthropic/claude-4-opus-20250522'
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user('agti_corp')
        all_questions_generated.to_sql('agti_qanda',dbconnx, if_exists='append')




    async def power_query_step_1__reduce_filings(self, user_query):

        xfilter = await self.output_key_query_info(user_query)
        slice1 = self.qanda_filings[(self.qanda_filings['date_published'] >= xfilter['start_date']) & (self.qanda_filings['date_published'] <= xfilter['end_date'])]
        reduced_filings = slice1[slice1['country'].apply(lambda x: x in xfilter['countr_list'])].copy()
        
        bulk_summary_filter = xfilter['bulk_summary']
        def create_user_index(vector_info_string, article_ref_string):
            op = f"""<<<ARTICLE REF {article_ref_string} STARTS HERE>>>
            {vector_info_string}
            <<<ARTICLE REF {article_ref_string} ENDS HERE>>>
            """
            return op
        reduced_filings['search_dex']= reduced_filings.apply(lambda x: create_user_index(x['vector_info'],x['article_ref']),axis=1)
        reduced_filings['search_dex_length']=reduced_filings['search_dex'].apply(lambda x: len(x))
        reduced_filings.sort_values('date_published',ascending=False,inplace=True)#[['date_published','search_dex','search_dex_length']]
        reduced_filings=reduced_filings[reduced_filings['search_dex_length'].cumsum()/4 <= 850_000]
        print('ran reduced filings')
        xfilter['user_query']=user_query
        return {'reduced_filings': reduced_filings,
                'query_metadata':xfilter}


    async def power_query_step_2__reduce_filings(self, step_1_map):
        reduced_filings = step_1_map['reduced_filings']
        user_query = step_1_map['query_metadata']['user_query']
        article_ref_guide = '\n'.join(list(reduced_filings['search_dex']))
        bulk_summary_filter= step_1_map['query_metadata']['bulk_summary']

        non_bulk_system_prompt = ''' You are the filter analyst. Your job is to reduce all possible articles about a topic down to the 3 most relevant articles 
        You always output your data in the specified format
        '''
        non_bulk_user_prompt = f""" Your job is to follow the attached instructions to output a list of integer article references most topical to the user's query

        <<< USER QUERY STARTS HERE >>> 
        {user_query}
        <<<USER QUERY ENDS HERE>>>

        Please take the following article ref guide

        XXXOOOXXXOOO___ARTICLE REF GUIDE STARTS HERE___XXXOOOXXXOOO
        {article_ref_guide}
        XXXOOOXXXOOO___ARTICLE REF GUIDE ENDS HERE___XXXOOOXXXOOO

        output a list of article refs that are needed to answer the user query effectively. There can be up to 3 articles 
        in the list. provide the article list which best provides the answer to the query. Do not return articles that are not relevant. 
        if there are not 3 articles you can return fewer in the list 

        Always provide your final output in the following format: 
        | ARTICLE LIST | comma delimited list of article refs|
        """

        bulk_system_prompt = ''' You are the filter analyst. Your job is to reduce all possible articles about a topic down to all the most relevant articles 
        You always output your data in the specified format
        '''
        bulk_user_prompt = f""" Your job is to follow the attached instructions to output a list of integer article references most topical to the user's query

        <<< USER QUERY STARTS HERE >>> 
        {user_query}
        <<<USER QUERY ENDS HERE>>>

        Please take the following article ref guide

        XXXOOOXXXOOO___ARTICLE REF GUIDE STARTS HERE___XXXOOOXXXOOO
        {article_ref_guide}
        XXXOOOXXXOOO___ARTICLE REF GUIDE ENDS HERE___XXXOOOXXXOOO

        output a list of article refs that are needed to answer the user query effectively. There can be up to 100 articles 
        in the list. provide the article list which best provides the answer to the query

        Guidelines:
        * if the user is asking for bulk information such as the behavior of multiple central banks then keep at least 1 article
        per bank requested
        * The default list of banks is all G10 central banks bank
        * if you are providing multiple articles per central bank have a good reason for doing so otherwise provide the most
        recent article per bank (a good reason for doing so might include comparing the last 2 BOJ articles)

        Always provide your final output in the following format: 
        | ARTICLE LIST | comma delimited list of article refs|
        """
        proper_system_prompt = bulk_system_prompt
        proper_user_prompt = bulk_user_prompt
        if bulk_summary_filter == 'NO':
            proper_system_prompt = non_bulk_system_prompt
            proper_user_prompt = non_bulk_user_prompt


        api_arg =  {
                    "model": 'google/gemini-2.5-pro',
                    "messages": [
                        {"role": "system", "content": proper_system_prompt},
                        {"role": "user", "content": proper_user_prompt}
                    ],
                        "temperature":0
                    }


        classifier = {'run1':api_arg}
        open_router_output= await self.openrouter_tool.run_async_chat_completions_with_error_handling(classifier)

        bulk_output = open_router_output['run1'].choices[0].message.content
        extracted_article_list =[]
        if bulk_summary_filter == 'YES':
            all_articles = bulk_output.split('ARTICLE LIST |')[-1:][0].replace('|','').strip().split(',')
        if bulk_summary_filter == 'NO':
            all_articles = bulk_output.split('ARTICLE LIST |')[-1:][0].replace('|','').strip().split(',')
        step_2_map = {'bulk_output':bulk_output,
                        'bulk_summary': bulk_summary_filter,
                        'extracted_article_list': all_articles,
                        'reduced_filings':reduced_filings,
                        'user_query':user_query}
        return step_2_map

    async def step3_non_bulk_answer(self, step_2_map):
        # non bulk summary code 
        step_2_map['bulk_summary'] == 'NO'
        reduced_filings = step_2_map['reduced_filings']
        three_filings_to_run = reduced_filings[reduced_filings['article_ref'].apply(lambda x: str(x) 
                                                            in step_2_map['extracted_article_list'])].copy()
        user_query = step_2_map['user_query']
        def construct_api_args(full_extracted_text):
            full_extracted_text = full_extracted_text[0:700_000]
            system_prompt = ''' You are the worlds foremost macro analyst. Your job is to take in content from a document
            and answer user queries exactly correctly in a concise but well warranted format 
            '''
            user_prompt = f'''You are given the following user prompt
            <<< USER PROMPT STARTS HERE >>>
            {user_query}
            <<<USER PROMPT ENDS HERE >>>

            Your job is to answer the question succinctly using the full extracted text
            <<<FULL EXTRACTED TEXT STARTS HERE>>>
            {full_extracted_text}
            <<< FULL EXTRACTED TEXT ENDS HERE>>>

            When providing answers:
            1. Be detail oriented
            2. answer the entirety of the user prompt and do not skip parts 
            3. do not make up information 

            Always provide your output in a pipe delimited answer field
            | RESTATE USER PROMPT | < include the exact user prompt >
            | ANSWER TO USER PROMPT | < provide your answer to the user prompt in less than 15 sentences. be as terse as possible
            while still being accurate > 
            '''
            api_arg =  {
                    "model": 'anthropic/claude-opus-4',
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                        "temperature":0
                    }
            return api_arg
        three_filings_to_run['api_args']=three_filings_to_run['full_extracted_text'].apply(lambda x: construct_api_args(full_extracted_text=x))#.to_dict()
        three_filings_to_run['standardized_ref_output']= reduced_filings['title'] +', '+ reduced_filings['date_published'].astype(str)+', '+reduced_filings['country']+', '+reduced_filings['dex_copy']
        answers_to_work = three_filings_to_run['api_args'].to_dict()
        open_router_output= await self.openrouter_tool.run_async_chat_completions_with_error_handling(answers_to_work)
        ymap = {}
        for xkey in open_router_output.keys():
            ymap[xkey]= open_router_output[xkey].choices[0].message.content.split('| ANSWER TO USER PROMPT |')[-1:][0].strip()
        three_filings_to_run['extracted_answer']=ymap
        three_filings_to_run['full_answer_blob']= 'REFERENCE: '+three_filings_to_run['standardized_ref_output'] + ' EXTRACTED ANSWER: '+ three_filings_to_run['extracted_answer']
        full_answer_blob = '\n'.join(list(three_filings_to_run['full_answer_blob']))
        system_prompt = ''' You are the worlds foremost macro analyst. You have been given a list of document references along with extracted answers to a user query.
        Your job is to clean up a unified answer to the user query referencing the sources provided'''
        user_promt = f'''Below you have been provided with a list of references and answers to the user query
        <<<USER QUERY STARTS HERE>>>
        {user_query}
        <<< USER QUERY ENDS HERE>>>

        <<<EXTRACTED ANSWERS START HERE>>>
        {full_answer_blob}
        <<<EXTRACTED ANSWERS END HERE>>>

        Your job is to take the above and:
        1] answer the user query correctly in a definitive way
        - pay special attention to exactly what the question is asking and provide the correct answers for all parts
        - if the question asks for statistics return the correct statistics
        - if the question asks for justifications return justifications 
        - DO NOT MAKE UP INFORMATION - your answer should be from the extracted answers
        2] explain your answer succinctly and clear up any ambiguity in the answer if that is neccesary (for example -
        conflicting points, insufficient data or multiple events)
        You do not need to cite sources as annotations will be added to the bottom of your response 
        '''
        api_arg =  {
                "model": 'anthropic/claude-opus-4',
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_promt}
                ],
                    "temperature":0
                }
        classifier = {'run1':api_arg}
        open_router_output= await self.openrouter_tool.run_async_chat_completions_with_error_handling(classifier)
        references = '\n'.join(list(three_filings_to_run['standardized_ref_output']))
        final_output = open_router_output['run1'].choices[0].message.content + '''
        REFERENCES:
        '''+references
        return final_output

    async def step3_bulk_answer(self, step_2_map):
        user_query =step_2_map['user_query']
        reduced_filings = step_2_map['reduced_filings']
        all_articles = list(step_2_map['extracted_article_list'])

        reduced_filings['standardized_ref_output']= reduced_filings['title'] +', '+ reduced_filings['date_published'].astype(str)+', '+reduced_filings['country']+', '+reduced_filings['dex_copy']

        extractions_only = reduced_filings[reduced_filings['article_ref'].astype(str).apply(lambda x: x in all_articles)].copy()
        extractions_only['article_ref'] = range(0,len(extractions_only))
        extractions_only['article_ref']=extractions_only['article_ref']+1
        extractions_only['full_ref']= extractions_only['article_ref'].astype(str)+'. '+extractions_only['standardized_ref_output']

        full_appendix_ref = '\n'.join(list(extractions_only['full_ref']))
        all_summary_text = '\n'.join(list(extractions_only['extracted_info']))
        system_answer_prompt = 'You are the worlds most effective macro analyst'
        user_prompt = f'''Please take the following user query 
        <<< USER QUERY STARTS HERE >>>
        {user_query}
        <<<USER QUERY ENDS HERE>>>

        And answer it with the full document dump
        <<< FULL DOCUMENT DUMP STARTS HERE >>>
        {all_summary_text}
        <<< FULL DOCUMENT DUMP ENDS HERE >>>

        <<< FORMATTED SOURCE APPENDIX STARTS HERE >>>
        {full_appendix_ref}
        <<< FORMATTED SOURCE APPENDIX ENDS HERE >>>

        And answer the query with the full document dump. 
        Tone for your answer:
        * Cite all key quantitative stats precisely and walk through their implications
        * Be thorough. Assume you are talking to a top performer like Stan Druckenmiller or George Soros who is used to
        sophisticated analysis and will penalize you for omitting key information 
        * Do not provide disclaimers 
        * Answer the query directly and read between the lines and also include important meta context 
        that might help the user ask better question

        In your answers use the appropriate citation (1) or (2) etc to match the appendix.
        '''

        api_arg =  {
                    "model": 'google/gemini-2.5-pro',
                    "messages": [
                        {"role": "system", "content": system_answer_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                        "temperature":0
                    }


        classifier = {'run1':api_arg}
        op= await self.openrouter_tool.run_async_chat_completions_with_error_handling(classifier)
        batch_output = op['run1'].choices[0].message.content
        output = batch_output+ '''

        APPENDIX
        '''+full_appendix_ref
        return output
    async def output_full_question_response(self,user_query="what is the tone from central banks lately about cuts"):
        step_1_map = await self.power_query_step_1__reduce_filings(user_query =user_query)
        step_2_map= await self.power_query_step_2__reduce_filings(step_1_map=step_1_map)
        #if step_2_map
        if step_2_map['bulk_summary'] == "NO":
            step_3_map = await self.step3_non_bulk_answer(step_2_map=step_2_map)

        if step_2_map['bulk_summary'] == "YES":
            step_3_map = await self.step3_bulk_answer(step_2_map=step_2_map)
        return step_3_map
    
    async def output_full_question_response_batch(
        self, 
        questions, 
        batch_size=5,
        delay_between_batches=0.0
    ):
        """
        Process multiple questions in batches using output_full_question_response.
        
        Parameters:
        -----------
        questions : list
            List of questions to process
        batch_size : int, default=5
            Number of questions to process concurrently in each batch
        delay_between_batches : float, default=0.0
            Optional delay in seconds between processing batches (for rate limiting)
        
        Returns:
        --------
        dict
            Dictionary containing:
            - 'results': List of results for each question
            - 'errors': List of any errors encountered
            - 'metadata': Processing metadata (timing, success rate, etc.)
        """
        
        # Initialize tracking variables
        all_results = []
        all_errors = []
        start_time = datetime.datetime.now()
        
        # Process questions in batches
        for i in range(0, len(questions), batch_size):
            batch_start_time = datetime.datetime.now()
            batch_questions = questions[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(questions) + batch_size - 1) // batch_size
            
            print(f"Processing batch {batch_num}/{total_batches} with {len(batch_questions)} questions...")
            
            # Create tasks for all questions in the current batch
            tasks = []
            for idx, question in enumerate(batch_questions):
                task_id = i + idx
                task = asyncio.create_task(
                    self._process_single_question_with_error_handling(question, task_id)
                )
                tasks.append(task)
            
            # Wait for all tasks in the batch to complete
            batch_results = await asyncio.gather(*tasks)
            
            # Process batch results
            for result in batch_results:
                if result['success']:
                    all_results.append(result)
                else:
                    all_errors.append(result)
            
            batch_elapsed = (datetime.datetime.now() - batch_start_time).total_seconds()
            print(f"Batch {batch_num} completed in {batch_elapsed:.2f} seconds")
            
            # Add delay between batches if specified (except for the last batch)
            if delay_between_batches > 0 and i + batch_size < len(questions):
                print(f"Waiting {delay_between_batches} seconds before next batch...")
                await asyncio.sleep(delay_between_batches)
        
        # Calculate summary statistics
        total_elapsed = (datetime.datetime.now() - start_time).total_seconds()
        success_count = len(all_results)
        error_count = len(all_errors)
        success_rate = success_count / len(questions) if questions else 0
        
        # Prepare output
        output = {
            'results': all_results,
            'errors': all_errors,
            'metadata': {
                'total_questions': len(questions),
                'successful': success_count,
                'failed': error_count,
                'success_rate': success_rate,
                'total_time_seconds': total_elapsed,
                'avg_time_per_question': total_elapsed / len(questions) if questions else 0,
                'batch_size': batch_size,
                'timestamp': datetime.datetime.now().isoformat()
            }
        }
        
        print(f"\nBatch processing complete:")
        print(f"  Total questions: {len(questions)}")
        print(f"  Successful: {success_count}")
        print(f"  Failed: {error_count}")
        print(f"  Success rate: {success_rate:.2%}")
        print(f"  Total time: {total_elapsed:.2f} seconds")
        print(f"  Average time per question: {output['metadata']['avg_time_per_question']:.2f} seconds")
        
        return output


    async def _process_single_question_with_error_handling(
        self, 
        question, 
        task_id
    ):
        """
        Process a single question with error handling.
        
        Parameters:
        -----------
        question : str
            The question to process
        task_id : int
            Unique identifier for this task
        
        Returns:
        --------
        dict
            Result dictionary with success status and either result or error
        """
        try:
            start_time = datetime.datetime.now()
            result = await self.output_full_question_response(user_query=question)
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            
            return {
                'success': True,
                'task_id': task_id,
                'question': question,
                'result': result,
                'processing_time': elapsed,
                'timestamp': datetime.datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'success': False,
                'task_id': task_id,
                'question': question,
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.datetime.now().isoformat()
            }


    async def write_agti_processed_questions(self):
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user('agti_corp')
        core_qanda = pd.read_sql('agti_qanda',dbconnx)
        allqanda= await self.output_full_question_response_batch(questions=list(core_qanda['question']), batch_size=5,
                delay_between_batches=1)
        processing_run = pd.DataFrame(allqanda['results'])
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        processing_run.to_sql('agti_system_qanda_responses',dbconnx, if_exists='append')