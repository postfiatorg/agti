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

class CentralBankPDFProcessor:
    def __init__(self, pw_map: Dict, user_name: str = 'agti_corp'):
        """Initialize with password map and user."""
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_conn_manager = DBConnectionManager(pw_map)
        self.dbconn = None
        self.data = None
        
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
        valid_g10 = pd.read_sql('central_banks_g10', self.dbconn)
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
        
        # Check if table exists
        try:
            existing_df = pd.read_sql(f"SELECT 1 FROM {table_name} LIMIT 1", self.dbconn)
            table_exists = True
        except:
            table_exists = False
        
        # Save to database (append if exists, create if not)
        self.data.to_sql(
            table_name,
            self.dbconn,
            if_exists='append' if table_exists else 'replace',
            index=False,
            method='multi',
            chunksize=chunksize
        )
        
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

            openrouter_tool = OpenRouterTool(pw_map=password_map_loader.pw_map,max_concurrent_requests=50)
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
