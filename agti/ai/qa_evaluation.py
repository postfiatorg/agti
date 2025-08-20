import pandas as pd
from typing import Dict, Any, Optional, List
import asyncio
from dataclasses import dataclass

@dataclass
class ModelConfig:
    """Configuration for a model."""
    name: str
    model_id: str
    requires_web_search: bool = False

class QAEvaluationSystem:
    """
    A system for evaluating Q&A responses from different AI models and search tools.
    
    This class handles:
    - Loading Q&A data from database
    - Generating responses from multiple models (Opus, O3, etc.)
    - Web search augmentation (Anthropic and OpenAI)
    - Scoring responses against ground truth
    """
    
    def __init__(self, pw_map: Dict[str, Any]):
        """
        Initialize the Q&A Evaluation System.
        
        Args:
            pw_map: Password map for authentication
        """
        self.pw_map = pw_map
        self._initialize_tools()
        self._configure_models()
        
    def _initialize_tools(self):
        """Initialize all required tools and connections."""
        from agti.agti_queries.augmented_summaries import CentralBankPDFProcessor
        from agti.utilities.db_manager import DBConnectionManager
        from agti.ai.openai_web import OpenAIResponsesTool
        from agti.ai.anthropic_web import AnthropicWebSearchTool
        
        # Initialize core processor
        self.pdf_processor = CentralBankPDFProcessor(pw_map=self.pw_map)
        self.db_conn_manager = self.pdf_processor.db_conn_manager
        self.openrouter_tool = self.pdf_processor.openrouter_tool
        
        # Initialize web search tools
        self.openai_web_search = OpenAIResponsesTool(pw_map=self.pw_map)
        self.anthropic_web_search = AnthropicWebSearchTool(pw_map=self.pw_map)
        
    def _configure_models(self):
        """Configure the models to be evaluated."""
        self.models = {
            'opus': ModelConfig('opus', 'anthropic/claude-opus-4'),
            'o3': ModelConfig('o3', 'openai/o3'),
            'anthropic_web': ModelConfig('anthropic_web', 'claude-opus-4-20250514', requires_web_search=True),
            'openai_web': ModelConfig('openai_web', 'gpt-4o', requires_web_search=True)
        }
        
    def load_qanda_data(self) -> pd.DataFrame:
        """
        Load Q&A data from the database.
        
        Returns:
            DataFrame containing questions and answers
        """
        dbconnx = self.db_conn_manager.spawn_sqlalchemy_db_connection_for_user('agti_corp')
        core_qanda = pd.read_sql('agti_qanda', dbconnx)
        
        # Load existing AGTI responses
        agti_responses = pd.read_sql('agti_system_qanda_responses', dbconnx)
        core_qanda['agti_response'] = core_qanda['question'].map(
            agti_responses.groupby('question').first()['result']
        )
        
        # Load AGTI vector responses (last response per question)
        core_qanda['agti_vector_response'] = core_qanda['question'].map(
            agti_responses.groupby('question').last()['result']
        )
        
        dbconnx.dispose()
        return core_qanda
    
    def create_prompt(self, question: str, model: str) -> Dict[str, Any]:
        """
        Create a standardized prompt for a given question and model.
        
        Args:
            question: The question to answer
            model: The model identifier
            
        Returns:
            API arguments dictionary
        """
        system_prompt = 'You are a helpful AI assistant'
        user_prompt = f'Please answer the following user question {question} using knowledge from your training data'
        
        return {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0
        }
    
    async def generate_raw_responses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate raw responses from non-web-search models.
        
        Args:
            df: DataFrame with questions
            
        Returns:
            DataFrame with raw responses added
        """
        # Create API arguments for each model
        df['opus_api_arg'] = df.apply(
            lambda x: self.create_prompt(x['question'], self.models['opus'].model_id), 
            axis=1
        )
        df['o3_api_arg'] = df.apply(
            lambda x: self.create_prompt(x['question'], self.models['o3'].model_id), 
            axis=1
        )
        
        # Run Opus
        opus_run = await self.openrouter_tool.run_async_chat_completions_with_error_handling(
            df.set_index('question')['opus_api_arg'].to_dict()
        )
        opus_responses = {
            question: result.choices[0].message.content 
            for question, result in opus_run.items()
        }
        df['opus_raw'] = df['question'].map(opus_responses)
        
        # Run O3
        o3_run = await self.openrouter_tool.run_async_chat_completions_with_error_handling(
            df.set_index('question')['o3_api_arg'].to_dict()
        )
        o3_responses = {
            question: result.choices[0].message.content 
            for question, result in o3_run.items()
        }
        df['o3_raw'] = df['question'].map(o3_responses)
        
        return df
    
    async def generate_web_search_responses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate responses using web search augmentation.
        
        Args:
            df: DataFrame with questions
            
        Returns:
            DataFrame with web search responses added
        """
        web_search_dict = df['question'].to_dict()
        
        # Anthropic web search
        anthropic_results = self.anthropic_web_search.execute_bulk_web_search_rate_constrained(
            queries_dict=web_search_dict,
            model=self.models['anthropic_web'].model_id,
            batch_size=10,
            max_tokens=5000,
            max_uses=5,
            delay_between_batches=60
        )
        df['anthropic_web'] = df['question'].map(
            anthropic_results.groupby('query').first()['clean_response']
        )
        
        # OpenAI web search
        openai_results = self.openai_web_search.execute_bulk_web_search_rate_constrained(
            queries_dict=web_search_dict,
            model=self.models['openai_web'].model_id,
            batch_size=10,
            instructions="You are a helpful AI assistant",
            temperature=0.1,
            delay_between_batches=1.0
        )
        df['openai_web'] = df['question'].map(
            openai_results.groupby('query').first()['clean_response']
        )
        
        return df
    
    def create_scoring_prompt(self, question: str, formal_answer: str, provided_answer: str) -> Dict[str, Any]:
        """
        Create a prompt for scoring a provided answer against the correct answer.
        
        Args:
            question: The original question
            formal_answer: The correct answer
            provided_answer: The answer to be scored
            
        Returns:
            API arguments for scoring
        """
        system_prompt = '''You are the Macro Grading System. You are a harsh but accurate grader of information provided.
You follow instructions exactly and output the provided answer'''
        
        user_prompt = f'''You are provided the following question 
<<< QUESTION STARTS HERE >>> 
{question}
<<<QUESTION ENDS HERE>>>

The correct answer to the question is 
<<<ANSWER STARTS HERE>>>
{formal_answer}
<<<ANSWER ENDS HERE>>>

The user provided answer to the question is
<<<PROVIDED ANSWER STARTS HERE>>>
{provided_answer}
<<<PROVIDED ANSWER ENDS HERE>>>

Please score the users answer on correctness from 0-100 where 0 is completely wrong and 100
is completely correct
output your score in the following format
Elements in scoring:
* higher score for answers that are factually aligned with the formal answer
* higher score for answers that are qualitatively aligned with the formal answer
* lower score for omissions of key details
* lower score for getting things qualitatively incorrect 

| SCORE JUSTIFICATION | 4-5 sentences elaborating on the score you provided citing specific reasons |
| ACTUAL SCORE | <integer from 1-100 that represents the users score answering the question>
'''
        
        return {
            "model": 'google/gemini-2.5-pro',
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0
        }
    
    async def score_responses(self, df: pd.DataFrame, response_columns: List[str]) -> pd.DataFrame:
        """
        Score all responses against ground truth answers.
        
        Args:
            df: DataFrame with questions, answers, and responses
            response_columns: List of column names containing responses to score
            
        Returns:
            DataFrame with scores added
        """
        for col in response_columns:
            # Skip if column doesn't exist or has no data
            if col not in df.columns or df[col].isna().all():
                print(f"Skipping {col} - no data available")
                continue
                
            # Create scoring API arguments
            df[f'{col}_scoring_api_args'] = df.apply(
                lambda x: self.create_scoring_prompt(x['question'], x['answer'], x[col]) if pd.notna(x[col]) else None,
                axis=1
            )
            
            # Filter out None values for API call
            valid_scoring = df[df[f'{col}_scoring_api_args'].notna()]
            
            if len(valid_scoring) == 0:
                print(f"No valid data to score for {col}")
                continue
                
            # Run scoring
            scoring_results = await self.openrouter_tool.run_async_chat_completions_with_error_handling(
                valid_scoring.set_index('question')[f'{col}_scoring_api_args'].to_dict()
            )
            
            # Extract results
            df[f'{col}__score__full_api'] = df['question'].map(scoring_results)
            
            # Parse scores
            df[f'{col}__score_string'] = df[f'{col}__score__full_api'].apply(
                lambda x: x.choices[0].message.content if hasattr(x, 'choices') else ''
            )
            
            df[f'{col}__score'] = df[f'{col}__score_string'].apply(
                lambda x: x.split('ACTUAL SCORE |')[-1:][0].replace('|', '').strip() if x else '0'
            )
            
            df[f'{col}__score_justification'] = df[f'{col}__score_string'].apply(
                lambda x: x.split('SCORE JUSTIFICATION |')[-1:][0].split('|')[0] if x else ''
            )
            
            # Clean up temporary column
            df = df.drop(columns=[f'{col}_scoring_api_args'])
        
        # Convert scores to numeric
        for col in response_columns:
            score_col = f'{col}__score'
            if score_col in df.columns:
                df[score_col] = pd.to_numeric(df[score_col], errors='coerce')
        
        return df
    
    def create_summary_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create a summary report with key metrics.
        
        Args:
            df: DataFrame with all results
            
        Returns:
            Summary DataFrame
        """
        score_columns = [
            'agti_response__score',
            'agti_vector_response__score',  # Added vector response
            'anthropic_web__score',
            'openai_web__score',
            'o3_raw__score',
            'opus_raw__score'
        ]
        
        # Calculate average scores
        avg_scores = {}
        for col in score_columns:
            if col in df.columns:
                avg_scores[col.replace('__score', '')] = df[col].mean()
        
        # Create summary report
        summary = pd.DataFrame([avg_scores]).T
        summary.columns = ['Average Score']
        summary = summary.sort_values('Average Score', ascending=False)
        
        return summary
    
    async def run_full_evaluation(self) -> Dict[str, pd.DataFrame]:
        """
        Run the complete evaluation pipeline.
        
        Returns:
            Dictionary containing:
            - 'full_results': Complete results DataFrame
            - 'summary': Summary statistics
            - 'detailed_scores': Detailed scoring breakdown
        """
        # Load data
        print("Loading Q&A data...")
        df = self.load_qanda_data()
        
        # Generate responses
        print("Generating raw model responses...")
        df = await self.generate_raw_responses(df)
        
        print("Generating web search responses...")
        df = await self.generate_web_search_responses(df)
        
        # Score all responses (including agti_vector_response)
        print("Scoring responses...")
        response_columns = [
            'anthropic_web', 
            'openai_web', 
            'o3_raw', 
            'opus_raw', 
            'agti_response',
            'agti_vector_response'  # Added vector response
        ]
        df = await self.score_responses(df, response_columns)
        
        # Create reports
        print("Creating summary report...")
        summary = self.create_summary_report(df)
        
        # Create detailed scores view
        score_cols = []
        for resp_col in response_columns:
            if resp_col in df.columns:
                score_cols.extend([resp_col, f'{resp_col}__score'])
        
        detailed_scores = df[['article', 'question', 'answer'] + score_cols].copy()
        
        return {
            'full_results': df,
            'summary': summary,
            'detailed_scores': detailed_scores
        }
    
    def save_results(self, results: Dict[str, pd.DataFrame], prefix: str = "qa_evaluation"):
        """
        Save evaluation results to files.
        
        Args:
            results: Dictionary containing evaluation results
            prefix: Prefix for output filenames
        """
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save full results
        results['full_results'].to_csv(f"{prefix}_full_{timestamp}.csv", index=False)
        
        # Save summary
        results['summary'].to_csv(f"{prefix}_summary_{timestamp}.csv")
        
        # Save detailed scores
        results['detailed_scores'].to_csv(f"{prefix}_scores_{timestamp}.csv", index=False)
        
        print(f"Results saved with timestamp: {timestamp}")