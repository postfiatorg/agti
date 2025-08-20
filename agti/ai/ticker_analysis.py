import pandas as pd
import asyncio
import nest_asyncio
from collections import Counter
import datetime
import json
from agti.ai.openrouter import OpenRouterTool
from agti.utilities.settings import PasswordMapLoader

class TickerAnalyzer:
    """
    Analyze ticker symbol choices using AI models via OpenRouter.
    """
    
    def __init__(self, pw_map):
        """Initialize with password map for OpenRouter."""
        self.pw_map = pw_map
        self.openrouter_tool = OpenRouterTool(pw_map=self.pw_map, max_concurrent_requests=10)
        
    def create_ticker_prompt(self):
        """Create the ticker analysis prompt."""
        return """The year is 2026. there is a new version of XRP called Post Fiat that is decentralized via AI selecting the unique node list fairly. its use case is the investment bank not the transaction bank, which has far less compliance issues than replacing swift. what is the estimated market cap of this coin (use best estimate, market cap not FDV). do not use web search

Ticker ideas:
The year is 2026. there is a new version of XRP called Post Fiat that is decentralized via AI selecting the unique node list fairly. its use case is the investment bank not the transaction bank, which has far less compliance issues than replacing swift. what is the estimated market cap of this coin (use best estimate, market cap not FDV). do not use web search

Ticker 1:
XRPF
Comments: clearly identifies this as an XRP fork 

Ticker 2:
XP
Comment: shows progression of user from doing tasks / growing with Post Fiat economy 

Ticker 3:
PFT 
Comment: this is the existing ticker of the testnet token

Ticker 4:
XPFT
Comment: X+ existing ticker 

Please output your choice for the ticker with justification. Your response should be in the following format:
CHOSEN TICKER: [Your choice from XRPF, XP, PFT, or XPFT]
JUSTIFICATION: [Your reasoning in 2-3 sentences]
ESTIMATED MARKET CAP: [Your estimate in billions USD]"""
    
    def create_api_args_map(self, num_runs=25, model="anthropic/claude-opus-4", temperature=0.8):
        """Create API arguments for multiple runs."""
        prompt = self.create_ticker_prompt()
        system_prompt = "You are a cryptocurrency and financial markets expert analyst."
        
        api_args_map = {}
        for i in range(num_runs):
            api_args_map[f"run_{i+1}"] = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "temperature": temperature
            }
        
        return api_args_map
    
    async def run_analysis(self, num_runs=25, model="anthropic/claude-opus-4", temperature=0.8):
        """Run the ticker analysis multiple times and collect results."""
        print(f"Starting ticker analysis with {num_runs} runs using model: {model}")
        
        # Create API arguments
        api_args_map = self.create_api_args_map(num_runs, model, temperature)
        
        # Run async requests with error handling
        results = await self.openrouter_tool.run_async_chat_completions_with_error_handling(
            arg_async_map=api_args_map
        )
        
        # Process results
        processed_results = []
        for run_id, result in results.items():
            try:
                if hasattr(result, 'choices') and result.choices:
                    content = result.choices[0].message.content
                    
                    # Extract ticker choice
                    ticker = None
                    if "CHOSEN TICKER:" in content:
                        ticker_line = content.split("CHOSEN TICKER:")[1].split("\n")[0].strip()
                        # Clean up the ticker
                        for t in ["XRPF", "XP", "PFT", "XPFT"]:
                            if t in ticker_line:
                                ticker = t
                                break
                    
                    # Extract justification
                    justification = ""
                    if "JUSTIFICATION:" in content:
                        justification = content.split("JUSTIFICATION:")[1].split("ESTIMATED MARKET CAP:")[0].strip()
                    
                    # Extract market cap estimate
                    market_cap = ""
                    if "ESTIMATED MARKET CAP:" in content:
                        market_cap = content.split("ESTIMATED MARKET CAP:")[1].strip()
                    
                    processed_results.append({
                        'run_id': run_id,
                        'ticker': ticker,
                        'justification': justification,
                        'market_cap_estimate': market_cap,
                        'full_response': content,
                        'status': 'success'
                    })
                else:
                    processed_results.append({
                        'run_id': run_id,
                        'ticker': None,
                        'justification': 'Error: No response',
                        'market_cap_estimate': None,
                        'full_response': str(result),
                        'status': 'error'
                    })
            except Exception as e:
                processed_results.append({
                    'run_id': run_id,
                    'ticker': None,
                    'justification': f'Error processing: {str(e)}',
                    'market_cap_estimate': None,
                    'full_response': str(result),
                    'status': 'error'
                })
        
        # Create DataFrame
        df = pd.DataFrame(processed_results)
        
        # Calculate statistics
        successful_runs = df[df['status'] == 'success'].shape[0]
        ticker_counts = df[df['ticker'].notna()]['ticker'].value_counts()
        
        print(f"\nAnalysis complete!")
        print(f"Successful runs: {successful_runs}/{num_runs}")
        print(f"\nTicker selection distribution:")
        print(ticker_counts)
        
        return df, ticker_counts
    
    def summarize_results(self, df):
        """Create a summary of the analysis results."""
        summary = {
            'timestamp': datetime.datetime.now().isoformat(),
            'total_runs': len(df),
            'successful_runs': df[df['status'] == 'success'].shape[0],
            'ticker_distribution': df[df['ticker'].notna()]['ticker'].value_counts().to_dict(),
            'winner': df[df['ticker'].notna()]['ticker'].mode()[0] if not df[df['ticker'].notna()].empty else None,
            'sample_justifications': {}
        }
        
        # Get sample justifications for each ticker
        for ticker in ['XRPF', 'XP', 'PFT', 'XPFT']:
            ticker_df = df[df['ticker'] == ticker]
            if not ticker_df.empty:
                summary['sample_justifications'][ticker] = ticker_df.iloc[0]['justification']
        
        return summary
    
    def save_results(self, df, summary, filename_prefix="ticker_analysis"):
        """Save results to files."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save detailed results
        df.to_csv(f"{filename_prefix}_detailed_{timestamp}.csv", index=False)
        
        # Save summary
        with open(f"{filename_prefix}_summary_{timestamp}.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\nResults saved to:")
        print(f"- {filename_prefix}_detailed_{timestamp}.csv")
        print(f"- {filename_prefix}_summary_{timestamp}.json")


async def main():
    """Main function to run ticker analysis."""
    # Initialize password map
    password_map_loader = PasswordMapLoader()
    
    # Create analyzer
    analyzer = TickerAnalyzer(pw_map=password_map_loader.pw_map)
    
    # Run analysis
    df, ticker_counts = await analyzer.run_analysis(
        num_runs=25,
        model="anthropic/claude-opus-4",
        temperature=0.8
    )
    
    # Create summary
    summary = analyzer.summarize_results(df)
    
    # Print final summary
    print("\n" + "="*50)
    print("FINAL SUMMARY")
    print("="*50)
    print(f"Winning ticker: {summary['winner']}")
    print(f"\nDetailed distribution:")
    for ticker, count in sorted(summary['ticker_distribution'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / summary['successful_runs']) * 100
        print(f"  {ticker}: {count} votes ({percentage:.1f}%)")
    
    # Save results
    analyzer.save_results(df, summary)
    
    return df, summary


if __name__ == "__main__":
    # Apply nest_asyncio to allow running in Jupyter notebooks
    nest_asyncio.apply()
    
    # Run the analysis
    loop = asyncio.get_event_loop()
    df, summary = loop.run_until_complete(main())