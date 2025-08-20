#!/usr/bin/env python3
"""
Simplified ticker analysis script that uses OpenRouter API directly.
"""

import asyncio
import pandas as pd
from collections import Counter
import datetime
import json
from openai import AsyncOpenAI
import nest_asyncio
from tqdm import tqdm

class SimpleTickerAnalyzer:
    def __init__(self, openrouter_api_key):
        """Initialize with OpenRouter API key."""
        self.client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
        )
        
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
    
    async def get_single_response(self, run_id, model="anthropic/claude-opus-4", temperature=0.0):
        """Get a single response from the API."""
        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a cryptocurrency and financial markets expert analyst."},
                    {"role": "user", "content": self.create_ticker_prompt()}
                ],
                temperature=temperature
            )
            
            content = response.choices[0].message.content
            
            # Extract ticker choice
            ticker = None
            if "CHOSEN TICKER:" in content:
                ticker_line = content.split("CHOSEN TICKER:")[1].split("\n")[0].strip()
                for t in ["XRPF", "XP", "PFT", "XPFT"]:
                    if t in ticker_line:
                        ticker = t
                        break
            
            # Extract justification
            justification = ""
            if "JUSTIFICATION:" in content:
                justification = content.split("JUSTIFICATION:")[1].split("ESTIMATED MARKET CAP:")[0].strip()
            
            # Extract market cap
            market_cap = ""
            if "ESTIMATED MARKET CAP:" in content:
                market_cap = content.split("ESTIMATED MARKET CAP:")[1].strip()
            
            return {
                'run_id': run_id,
                'ticker': ticker,
                'justification': justification,
                'market_cap_estimate': market_cap,
                'full_response': content,
                'status': 'success'
            }
            
        except Exception as e:
            return {
                'run_id': run_id,
                'ticker': None,
                'justification': f'Error: {str(e)}',
                'market_cap_estimate': None,
                'full_response': str(e),
                'status': 'error'
            }
    
    async def run_analysis(self, num_runs=25, model="anthropic/claude-opus-4", temperature=0.0):
        """Run multiple analyses concurrently."""
        print(f"Starting {num_runs} analyses with model: {model} at temperature: {temperature}")
        
        # Create tasks for all runs
        tasks = []
        for i in range(num_runs):
            task = self.get_single_response(f"run_{i+1}", model, temperature)
            tasks.append(task)
        
        # Run with progress bar
        results = []
        with tqdm(total=num_runs, desc="Running analyses") as pbar:
            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)
                pbar.update(1)
        
        # Create DataFrame
        df = pd.DataFrame(results)
        
        # Calculate statistics
        successful_runs = df[df['status'] == 'success'].shape[0]
        ticker_counts = df[df['ticker'].notna()]['ticker'].value_counts()
        
        print(f"\nCompleted! Successful runs: {successful_runs}/{num_runs}")
        
        return df, ticker_counts


async def main(api_key):
    """Run the analysis with provided API key."""
    analyzer = SimpleTickerAnalyzer(api_key)
    
    # Run analysis
    df, ticker_counts = await analyzer.run_analysis(
        num_runs=25,
        model="anthropic/claude-opus-4",
        temperature=0.0
    )
    
    # Display results
    print("\n" + "="*50)
    print("TICKER VOTE DISTRIBUTION")
    print("="*50)
    
    total_votes = ticker_counts.sum()
    for ticker, count in ticker_counts.items():
        percentage = (count / total_votes) * 100
        bar = "█" * int(percentage / 2)
        print(f"{ticker:4s}: {count:2d} votes ({percentage:4.1f}%) {bar}")
    
    winner = ticker_counts.index[0] if len(ticker_counts) > 0 else "None"
    print(f"\n✓ WINNER: {winner}")
    
    # Save results
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    df.to_csv(f"ticker_analysis_{timestamp}.csv", index=False)
    print(f"\nResults saved to: ticker_analysis_{timestamp}.csv")
    
    # Show sample justifications
    print("\n" + "="*50)
    print("SAMPLE JUSTIFICATIONS")
    print("="*50)
    
    for ticker in ticker_counts.index[:4]:
        ticker_df = df[(df['ticker'] == ticker) & (df['status'] == 'success')]
        if not ticker_df.empty:
            sample = ticker_df.iloc[0]
            print(f"\n{ticker}:")
            print(f"  {sample['justification'][:150]}...")
            print(f"  Market Cap Estimate: {sample['market_cap_estimate']}")
    
    return df


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python ticker_analysis_simple.py YOUR_OPENROUTER_API_KEY")
        print("Get your API key from: https://openrouter.ai/keys")
        sys.exit(1)
    
    api_key = sys.argv[1]
    
    # Run the analysis
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    
    try:
        df = loop.run_until_complete(main(api_key))
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()