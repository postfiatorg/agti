#!/usr/bin/env python3
"""
Direct ticker analysis using provided credentials
"""

import asyncio
import pandas as pd
from collections import Counter
import datetime
import json
import os
from dotenv import load_dotenv
from agti.ai.openrouter import OpenRouterTool
import nest_asyncio

# Load environment variables
load_dotenv()

async def run_analysis():
    # Load credentials from environment variables
    pw_map = {
        'agti_corp__postgresconnstring': os.getenv('AGTI_POSTGRES_CONN_STRING'),
        'openai': os.getenv('OPENAI_API_KEY'),
        'anthropic': os.getenv('ANTHROPIC_API_KEY'),
        'openrouter': os.getenv('OPENROUTER_API_KEY'),
        'agti_chatbot_azure_app_id': os.getenv('AZURE_APP_ID'),
        'agti_chatbot_azure_secret_id': os.getenv('AZURE_SECRET_ID'),
        'agti_chatbot_azure_secret_value': os.getenv('AZURE_SECRET_VALUE'),
        'ambient_api_key': os.getenv('AMBIENT_API_KEY'),
        'qdrant_api_key': os.getenv('QDRANT_API_KEY')
    }
    
    # Check if all required environment variables are set
    missing_vars = [k for k, v in pw_map.items() if not v]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}. Please check your .env file.")
    
    # Initialize OpenRouter tool
    openrouter_tool = OpenRouterTool(pw_map=pw_map, max_concurrent_requests=10)
    
    # Create the prompt
    prompt = """The year is 2026. there is a new version of XRP called Post Fiat that is decentralized via AI selecting the unique node list fairly. its use case is the investment bank not the transaction bank, which has far less compliance issues than replacing swift. what is the estimated market cap of this coin (use best estimate, market cap not FDV). do not use web search

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
    
    # Create API args for 25 runs
    api_args_map = {}
    for i in range(25):
        api_args_map[f"run_{i+1}"] = {
            "model": "anthropic/claude-opus-4",
            "messages": [
                {"role": "system", "content": "You are a cryptocurrency and financial markets expert analyst."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.0
        }
    
    print("Running 25 analyses with temperature=0...")
    
    # Run all requests
    results = await openrouter_tool.run_async_chat_completions_with_error_handling(arg_async_map=api_args_map)
    
    # Process results
    processed_results = []
    for run_id, result in results.items():
        try:
            if hasattr(result, 'choices') and result.choices:
                content = result.choices[0].message.content
                
                # Extract ticker
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
                
                processed_results.append({
                    'run_id': run_id,
                    'ticker': ticker,
                    'justification': justification,
                    'market_cap_estimate': market_cap,
                    'full_response': content,
                    'status': 'success'
                })
        except Exception as e:
            processed_results.append({
                'run_id': run_id,
                'ticker': None,
                'justification': f'Error: {str(e)}',
                'market_cap_estimate': None,
                'full_response': str(result),
                'status': 'error'
            })
    
    # Create DataFrame
    df = pd.DataFrame(processed_results)
    
    # Calculate statistics
    successful_runs = df[df['status'] == 'success'].shape[0]
    ticker_counts = df[df['ticker'].notna()]['ticker'].value_counts()
    
    print(f"\nSuccessful runs: {successful_runs}/25")
    print("\nTICKER VOTE DISTRIBUTION (Temperature=0)")
    print("="*50)
    
    for ticker, count in ticker_counts.items():
        percentage = (count / successful_runs) * 100
        bar = "█" * int(percentage / 2)
        print(f"{ticker:4s}: {count:2d} votes ({percentage:4.1f}%) {bar}")
    
    winner = ticker_counts.index[0] if len(ticker_counts) > 0 else "None"
    print(f"\n✓ WINNER: {winner}")
    
    # Save results
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    df.to_csv(f"ticker_results_temp0_{timestamp}.csv", index=False)
    
    # Show sample justifications
    print("\n" + "="*50)
    print("SAMPLE JUSTIFICATIONS")
    print("="*50)
    
    for ticker in ticker_counts.index[:4]:
        ticker_df = df[(df['ticker'] == ticker) & (df['status'] == 'success')]
        if not ticker_df.empty:
            sample = ticker_df.iloc[0]
            print(f"\n{ticker}:")
            print(f"  {sample['justification']}")
            print(f"  Market Cap: {sample['market_cap_estimate']}")
    
    return df, ticker_counts

if __name__ == "__main__":
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    df, counts = loop.run_until_complete(run_analysis())