#!/usr/bin/env python3
"""
Standalone script to analyze Post Fiat ticker symbol choices using OpenRouter.
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agti.ai.ticker_analysis import TickerAnalyzer
from agti.utilities.settings import PasswordMapLoader
import nest_asyncio

async def run_ticker_analysis():
    """Run the ticker analysis."""
    print("Post Fiat Ticker Symbol Analysis")
    print("=" * 50)
    
    # Initialize with password
    password_map_loader = PasswordMapLoader(password='everythingIsRigged1a')
    analyzer = TickerAnalyzer(pw_map=password_map_loader.pw_map)
    
    # Get parameters
    num_runs = 25
    model = "anthropic/claude-opus-4"
    
    print(f"Running {num_runs} analyses with model: {model}")
    print("This may take a few minutes...\n")
    
    # Run analysis
    df, ticker_counts = await analyzer.run_analysis(
        num_runs=num_runs,
        model=model,
        temperature=0.0  # Zero temperature for deterministic results
    )
    
    # Create and display summary
    summary = analyzer.summarize_results(df)
    
    print("\n" + "="*50)
    print("FINAL RESULTS")
    print("="*50)
    print(f"✓ Winning ticker: {summary['winner']}")
    print(f"\nVote distribution:")
    for ticker, count in sorted(summary['ticker_distribution'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / summary['successful_runs']) * 100
        bar = "█" * int(percentage / 2)  # Visual bar chart
        print(f"  {ticker:4s}: {count:2d} votes ({percentage:4.1f}%) {bar}")
    
    print(f"\nSuccess rate: {summary['successful_runs']}/{num_runs} ({summary['successful_runs']/num_runs*100:.1f}%)")
    
    # Save results
    analyzer.save_results(df, summary, "post_fiat_ticker")
    
    # Display sample justifications
    print("\n" + "="*50)
    print("SAMPLE JUSTIFICATIONS")
    print("="*50)
    for ticker, justification in summary['sample_justifications'].items():
        print(f"\n{ticker}:")
        print(f"  {justification[:200]}..." if len(justification) > 200 else f"  {justification}")
    
    return df, summary


if __name__ == "__main__":
    # Apply nest_asyncio for compatibility
    nest_asyncio.apply()
    
    try:
        # Run the analysis
        loop = asyncio.get_event_loop()
        df, summary = loop.run_until_complete(run_ticker_analysis())
        print("\n✓ Analysis complete!")
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()