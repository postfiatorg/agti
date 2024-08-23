import requests
import pandas as pd
import math

class FMPMarketDataRetriever:
    def __init__(self, pw_map):
        self.api_key = pw_map['financialmodelingprep']
        self.base_url = 'https://financialmodelingprep.com/api/v3/quote/'
        self.pre_post_market_base_url = 'https://financialmodelingprep.com/api/v4/batch-pre-post-market-trade/'
        
    def retrieve_batch_equity_data(self, symbols, batch_size=5):
        """
        Retrieves equity data for a batch of symbols and returns a combined DataFrame.
        
        Parameters:
        symbols (list): List of stock symbols to retrieve data for.
        batch_size (int): Number of symbols to retrieve per request (default is 5).
        
        Returns:
        pd.DataFrame: A DataFrame containing the combined data for all symbols.
        """
        # Initialize an empty DataFrame to store results
        full_df = pd.DataFrame()

        # Calculate the number of batches
        num_batches = math.ceil(len(symbols) / batch_size)

        # Loop through each batch
        for i in range(num_batches):
            # Get the current batch of symbols
            batch_symbols = symbols[i * batch_size:(i + 1) * batch_size]
            symbols_str = ','.join(batch_symbols)

            # Construct the URL for the API request
            url = f'{self.base_url}{symbols_str}?apikey={self.api_key}'

            # Make the request
            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                # Convert the response to a DataFrame and append to the full DataFrame
                batch_df = pd.DataFrame(response.json())
                full_df = pd.concat([full_df, batch_df], ignore_index=True)
            else:
                print(f"Failed to fetch data for batch {i + 1}: {response.status_code}")
        
        return full_df
    
    def retrieve_batch_pre_market_data(self, symbols, batch_size=5):
        """
        Retrieves pre-market and post-market data for a batch of symbols and returns a combined DataFrame.
        
        Parameters:
        symbols (list): List of stock symbols to retrieve data for.
        batch_size (int): Number of symbols to retrieve per request (default is 5).
        
        Returns:
        pd.DataFrame: A DataFrame containing the combined pre-market and post-market data for all symbols.
        """
        # Initialize an empty DataFrame to store results
        full_df = pd.DataFrame()

        # Calculate the number of batches
        num_batches = math.ceil(len(symbols) / batch_size)

        # Loop through each batch
        for i in range(num_batches):
            # Get the current batch of symbols
            batch_symbols = symbols[i * batch_size:(i + 1) * batch_size]
            symbols_str = ','.join(batch_symbols)

            # Construct the URL for the API request
            url = f'{self.pre_post_market_base_url}{symbols_str}?apikey={self.api_key}'

            # Make the request
            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                # Convert the response to a DataFrame and append to the full DataFrame
                batch_df = pd.DataFrame(response.json())
                full_df = pd.concat([full_df, batch_df], ignore_index=True)
            else:
                print(f"Failed to fetch data for batch {i + 1}: {response.status_code}")
        
        return full_df
