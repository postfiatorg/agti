import requests
import pandas as pd
from io import StringIO
import numpy as np
import datetime

class BitcoinNetworkData:
    def __init__(self):
        pass

    def get_blockchain_field_data(self, blockchain_field='hash-rate'):
        """
        Fetches historical data for a specified blockchain field from Blockchain.info API.

        Parameters:
        - blockchain_field (str): The blockchain field to fetch data for (e.g., 'hash-rate').

        Returns:
        - pd.DataFrame: A DataFrame containing the historical data for the specified field.
        """
        # Get all-time data
        url_all_time = f'https://api.blockchain.info/charts/{blockchain_field}?cors=true&timespan=all&format=json&lang=en'
        all_data = requests.get(url_all_time).json()
        all_df = pd.DataFrame(all_data['values'])
        all_df.columns = ['date', 'value']
        all_df['date'] = pd.to_datetime(all_df['date'], unit='s')

        # Get last year's data
        url_one_year = f'https://api.blockchain.info/charts/{blockchain_field}?cors=true&timespan=1year&format=json&lang=en'
        one_year_data = requests.get(url_one_year).json()
        one_year_df = pd.DataFrame(one_year_data['values'])
        one_year_df.columns = ['date', 'value']
        one_year_df['date'] = pd.to_datetime(one_year_df['date'], unit='s')

        # Combine and resample
        full_df = pd.concat([all_df, one_year_df]).drop_duplicates('date').set_index('date').sort_index()
        full_df = full_df.resample('D').last().fillna(method='pad')
        full_df['field_name'] = blockchain_field
        return full_df.reset_index()

    def get_all_blockchain_info_indicators(self):
        """
        Fetches multiple blockchain indicators from Blockchain.info API.

        Returns:
        - pd.DataFrame: A combined DataFrame containing multiple blockchain indicators.
        """
        indicators = []
        fields = ['hash-rate', 'n-transactions', 'estimated-transaction-volume-usd', 'n-unique-addresses', 'median-confirmation-time']
        for field in fields:
            df = self.get_blockchain_field_data(blockchain_field=field)
            indicators.append(df)
        full_indicators = pd.concat(indicators).reset_index(drop=True)
        return full_indicators

    def load_bitcoin_visual_indicators(self):
        """
        Loads Bitcoin network indicators from BitcoinVisuals.

        Returns:
        - pd.DataFrame: A DataFrame containing Bitcoin network indicators.
        """
        url = 'https://bitcoinvisuals.com/static/data/data_daily.csv'
        response = requests.get(url)
        df = pd.read_csv(StringIO(response.text))
        df['date'] = pd.to_datetime(df['day'])
        final_df = pd.DataFrame(df.set_index('date').stack()).reset_index()
        final_df.columns=['date','field_name','value']
        return final_df

    def get_core_bitcoin_network_data(self):
        bitcoin_visuals = self.load_bitcoin_visual_indicators()
        blockchain_info = self.get_all_blockchain_info_indicators()
        all_btc_network_data = pd.concat([bitcoin_visuals, blockchain_info])
        return all_btc_network_data