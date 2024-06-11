import requests
# Print the JSON response
import pandas as pd

class TiingoNewsAPI:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.api_key = self.pw_map['tiingo']
    def get_ticker_list_news(self,ticker_list):
        
        # Define the headers for the request
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Specify the tickers you want news for
        tickers =','.join(ticker_list)
        api_key = self.api_key
        # Construct the URL with the specified tickers
        url = f"https://api.tiingo.com/tiingo/news?tickers={tickers}&token={api_key}"
        
        # Make the GET request
        request_response = requests.get(url, headers=headers)
        full_df = pd.DataFrame(request_response.json())
        return full_df

