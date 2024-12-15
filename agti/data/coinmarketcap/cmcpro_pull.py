from requests import Session
import pandas as pd
import time
from datetime import datetime
from agti.data.coinmarketcap.scrape_and_cache import CoinMarketCapDataTool
from sqlalchemy import text

class CoinMarketCapAPI:
    """
    EXAMPLE USAGE 
    content_df = api.get_content(symbol='XRP', limit=10, news_type='news')
    price_df = api.get_dataframe('BTC', interval='1d', count=365*5)
    """ 
    def __init__(self, pw_map):
        self.session = Session()
        self.session.headers.update({
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': pw_map['coinmarketcap_api']
        })
        self.data_tool = CoinMarketCapDataTool(pw_map)
        self.request_count = 0
        self.last_request_time = datetime.now()
        self.RATE_LIMIT = 60  # requests per minute

    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = datetime.now()
        elapsed = (current_time - self.last_request_time).total_seconds()
        
        # Reset counter if a minute has passed
        if elapsed >= 60:
            self.request_count = 0
            self.last_request_time = current_time
        
        # If we're at the rate limit, wait until the minute is up
        if self.request_count >= self.RATE_LIMIT:
            sleep_time = 60 - elapsed
            if sleep_time > 0:
                print(f"Rate limit reached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_request_time = datetime.now()

    def get_dataframe(self, symbol, interval='1d', count=30, convert='USD', max_retries=3):
        """
        Fetch historical data and return as pandas DataFrame with rate limit handling
        """
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/historical'
        params = {
            'symbol': symbol,
            'interval': interval,
            'count': min(count, 2000),  # Maximum allowed per request
            'convert': convert,
            'aux': 'price,volume,market_cap,circulating_supply,quote_timestamp'
        }

        for attempt in range(max_retries):
            try:
                self._wait_for_rate_limit()
                response = self.session.get(url, params=params)
                self.request_count += 1
                
                response.raise_for_status()
                data = response.json()
                quotes = data['data']['quotes']
                
                records = []
                for quote in quotes:
                    quote_data = quote['quote'][convert]
                    records.append({
                        'timestamp': quote['timestamp'],
                        'price': quote_data['price'],
                        'volume': quote_data['volume_24h'],
                        'market_cap': quote_data['market_cap'],
                        'supply': quote_data['circulating_supply']
                    })
                
                df = pd.DataFrame(records)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                return df
                
            except Exception as e:
                if response.status_code == 429:  # Rate limit exceeded
                    if attempt < max_retries - 1:
                        print("Rate limit hit, waiting for reset...")
                        time.sleep(61)  # Wait for rate limit reset
                        continue
                print(f"Error fetching data: {str(e)}")
                if 'response' in locals():
                    print(f"Response status code: {response.status_code}")
                    print(f"Response text: {response.text}")
                return pd.DataFrame()

    def get_content(self, symbol=None, slug=None, content_id=None, start=1, limit=100, 
                    news_type='all', content_type='all', category=None, language='en'):
        """
        Fetch content from CMC News/Headlines and Alexandria articles
        """
        url = 'https://pro-api.coinmarketcap.com/v1/content/latest'
        params = {
            'start': max(1, start),
            'limit': min(200, limit),
            'news_type': news_type,
            'content_type': content_type,
            'language': language
        }

        if symbol:
            params['symbol'] = symbol
        if slug:
            params['slug'] = slug
        if content_id:
            params['id'] = content_id
        if category:
            params['category'] = category

        try:
            self._wait_for_rate_limit()
            response = self.session.get(url, params=params)
            self.request_count += 1
            
            response.raise_for_status()
            data = response.json()
            
            if not data.get('data'):
                return pd.DataFrame()
            
            df = pd.DataFrame(data['data'])
            
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'released_at' in df.columns:
                df['released_at'] = pd.to_datetime(df['released_at'])
            
            if 'assets' in df.columns:
                df['asset_ids'] = df['assets'].apply(lambda x: ','.join([str(asset['id']) for asset in x]) if isinstance(x, list) else None)
                df['asset_names'] = df['assets'].apply(lambda x: ','.join([asset['name'] for asset in x]) if isinstance(x, list) else None)
                df['asset_symbols'] = df['assets'].apply(lambda x: ','.join([asset['symbol'] for asset in x]) if isinstance(x, list) else None)
                df = df.drop('assets', axis=1)
            
            return df
            
        except Exception as e:
            print(f"Error fetching content: {str(e)}")
            if 'response' in locals():
                print(f"Response status code: {response.status_code}")
                print(f"Response text: {response.text}")
            return pd.DataFrame()

    def write_full_history(self, months=60):
        """
        Write full price history for all currencies in cmc_details_df
        
        Parameters:
        - months (int): Number of months of history to fetch (max 60)
        """
        if months > 60:
            print("Warning: Maximum historical data access is 60 months. Setting to 60 months.")
            months = 60
        
        days_per_request = 2000  # Maximum days per request
        total_days = months * 30
        requests_needed = (total_days + days_per_request - 1) // days_per_request
        
        # Get all tickers from cmc_details_df
        tickers = self.data_tool.cmc_details_df['coin_ticker'].tolist()
        
        # Initialize database connection
        dbconnx = self.data_tool.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        
        print(f"Starting historical data collection for {len(tickers)} tickers")
        print(f"Total days: {total_days}, Requests per ticker: {requests_needed}")
        
        for ticker in tickers:
            try:
                full_history = []
                
                for i in range(requests_needed):
                    start_day = i * days_per_request
                    days = min(days_per_request, total_days - start_day)
                    
                    history = self.get_dataframe(
                        symbol=ticker,
                        interval='1d',
                        count=days
                    )
                    
                    if not history.empty:
                        history['ticker'] = ticker
                        full_history.append(history)
                        
                if full_history:
                    ticker_df = pd.concat(full_history)
                    ticker_df.to_sql(
                        'coinmarketcap__price_history_temp',
                        dbconnx,
                        if_exists='append'
                    )
                    print(f"Saved {len(ticker_df)} days of history for {ticker}")
                
            except Exception as e:
                print(f"Error processing {ticker}: {str(e)}")
                continue
        
        try:
            # Combine all saved data into final table
            full_df = pd.read_sql('coinmarketcap__price_history_temp', dbconnx)
            full_df.to_sql('coinmarketcap__price_history', dbconnx, if_exists='replace')
            
            # Drop temporary table using SQLAlchemy text()
            with dbconnx.connect() as connection:
                connection.execute(text('DROP TABLE IF EXISTS coinmarketcap__price_history_temp'))
                connection.commit()
            
            print(f"Successfully wrote full price history to database")
        except Exception as e:
            print(f"Error writing final data: {str(e)}")

# Usage example:
# api = CoinMarketCapAPI(pw_map)
# api.write_full_history(months=60)  # Get maximum allowed history
