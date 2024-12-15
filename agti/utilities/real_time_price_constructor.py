from agti.data.tiingo.equities import TiingoDataTool
from agti.data.fmp.market_data import FMPMarketDataRetriever
import pandas as pd
import datetime
import logging
from urllib.error import HTTPError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimePriceDataframeConstructor:
    """
    A class to handle real-time financial data processing.
    """

    def __init__(self, pw_map):
        """
        Initialize the RealTimeValues class.

        Args:
            pw_map (dict): A dictionary containing password mappings.
        """
        self.pw_map = pw_map
        self.tiingo_equity_tool = TiingoDataTool(pw_map=pw_map)
        self.fmp_market_data_tool = FMPMarketDataRetriever(pw_map=pw_map)
        self._cached_data = {}

    def get_data(self, start_date='2021-01-01', tickers=['MSTR','IEF'], skip_tickers=None):
        """
        Retrieve both price and volume data for the specified tickers in a single pass.

        Args:
            start_date (str): Start date for historical data in YYYY-MM-DD format
            tickers (list): List of stock tickers to fetch data for
            skip_tickers (set): Optional set of tickers to skip due to previous failures

        Returns:
            tuple: (price_df, volume_df) containing price and volume data
        """
        end_date = (datetime.datetime.now() + datetime.timedelta(5)).strftime('%Y-%m-%d')
        cache_key = (start_date, end_date, tuple(sorted(tickers)))
        
        if cache_key in self._cached_data:
            return self._cached_data[cache_key]
        
        if skip_tickers:
            tickers = [t for t in tickers if t not in skip_tickers]
            if not tickers:
                logger.warning("No valid tickers to process after filtering")
                return pd.DataFrame(), pd.DataFrame()

        tiingo_data_list = []
        failed_tickers = set()

        for ticker in tickers:
            try:
                df = self.tiingo_equity_tool.raw_load_tiingo_data(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date
                )
                df['date'] = pd.to_datetime(df['date'].apply(lambda x: str(x)[:10]))
                df['ticker'] = ticker
                tiingo_data_list.append(df)
                logger.info(f"Successfully loaded data for {ticker}")
            except HTTPError as e:
                if e.code == 404:
                    failed_tickers.add(ticker)
                    logger.warning(f"Ticker {ticker} not found in Tiingo database")
                else:
                    logger.error(f"HTTP error loading {ticker}: {str(e)}")
            except Exception as e:
                logger.error(f"Error loading data for {ticker}: {str(e)}")
        
        if not tiingo_data_list:
            logger.warning("No data was successfully loaded for any tickers")
            return pd.DataFrame(), pd.DataFrame()

        tiingo_df = pd.concat(tiingo_data_list)
        
        # Process price data
        tiingo_close_df = tiingo_df[['date', 'ticker', 'adjClose']]
        rt_px_df = tiingo_close_df.groupby(['date','ticker']).last()['adjClose'].unstack()
        
        # Process volume data
        tiingo_volume_df = tiingo_df[['date', 'ticker', 'volume']]
        rt_volume_df = tiingo_volume_df.groupby(['date','ticker']).last()['volume'].unstack()
        
        # Only fetch real-time data for tickers that succeeded with Tiingo
        valid_tickers = [t for t in tickers if t not in failed_tickers]
        if valid_tickers:
            try:
                fmp_real_time_data = self.fmp_market_data_tool.retrieve_batch_equity_data(
                    symbols=valid_tickers, 
                    batch_size=1000
                )
                fmp_real_time_data['date'] = pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d'))
                fmp_real_time_data['ticker'] = fmp_real_time_data['symbol']
                
                # Update price data
                price_appender = fmp_real_time_data[['ticker','date','price']].groupby(['ticker','date']).last()['price'].unstack().transpose()
                real_timevalues = pd.concat([rt_px_df, price_appender]).reset_index().groupby('date').last()
                
                # Update volume data
                volume_appender = fmp_real_time_data[['ticker','date','volume']].groupby(['ticker','date']).last()['volume'].unstack().transpose()
                real_timevolumes = pd.concat([rt_volume_df, volume_appender]).reset_index().groupby('date').last()
            except Exception as e:
                logger.error(f"Error fetching real-time data: {str(e)}")
                real_timevalues = rt_px_df
                real_timevolumes = rt_volume_df
        else:
            real_timevalues = rt_px_df
            real_timevolumes = rt_volume_df

        result = (real_timevalues, real_timevolumes)
        self._cached_data[cache_key] = result
        return result

    def get_combined_close_data(self, start_date='2021-01-01', tickers=['MSTR','IEF'], skip_tickers=None):
        """
        Retrieve close data for the specified tickers.
        
        Args:
            start_date (str): Start date for historical data in YYYY-MM-DD format
            tickers (list): List of stock tickers to fetch data for
            skip_tickers (set): Optional set of tickers to skip due to previous failures

        Returns:
            pandas.DataFrame: Combined real-time close data for the tickers.
        """
        price_data, _ = self.get_data(start_date, tickers, skip_tickers)
        return price_data

    def get_combined_volume_data(self, start_date='2021-01-01', tickers=['MSTR','IEF'], skip_tickers=None):
        """
        Retrieve volume data for the specified tickers.
        
        Args:
            start_date (str): Start date for historical data in YYYY-MM-DD format
            tickers (list): List of stock tickers to fetch data for
            skip_tickers (set): Optional set of tickers to skip due to previous failures

        Returns:
            pandas.DataFrame: Combined real-time volume data for the tickers.
        """
        _, volume_data = self.get_data(start_date, tickers, skip_tickers)
        return volume_data
