#Here's a clean class implementation based on the provided code:
from agti.data.tiingo.equities import TiingoDataTool
from agti.data.fmp.market_data import FMPMarketDataRetriever
import pandas as pd
import datetime

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

    def get_combined_close_data(self,start_date = '2010-01-01', tickers=['MSTR','IEF']):
        """
        Retrieve and combine close data for the specified tickers.

        Returns:
            pandas.DataFrame: Combined real-time close data for the tickers.
        """
        end_date = (datetime.datetime.now() + datetime.timedelta(5)).strftime('%Y-%m-%d')
        

        tiingo_data_list = []
        for ticker in tickers:
            df = self.tiingo_equity_tool.raw_load_tiingo_data(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date
            )
            df['date'] = df['date'].apply(lambda x: str(x)[:10])
            df['date'] = pd.to_datetime(df['date'])
            df['ticker'] = ticker
            tiingo_data_list.append(df)
        
        tiingo_df = pd.concat(tiingo_data_list)
        tiingo_close_df = tiingo_df[['date', 'ticker', 'adjClose']]
        rt_px_df = tiingo_close_df.groupby(['date','ticker']).last()['adjClose'].unstack()
        
        fmp_real_time_data = self.fmp_market_data_tool.retrieve_batch_equity_data(symbols=tickers, batch_size=1000)
        fmp_real_time_data['date'] = pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d'))
        fmp_real_time_data['ticker'] = fmp_real_time_data['symbol']
        appender = fmp_real_time_data[['ticker','date','price']].groupby(['ticker','date']).last()['price'].unstack().transpose()
        
        real_timevalues = pd.concat([rt_px_df, appender]).reset_index().groupby('date').last()
        return real_timevalues


