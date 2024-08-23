import pandas as pd
from sec_cik_mapper import StockMapper
from agti.utilities.db_manager import DBConnectionManager
from agti.data.sec_methods.update_cik import RunCIKUpdate
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import logging

class CIKDefaultMap:
    def __init__(self, pw_map, user_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.cik_update_tool = RunCIKUpdate(pw_map=self.pw_map, user_name=self.user_name)
        self.cik_map = self._generate_cik_to_ticker_map()

    def _generate_cik_to_ticker_map(self):
        try:
            # StockMapper data
            stockmapper = StockMapper()
            stock_mapper_data = [(key, value) for key, values in stockmapper.cik_to_tickers.items() for value in values]
            stock_mapper_df = pd.DataFrame(stock_mapper_data, columns=['cik', 'ticker'])
            stock_mapper_df['source'] = 'stockmapper'

            # Sharadar data
            sharadar_df = self._get_sharadar_data()

            # SEC data
            sec_df = self.cik_update_tool.output_cached_cik_df()[['cik', 'ticker']]
            sec_df['source'] = 'sec'

            # Combine all sources
            combined_df = pd.concat([stock_mapper_df, sharadar_df, sec_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['cik', 'ticker'])

            # Prioritize sources: SEC > Sharadar > StockMapper
            priority_order = {'sec': 1, 'sharadar': 2, 'stockmapper': 3}
            combined_df['priority'] = combined_df['source'].map(priority_order)
            
            # Get the highest priority mapping for each CIK
            final_mapping = combined_df.sort_values('priority').groupby('cik').first()['ticker']
            
            return final_mapping

        except Exception as e:
            logging.error(f"Error in _generate_cik_to_ticker_map: {str(e)}")
            return pd.Series()

    def _get_sharadar_data(self):
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
            sharadar_query = text("""
            SELECT DISTINCT ticker, secfilings
            FROM sharadar__tickers
            WHERE ticker != 'N/A' AND isdelisted = 'N' AND category ILIKE '%stock%';
            """)
            
            with dbconnx.connect() as connection:
                result = connection.execute(sharadar_query)
                sharadar_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            sharadar_df['cik'] = sharadar_df['secfilings'].str.extract(r'&CIK=(\d+)')
            sharadar_df = sharadar_df[['cik', 'ticker']]
            sharadar_df['source'] = 'sharadar'
            return sharadar_df
        except SQLAlchemyError as e:
            logging.error(f"Database error in _get_sharadar_data: {str(e)}")
            return pd.DataFrame(columns=['cik', 'ticker', 'source'])
        except Exception as e:
            logging.error(f"Error in _get_sharadar_data: {str(e)}")
            return pd.DataFrame(columns=['cik', 'ticker', 'source'])

    def get_ticker_for_cik(self, cik):
        return self.cik_map.get(cik)

    def get_cik_for_ticker(self, ticker):
        return self.cik_map[self.cik_map == ticker].index[0] if ticker in self.cik_map.values else None