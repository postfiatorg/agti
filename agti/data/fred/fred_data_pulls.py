import pandas as pd
import requests
from typing import Union, List, Dict
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from typing import Union, List, Dict
from datetime import datetime, timedelta
import time

class FREDDataFetcher:
    """
    A class to fetch and process time series data from FRED (Federal Reserve Economic Data).

    # Example usage with your API key
    fred = FREDDataFetcher(pw_map=password_map_loader.pw_map)
    
    try:
        # Test single series fetch
        print("\nFetching single series...")
        df = fred.get_series(
            series_id="MTSDS133FMS",
            start_date="2015-01-01",
            frequency="m"
        )
        print("Single series data:")
        print(df.head())
        
        # Test series info
        print("\nFetching series info...")
        info = fred.get_series_info("MTSDS133FMS")
        print("Series information:")
        print(f"Title: {info.get('title')}")
        print(f"Units: {info.get('units')}")
        print(f"Frequency: {info.get('frequency')}")
        
        # Test multiple series
        print("\nFetching multiple series...")
        multi_df = fred.get_multiple_series(
            series_ids=["MTSDS133FMS", "GDP", "UNRATE"],
            start_date="2015-01-01",
            frequency="q"
        )
        print("Multiple series data:")
        print(multi_df.head())
        
    """
    
    def __init__(self, pw_map):
        """Initialize with FRED API key"""
        self.api_key = pw_map['fred_api_key']
        self.base_url = "https://api.stlouisfed.org/fred"
        
    def get_series(self, 
                   series_id: str, 
                   start_date: Union[str, datetime] = None,
                   end_date: Union[str, datetime] = None,
                   frequency: str = None) -> pd.DataFrame:
        """Fetch a single time series from FRED."""
        # Format dates if provided
        if start_date:
            if isinstance(start_date, datetime):
                start_date = start_date.strftime('%Y-%m-%d')
        if end_date:
            if isinstance(end_date, datetime):
                end_date = end_date.strftime('%Y-%m-%d')
                
        # Build API parameters
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json'
        }
        
        if start_date:
            params['observation_start'] = start_date
        if end_date:
            params['observation_end'] = end_date
        if frequency:
            params['frequency'] = frequency
            
        try:
            # Get series data using correct endpoint
            response = requests.get(
                f"{self.base_url}/series/observations",
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            
            if 'observations' not in data:
                raise ValueError(f"No data found for series {series_id}")
                
            df = pd.DataFrame(data['observations'])
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.set_index('date')[['value']]
            df.columns = [series_id]
            
            return df
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching data from FRED: {str(e)}")
            
    def get_multiple_series(self, 
                           series_ids: List[str],
                           start_date: Union[str, datetime] = None,
                           end_date: Union[str, datetime] = None,
                           frequency: str = None) -> pd.DataFrame:
        """Fetch multiple time series from FRED."""
        all_series = []
        for series_id in series_ids:
            try:
                df = self.get_series(
                    series_id=series_id,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency
                )
                all_series.append(df)
                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                print(f"Warning: Error fetching series {series_id}: {str(e)}")
                continue
                
        if not all_series:
            raise ValueError("No data was successfully retrieved")
            
        return pd.concat(all_series, axis=1)
    
    def get_series_info(self, series_id: str) -> Dict:
        """Get metadata information about a FRED series."""
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json'
        }
        
        try:
            # Use correct endpoint for series info
            response = requests.get(
                f"{self.base_url}/series",
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            if 'seriess' in data:
                return data['seriess'][0]
            else:
                raise ValueError(f"No information found for series {series_id}")
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching series info from FRED: {str(e)}")
