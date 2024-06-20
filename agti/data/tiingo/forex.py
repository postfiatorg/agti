import requests
from agti.utilities.db_manager import DBConnectionManager
import datetime 
import pandas as pd 
class TiingoFXTool:
    def __init__(self, pw_map):
        """ unclear if this is neccessary
        
        args:
        """ 
        self.pw_map = pw_map
        self.tiingo_key = self.pw_map['tiingo']
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)

    def get_historical_30_min_data_for_fx_cross(self, fx_cross, start_date='2020-01-01', end_date='2020-07-30'):
        headers = {'Content-Type': 'application/json'}
        req_string = f"https://api.tiingo.com/tiingo/fx/{fx_cross}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=30min&token={self.tiingo_key}"
        requestResponse = requests.get(req_string, headers=headers)
        xdf = pd.DataFrame(requestResponse.json())
        # Make sure end_date is timezone-naive
        end_date = pd.to_datetime(xdf['date'].max()).tz_localize(None) - datetime.timedelta(1)
    
        next_start_date = (end_date - datetime.timedelta(2)).strftime('%Y-%m-%d')
        next_end_date = (end_date + datetime.timedelta(180)).strftime('%Y-%m-%d')
        return {'data': xdf, 'end_date': end_date, 'next_start_date': next_start_date, 'next_end_date': next_end_date, 'fx_cross': fx_cross}

    def get_full_historical_data_for_fx_cross(self, fx_cross):
        # Get the current date as timezone-naive
        yarr=[]
        current_date = pd.to_datetime('today').tz_localize(None)
        result = self.get_historical_30_min_data_for_fx_cross(fx_cross=fx_cross)
        yarr.append(result['data'])
        while (current_date - result['end_date']).days > 2:
            result = self.get_historical_30_min_data_for_fx_cross(fx_cross=result['fx_cross'],
                                                                  start_date=result['next_start_date'], 
                                                                  end_date=result['next_end_date'])
            yarr.append(result['data'])
        
        full_data_df = pd.concat(yarr)
        full_data_df['date'] =pd.to_datetime(full_data_df['date'])
        # Assuming 'result' is your DataFrame returned from the loop_until_recent function
        # Step 1: Ensure the 'date' column is timezone-aware.
        # Step 2: Convert from UTC to EST directly without re-localizing
        full_data_df['date_est'] = full_data_df['date'].dt.tz_convert('US/Eastern')
        
        # Optional Step 3: Make 'date_est' timezone-naive
        full_data_df['date_est'] = full_data_df['date_est'].dt.tz_localize(None)
        

        return full_data_df

    def generate_full_fx_cross_frame(self):
        g10_fx_crosses = ['eurusd', 'usdjpy', 'gbpusd', 'audusd', 'nzdusd', 'usdcad', 'usdchf', 'usdnok', 'usdsek']
        liquid_non_g10_fx_crosses = ['usdmxn', 'usdsgd', 'usdhkd', 'usdzar','usdpln','usdhuf','usdcnh']
        
        all_crosses = g10_fx_crosses+liquid_non_g10_fx_crosses
        yarr = []
        for xcross in all_crosses:
            try:
                result = self.get_full_historical_data_for_fx_cross(fx_cross=xcross)
                yarr.append(result)
                print(f'FINISHED {xcross}')
            except:
                print(f'FAILED on {xcross}')
                pass
        op= pd.concat(yarr)
        return op

    def write_full_fx_spot_usd_denom_history(self):
        fxcframe = self.generate_full_fx_cross_frame()
        reverse_fxc = fxcframe.copy()
        for xcol in ['open','high','low','close']:
            reverse_fxc[xcol]=1/reverse_fxc[xcol]
        reverse_fxc['ticker']=reverse_fxc['ticker'].apply(lambda x: x[-3:])+reverse_fxc['ticker'].apply(lambda x: x[0:3])
        full_fx_frame = pd.concat([fxcframe, reverse_fxc])
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        full_fx_frame.to_sql('tiingo__fx_spot_usd_denom',dbconnx,if_exists='replace')
        dbconnx.dispose()
