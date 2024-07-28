## Cross appends interactive brokers data Bloomberg data etc
import pandas as pd
import itertools
from agti.utilities.db_manager import DBConnectionManager
class FXCrossGeneration:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
    def generate_historical_forex_cross_frame(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        dbx = pd.read_sql('ibkr_fx_cache', dbconnx)
        raw_closes = dbx[dbx['date'].apply(lambda x: '16:00' in str(x))].copy()
        raw_opens = dbx[dbx['date'].apply(lambda x: '09:30' in str(x))].copy()
        raw_opens['simple_date']=raw_opens['date'].apply(lambda x: str(x).split(' ')[0])
        raw_closes['simple_date']=raw_closes['date'].apply(lambda x: str(x).split(' ')[0])
        raw_opens['simple_date']=pd.to_datetime(raw_opens['simple_date'])
        raw_closes['simple_date']=pd.to_datetime(raw_closes['simple_date'])
        xcloser= raw_closes[['simple_date','pair','open']].groupby(['pair','simple_date']).last()
        xopens= raw_opens[['simple_date','pair','open']].groupby(['pair','simple_date']).last()
        full_ibkr_hist = pd.concat([xcloser, xopens],axis=1)
        full_ibkr_hist.columns=['close','open']
        ibkr_single_denomination=full_ibkr_hist.sort_index()
        reverse_denom= 1/ibkr_single_denomination
        reverse_denom=reverse_denom.reset_index()
        reverse_denom['pair']=reverse_denom['pair'].apply(lambda x: x[3:6]+x[0:3])
        final_rdenom= reverse_denom.groupby(['pair','simple_date']).last().sort_index()
        ibkr_multidex = pd.concat([ibkr_single_denomination, final_rdenom])
        ibkr_redex = ibkr_multidex.reset_index()
        ibkr_redex.columns=['ticker','simple_date','close','open']
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        tiingo_spot = pd.read_sql('tiingo__fx_spot_usd_denom',dbconnx)
        unique_dates = pd.DataFrame(list(tiingo_spot['date_est'].unique()))
        unique_dates.columns=['date_full']
        unique_dates['date_conversion']= unique_dates['date_full'].apply(lambda x: x.strftime("%Y-%m-%d"))
        unique_dates['date_conversion']=pd.to_datetime(unique_dates['date_conversion'])
        unique_dates['hour']=unique_dates['date_full'].apply(lambda x: str(x).split(' ')[-1:][0])
        date_full_to_date = unique_dates.groupby('date_full').first()['date_conversion']
        tiingo_spot['simple_date']=tiingo_spot['date_est'].map(date_full_to_date)
        date_full_to_hour= unique_dates.groupby('date_full').first()['hour']
        tiingo_spot['hour']=tiingo_spot['date_est'].map(date_full_to_hour)
        close_df  = tiingo_spot[tiingo_spot['hour']=='16:00:00'].groupby(['ticker','simple_date']).last()[['open']]
        open_df = tiingo_spot[tiingo_spot['hour']=='09:30:00'].groupby(['ticker','simple_date']).last()[['open']].copy()
        concatted_df = pd.concat([close_df,open_df],axis=1)
        concatted_df.columns=['close','open']
        concatted_df= concatted_df.dropna()
        concatted_df = concatted_df
        full_fx_df = pd.concat([concatted_df.reset_index(),ibkr_redex]).groupby(['ticker','simple_date']).last().sort_index()
        full_open_history= pd.read_sql('spm_angron__bloomberg_halfhour_cache__open', dbconnx)
        raw_opens = full_open_history[full_open_history['unique_identifier'].apply(lambda x: 
                                                                       '09:30:00' in x)].copy()
        raw_opens['date']=pd.to_datetime(raw_opens['unique_identifier'].apply(lambda x: x.split(' ')[0]))
        raw_closes = full_open_history[full_open_history['unique_identifier'].apply(lambda x: 
                                                                       '16:00:00' in x)].copy()
        raw_closes['date']=pd.to_datetime(raw_closes['unique_identifier'].apply(lambda x: x.split(' ')[0]))
        raw_opens['simple_ticker']=raw_opens['ticker'].apply(lambda x: x.split(' ')[0])
        raw_closes['simple_ticker']=raw_closes['ticker'].apply(lambda x: x.split(' ')[0])
        full_df = pd.concat([raw_opens.groupby(['simple_ticker','date']).last()['value'], raw_closes.groupby(['simple_ticker','date']).last()['value']],axis=1)
        full_df.columns=['open','close']
        inverse_df = (1/full_df).copy()
        inv_redex = inverse_df.reset_index().copy()
        inv_redex['simple_ticker']=inv_redex['simple_ticker'].apply(lambda x: x[-3:]+x[0:3])
        full_x_df = inv_redex.groupby(['simple_ticker','date']).first().sort_index()
        combined_bloomberg_df = pd.concat([full_x_df, full_df]).dropna()
        xmulti_dex = pd.concat([concatted_df,combined_bloomberg_df])
        xmulti_dex.index.names = ['ticker','date']
        multi_dex_df = xmulti_dex.dropna().reset_index().groupby(['ticker','date']).last()
        all_ticks = [i for i in list(set([i[0:3] for i in multi_dex_df.index.get_level_values(0).unique()])) if i!='usd']
        all_crosses = [''.join(x) for x in list(itertools.permutations(all_ticks,2))]
        varr=[]
        for tick_to_create in all_crosses:
            
            tick1= tick_to_create[0:3]
            tick2= tick_to_create[-3:]
            full_multi_dex = multi_dex_df.loc[f'{tick1}usd']*multi_dex_df.loc[f'usd{tick2}']
            full_multi_dex['ticker']=tick_to_create
            varr.append(full_multi_dex)
        full_combined_cross_df = pd.concat([pd.concat(varr).reset_index(),multi_dex_df.reset_index()]).copy()
        combined_history = full_combined_cross_df.groupby(['ticker','date']).last().sort_index()
        return combined_history