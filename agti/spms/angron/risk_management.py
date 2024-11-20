import pandas as pd
import pandas as pd
import re
from agti.data.bloomberg.standard_pulls import BloombergDailyDataTool
#pd.read_csv('https://etherscan.io/chart/ethersupplygrowth?output=csv')
from narg.spms.angron.sov_v2 import *#StoreOfValueIndicator
from narg.spms.angron.live_fx_signal import FXSignalGeneration
from agti.spms.angron.mosaic import MosaicCreator
from agti.utilities.db_manager import DBConnectionManager
from narg.spms.angron.ai_based_macro_scoring import AngronPDFMacroScoring
from agti.ai.anthropic import AnthropicTool
import random
from typing import List, Dict
import datetime

class AngronVolatilityAndCorrelFrame:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        self.bloomberg_daily_data= BloombergDailyDataTool(pw_map=password_map_loader.pw_map, bloomberg_connection=True)

    def output_beta_and_correl_map(self):
        bloomberg_tickers = [
                   'EURUSD', 'USDJPY', 'GBPUSD', 'USDCHF', 'USDCAD', 'AUDUSD', 'NZDUSD', 'USDNOK', 'USDSEK',
                   'EURGBP', 'EURJPY', 'EURCHF', 'EURCAD', 'EURAUD', 'EURNZD', 'EURNOK', 'EURSEK',
                   'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPCAD', 'GBPNZD', 'GBPNOK', 'GBPSEK',
                   'CHFJPY', 'CHFNOK', 'CHFSEK',
                   'AUDJPY', 'CADJPY', 'NZDJPY', 'NOKJPY', 'SEKJPY',
                   'AUDCAD', 'AUDCHF', 'AUDNZD', 'AUDNOK', 'AUDSEK',
                   'NZDCAD', 'NZDCHF', 'NZDNOK', 'NZDSEK',
                   'CADCHF', 'CADNOK', 'CADSEK','NOKSEK']
        added_ticker = ['SPY US EQUITY','GBTC US EQUITY','ETHE US EQUITY','SLV US EQUITY','GLD US EQUITY']
        vol_tickers = [i+'V1M CURNCY' for i in bloomberg_tickers]
        ydf = pd.DataFrame(vol_tickers)
        ydf.columns=['volatility']
        forward_vol=self.bloomberg_daily_data.BDP(bbgTickers=list(ydf['volatility']), field='px_last', overrides={})
        forward_vol['value']=forward_vol['value'].astype(float)
        implied_vol=self.bloomberg_daily_data.BDP(bbgTickers=added_ticker, field='3MTH_IMPVOL_100.0%MNY_DF', overrides={})
        historical_vol=self.bloomberg_daily_data.BDP(bbgTickers=added_ticker, field='VOLATILITY_90D', overrides={})
        
        implied_vol['hvol']=historical_vol['value']
        #implied_vol['value']=implied_vol['value'].astype(float)
        implied_vol['volatility']=implied_vol[['value','hvol']].astype(float).mean(1)
        forward_vol['ticker_name']=[i[0:6] for i in forward_vol.index]
        tl_histories = [i+'TL CURNCY' for i in bloomberg_tickers]+added_ticker
        yarr=[]
        for xtick in tl_histories:
            ydf = self.bloomberg_daily_data.BDH( bbgTicker=xtick,
                field='px_last',
                startDate='2022-01-01',
                endDate= datetime.datetime.now().strftime("%Y-%m-%d"),
                periodicity='DAILY',
                overrides=None,
            )
            yarr.append(ydf)
        weekly__resampled_history= pd.concat(yarr).reset_index()[['value','date',
                                       'bbgTicker']].groupby(['date','bbgTicker']).last()['value'].unstack().astype(float).resample('W-FRI').last()
        def calculate_beta_for_fx(fx='NZDUSDTL CURNCY'):
            op=reg.calculate_rolling_beta_of_ts(weekly__resampled_history.pct_change(1)[fx],
                                             market_ts=weekly__resampled_history.pct_change(1)['SPY US EQUITY'], win=52)[-1:].mean()
            return op
        beta_constructor = pd.DataFrame(weekly__resampled_history.columns)
        beta_constructor['beta']=beta_constructor['bbgTicker'].apply(lambda x: calculate_beta_for_fx(x))
        fx_only = beta_constructor[beta_constructor['bbgTicker'].apply(lambda x: 'TL' in x)].copy()
        non_Fx = beta_constructor[beta_constructor['bbgTicker'].apply(lambda x: 'TL' not in x)].copy()
        fx_only['fx_cross']=fx_only['bbgTicker'].apply(lambda x: x[0:6]).apply(lambda x: x.lower())
        fx_cross_to_beta = fx_only.groupby('fx_cross').last()['beta']
        non_Fx['fx_cross']=non_Fx['bbgTicker'].map({'ETHE US EQUITY':'Ethereum','GBTC US EQUITY': 'Bitcoin','GLD US EQUITY':'Gold',"SLV US EQUITY":"Silver",'SPY US EQUITY': 'S&P 500'})
        fx_cross_to_beta = pd.concat([fx_only,non_Fx]).groupby('fx_cross').last()['beta']
        fx_forward_vol = forward_vol.groupby('ticker_name').first()[['value']]
        fx_forward_vol.columns=['volatility']
        naming_map ={'spy us equity':'S&P', 'gbtc us equity':'Bitcoin', 'ethe us equity':'Ethereum', 'slv us equity':'Silver',
               'gld us equity':'Gold'}
        implied_vol['name']=implied_vol.index.map(naming_map)
        volatility_dex = pd.concat([fx_forward_vol,
                   implied_vol.groupby('name').first()[['volatility']]]).astype(float)
        volatility_dex.index.name = 'currency'
        vdex = volatility_dex.sort_values('volatility',ascending=False)
        vdex['beta']=fx_cross_to_beta
        beta_and_volatility = vdex.sort_values('beta', ascending=False).copy()
        correl_map = weekly__resampled_history.pct_change(1)[-52:].corr()

        
        beta_and_correl = self.output_beta_and_correl_map()
        beta_correl_json = beta_and_correl['correl_map'].to_json()
        full_string_output = beta_and_correl['beta_and_vol'].to_json()
        full_output = f"""<<S&P Beta and Volatility for each Asset Starts here>>
        {beta_and_volatility}
        <<S&P Beta and Volatility for each Asset Ends Here>>
        
        <<Correlation Matrix Starts Here>>
        {beta_correl_json}
        <<Correlation Matrix Ends Here>>"""
        return {'correl_map': correl_map,'beta_and_vol': beta_and_volatility,'string_description':full_output}