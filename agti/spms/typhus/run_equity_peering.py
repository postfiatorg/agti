import datetime
from agti.live_trading.universe_generation.fast_equity_pull import FastEquityPull
from agti.utilities.db_manager import DBConnectionManager
import datetime 
import pandas as pd 
import numpy as np 
class DefaultPeerTool:
    def __init__(self,pw_map):
        self.user_name= 'spm_typhus'
        self.pw_map = pw_map
        self.fast_equity_pull = FastEquityPull(pw_map=pw_map)
        self.standard_equity_df = self.fast_equity_pull.load_standard_equity_df()
        self.standard_equity_df['tick_copy']=self.standard_equity_df.index.get_level_values(0)
        self.standard_equity_df['tick_is1']=np.where(self.standard_equity_df['tick_copy']==self.standard_equity_df['tick_copy'].shift(1),1,np.nan)
        self.standard_equity_df['tRet']=(self.standard_equity_df['closeadj'].pct_change(1))*self.standard_equity_df['tick_is1']
        self.volume_multiplier_map = self.generate_volume_multiplier_map()
        self.adjusted_correl_frame = self.generate_adjusted_correlation_frame()
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        self.sharadar_ticker_table = pd.read_sql('sharadar__tickers',dbconnx).groupby('ticker').first()
        self.sharadar_ticker_table['security_type']=np.where(self.sharadar_ticker_table['category'].apply(lambda x: ('ETF' in x)|('ETN' in x)),'ETF','Stock')
        self.sharadar_ticker_table['default_financial']=self.sharadar_ticker_table['sector'].map( {'Basic Materials':'fcf',
                'Communication Services':'fcf',
                'Consumer Cyclical':'fcf',
                'Consumer Defensive':'fcf',
                'Energy':'fcf',
                'Financial Services':'netinc',
                'Healthcare':'fcf',
                'Industrials':'fcf',
                'Real Estate':'netinc',
                'Technology':'fcf',
                'Utilities':'netinc'})
    def generate_adjusted_correlation_frame(self):
        today = datetime.datetime.now()
        dateset1 = (today - datetime.timedelta(days=90), today)
        dateset2 = (today - datetime.timedelta(days=180), today - datetime.timedelta(days=90))
        dateset3 = (today - datetime.timedelta(days=270), today - datetime.timedelta(days=180))
        dateset4 = (today - datetime.timedelta(days=365), today - datetime.timedelta(days=270))
        dateset5 = (today - datetime.timedelta(days=455), today - datetime.timedelta(days=365))
        dateset6 = (today - datetime.timedelta(days=545), today - datetime.timedelta(days=455))
        dateset7 = (today - datetime.timedelta(days=910), today - datetime.timedelta(days=545))
        datex = dateset1
        corr_block__init= self.standard_equity_df[(self.standard_equity_df.index.get_level_values(1)>datex[0]) &
        (self.standard_equity_df.index.get_level_values(1)<=datex[1])]['tRet'].unstack(0).sort_index().corr().fillna(0)
        min_correl_fr = []
        all_correl_blocks = []
        all_correl_blocks.append(corr_block__init)
        for datex in [dateset2,dateset3,dateset4,dateset5, dateset6,dateset7]:
            corr_block__add =self.standard_equity_df[(self.standard_equity_df.index.get_level_values(1)>datex[0]) &
            (self.standard_equity_df.index.get_level_values(1)<=datex[1])]['tRet'].unstack(0).sort_index().corr().fillna(0)
            all_correl_blocks.append(corr_block__add)
            corr_block__init=corr_block__init+corr_block__add
        overall_corr_frame = corr_block__init/7
        min_correl_frame = all_correl_blocks[0].copy()
        
        ## Loop through the correlation blocks for dateset 1-4
        for i in range(1, 4):
            min_correl_frame = pd.concat([min_correl_frame, all_correl_blocks[i]]).groupby('ticker').min()
        adjusted_correl_frame = (min_correl_frame+overall_corr_frame)/2
        return adjusted_correl_frame
    def generate_volume_multiplier_map(self):
        dollar_volumes= self.standard_equity_df[self.standard_equity_df.index.get_level_values(1)> 
        datetime.datetime.now()-datetime.timedelta(90)][['dv']].groupby('ticker').sum()
        dollar_volumes['volume_multiplier']=dollar_volumes['dv'].rank()/dollar_volumes['dv'].rank().max()
        return dollar_volumes.sort_values('volume_multiplier', ascending=False)['volume_multiplier']
    ## Print the date sets
    #dateset1, dateset2, dateset3, dateset4, dateset5, dateset6
    def generate_ticker_peering_frame(self,ticker_to_work='CALF'):
        
        ticker_x= self.adjusted_correl_frame[[ticker_to_work]].copy()
        ticker_x['volume_map']=self.volume_multiplier_map
        ticker_x['agnostic_score']=ticker_x[ticker_to_work]*ticker_x['volume_map']
        all_future_hedges = ['SPY','IWM','QQQ','GLD','USO','TLT','UVXY','EFA','EEM','FXI','GBTC']
        all_future_hedges=[i for i in all_future_hedges if i!=ticker_to_work]
        primary_future= list(ticker_x.loc[all_future_hedges].sort_values(ticker_to_work, ascending=False).head(1).index)[0]
        non_futures_hedge = ticker_x[~ticker_x.index.get_level_values(0).isin(all_future_hedges)].copy()
        
        ticker_security_type =self.sharadar_ticker_table['security_type'][ticker_to_work]
        non_futures_hedge['ticker_security_type']=self.sharadar_ticker_table['security_type']
        non_futures_hedge['default_financial']=self.sharadar_ticker_table['default_financial']
        ticker_default_financial = self.sharadar_ticker_table['default_financial'][ticker_to_work]
        non_futures_hedge['security_type_boost']=np.where(non_futures_hedge['ticker_security_type']==ticker_security_type,.07,0)
        non_futures_hedge['financial_type_boost']=np.where(non_futures_hedge['default_financial']==ticker_default_financial,.07,0)
        non_futures_hedge['boosted_score']=non_futures_hedge[['agnostic_score','security_type_boost','financial_type_boost']].sum(1)
        non_futures_hedge=non_futures_hedge[non_futures_hedge.index.get_level_values(0)!=ticker_to_work].copy()
        
        peer_frame = non_futures_hedge.sort_values('boosted_score',ascending=False).head(15).copy()
        peer_frame['peer_weight']=peer_frame['boosted_score']/peer_frame['boosted_score'].sum()
        non_future_peers = peer_frame[['peer_weight']].copy()
        non_future_peers['peer_type']='standard'
        ind_hedge_append = pd.DataFrame({'ticker':primary_future,'peer_weight':1,'peer_type':'index_hedge'},index=[0]).set_index('ticker')
        peering_frame = pd.concat([non_future_peers,ind_hedge_append]).reset_index()
        peering_frame['peer_date']=pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d'))
        peering_frame['ticker_to_peer']=ticker_to_work
        return peering_frame

    def write_full_agti_equity_peering(self):
        all_tickers_to_work = list(self.standard_equity_df.index.get_level_values(0).unique())
        full_peer_arr=[]
        for x in all_tickers_to_work:
            try:
                full_peer_arr.append(self.generate_ticker_peering_frame(ticker_to_work=x))
            except:
                print(x)
                pass
        big_peer_arr = pd.concat(full_peer_arr)
        full_peering_df = big_peer_arr
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        full_peering_df.to_sql('spm_typhus__us_equity_peers', dbconnx, if_exists='append')
        return full_peering_df