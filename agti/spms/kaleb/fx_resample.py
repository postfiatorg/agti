from agti.data.bloomberg.standard_pulls import BloombergDailyDataTool
import itertools
from agti.data.tiingo.equities import TiingoDataTool
class FXFrameLoader:
    def __init__(self,pw_map):
        self.db_connection_manager = DBConnectionManager(pw_map=pw_map)
        self.fx_droid_cache = FXDroidCache(pw_map=pw_map)
        self.tiingo_fx_tool = TiingoFXTool(pw_map=pw_map)
        self.bloomberg_daily_tool = BloombergDailyDataTool(pw_map=pw_map, bloomberg_connection=True)
        self.tiingo_data_tool= TiingoDataTool(pw_map=password_loader.pw_map)
        #dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
    def output_full_historical_half_hourly_forex_history(self, update=False):
        if update==True:
            self.fx_droid_cache.update_all_forex_half_hourly_history()
        ## need to add spm_kaleb
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        fx_spot_tiingo = pd.read_sql('tiingo__fx_spot_usd_denom', dbconnx)
        bloomberg_fx__close_raw = pd.read_sql('spm_angron__bloomberg_halfhour_cache__close',dbconnx)
        bloomberg_fx__open_raw = pd.read_sql('spm_angron__bloomberg_halfhour_cache__open',dbconnx)
        bloomberg_fx__close_raw['date_est']=bloomberg_fx__close_raw['date'].apply(lambda x: str(x).split('+')[0])
        bloomberg_fx__close_raw['date_est'] = bloomberg_fx__close_raw['date_est'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
        bloomberg_fx__open_raw['date_est']=bloomberg_fx__open_raw['date'].apply(lambda x: str(x).split('+')[0])
        bloomberg_fx__open_raw['date_est'] = bloomberg_fx__open_raw['date_est'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
        bloomberg_fx__open_raw['open']=bloomberg_fx__open_raw['value'].astype(float)
        bloomberg_fx__close_raw['close']=bloomberg_fx__close_raw['value'].astype(float)
        bloomberg_fx__open_raw['ticker']=bloomberg_fx__open_raw['ticker'].apply(lambda x: x.split(' curncy')[0])
        bloomberg_fx__close_raw['ticker']=bloomberg_fx__close_raw['ticker'].apply(lambda x: x.split(' curncy')[0])
        fx_spot_tiingo['data_source']='tiingo'
        bloomberg_frame = pd.concat([bloomberg_fx__open_raw[['date_est','open',
                                'ticker']].groupby(['ticker',
                                                    'date_est']).first(),
                   bloomberg_fx__close_raw[['date_est','close',
                                'ticker']].groupby(['ticker',
                                                    'date_est']).first()],axis=1).reset_index()
        bloomberg_frame['data_source']='bloomberg'
        tiingo_frame = fx_spot_tiingo[['ticker','open','close','date_est','data_source']]
        full_forex_history_df = pd.concat([bloomberg_frame,tiingo_frame])
        dollar_constructor = full_forex_history_df.groupby(['ticker','date_est']).last().copy()[['open','close']]
        all_crosses = dollar_constructor.index.get_level_values(0).unique()
        all_liquid_crosses = list(set([i[0:3] for i in all_crosses]+[i[-3:] for i in all_crosses]))
        
        all_pairs = [''.join(i) for i in list(itertools.permutations(all_liquid_crosses,2))]
        non_usd_crosses = [i for i in all_pairs if 'usd' not in i]
        full_cross_constructor_arr=[]
        for ticker in non_usd_crosses:
            ticker_num= ticker[0:3]
            ticker_denom=ticker[-3:]
            cross_constructor = (dollar_constructor.loc[f'{ticker_num}usd']*dollar_constructor.loc[f'usd{ticker_denom}']).copy()
            cross_constructor['ticker']=ticker
            full_cross_constructor_arr.append(cross_constructor)
        full_constructor = pd.concat(full_cross_constructor_arr).dropna()
        full_constructor= full_constructor.reset_index()
        full_forex_history_df=pd.concat([full_constructor,dollar_constructor.reset_index()])
        date_to_type_map = pd.DataFrame(list(full_forex_history_df['date_est'].unique()))
        date_to_type_map['hour_map']=date_to_type_map[0].apply(lambda x: str(x).split(' ')[-1:][0])
        date_to_type_map['date_map']=date_to_type_map[0].apply(lambda x: str(x).split(' ')[0])
        full_forex_history_df['hour']=full_forex_history_df['date_est'].map(date_to_type_map.groupby(0).first()['hour_map'])
        full_forex_history_df['simple_date']=full_forex_history_df['date_est'].map(date_to_type_map.groupby(0).first()['date_map'])
        return full_forex_history_df

    def get_live_fx_price_for_all_crosses(self):
        full_list_of_tickers = """mxnhuf|mxnnok|mxnchf|mxnzar|mxncnh|mxnaud|mxnjpy|mxnsek|mxneur|mxnhkd|mxnsgd|mxngbp|mxnpln|mxncad|mxnnzd|hufmxn|hufnok|hufchf|hufzar|hufcnh|hufaud|hufjpy|hufsek|hufeur|hufhkd|hufsgd|hufgbp|hufpln|hufcad|hufnzd|nokmxn|nokhuf|nokchf|nokzar|nokcnh|nokaud|nokjpy|noksek|nokeur|nokhkd|noksgd|nokgbp|nokpln|nokcad|noknzd|chfmxn|chfhuf|chfnok|chfzar|chfcnh|chfaud|chfjpy|chfsek|chfeur|chfhkd|chfsgd|chfgbp|chfpln|chfcad|chfnzd|zarmxn|zarhuf|zarnok|zarchf|zarcnh|zaraud|zarjpy|zarsek|zareur|zarhkd|zarsgd|zargbp|zarpln|zarcad|zarnzd|cnhmxn|cnhhuf|cnhnok|cnhchf|cnhzar|cnhaud|cnhjpy|cnhsek|cnheur|cnhhkd|cnhsgd|cnhgbp|cnhpln|cnhcad|cnhnzd|audmxn|audhuf|audnok|audchf|audzar|audcnh|audjpy|audsek|audeur|audhkd|audsgd|audgbp|audpln|audcad|audnzd|jpymxn|jpyhuf|jpynok|jpychf|jpyzar|jpycnh|jpyaud|jpysek|jpyeur|jpyhkd|jpysgd|jpygbp|jpypln|jpycad|jpynzd|sekmxn|sekhuf|seknok|sekchf|sekzar|sekcnh|sekaud|sekjpy|sekeur|sekhkd|seksgd|sekgbp|sekpln|sekcad|seknzd|eurmxn|eurhuf|eurnok|eurchf|eurzar|eurcnh|euraud|eurjpy|eursek|eurhkd|eursgd|eurgbp|eurpln|eurcad|eurnzd|hkdmxn|hkdhuf|hkdnok|hkdchf|hkdzar|hkdcnh|hkdaud|hkdjpy|hkdsek|hkdeur|hkdsgd|hkdgbp|hkdpln|hkdcad|hkdnzd|sgdmxn|sgdhuf|sgdnok|sgdchf|sgdzar|sgdcnh|sgdaud|sgdjpy|sgdsek|sgdeur|sgdhkd|sgdgbp|sgdpln|sgdcad|sgdnzd|gbpmxn|gbphuf|gbpnok|gbpchf|gbpzar|gbpcnh|gbpaud|gbpjpy|gbpsek|gbpeur|gbphkd|gbpsgd|gbppln|gbpcad|gbpnzd|plnmxn|plnhuf|plnnok|plnchf|plnzar|plncnh|plnaud|plnjpy|plnsek|plneur|plnhkd|plnsgd|plngbp|plncad|plnnzd|cadmxn|cadhuf|cadnok|cadchf|cadzar|cadcnh|cadaud|cadjpy|cadsek|cadeur|cadhkd|cadsgd|cadgbp|cadpln|cadnzd|nzdmxn|nzdhuf|nzdnok|nzdchf|nzdzar|nzdcnh|nzdaud|nzdjpy|nzdsek|nzdeur|nzdhkd|nzdsgd|nzdgbp|nzdpln|nzdcad|audusd|cadusd|chfusd|cnhusd|eurusd|gbpusd|hkdusd|hufusd|jpyusd|mxnusd|nokusd|nzdusd|plnusd|sekusd|sgdusd|usdaud|usdcad|usdchf|usdcnh|usdeur|usdgbp|usdhkd|usdhuf|usdjpy|usdmxn|usdnok|usdnzd|usdpln|usdsek|usdsgd|usdzar|zarusd"""
        all_fields = full_list_of_tickers.split('|')
        all_currencies = list(set([i[0:3] for i in all_fields]))
        recent_snapshot = self.bloomberg_daily_tool.BDP(['usd'+i+' curncy' for i in all_currencies],'px_last')
        recent_snapshot['value']=recent_snapshot['value'].astype(float)
        recent_snapshot['value']=recent_snapshot['value'].fillna(1)
        inverse_snapshot = recent_snapshot.copy()
        inverse_snapshot['value']=1/inverse_snapshot['value']
        inverse_snapshot.index = [i[3:6]+i[0:3]+' curncy' for i in inverse_snapshot.index]
        inverse_snapshot.index.name = 'bbgTicker'
        real_time_price_constructor = pd.concat([inverse_snapshot, recent_snapshot])
        real_time_price_constructor['ticker'] = [i.split(' ')[0] for i in real_time_price_constructor.index]
        all_cross_constructor = real_time_price_constructor.set_index('ticker')[['value']]
        xmap = all_cross_constructor['value']
        all_non_usd = [i for i in list(set([i[0:3] for i in all_cross_constructor.index])) if i!='usd']
        all_non_usd_crosses = [''.join(x) for x in list(itertools.permutations(all_non_usd,2))]
        cross_creator = {}
        for ticker in all_non_usd_crosses:
            
            fthree=ticker[0:3]
            lthree=ticker[-3:]
            xcross =xmap[f'{fthree}usd']*xmap[f'usd{lthree}']
            cross_creator[ticker]=xcross
        live_cross_pricer = cross_creator
        live_price = pd.DataFrame(live_cross_pricer, index=[0]).transpose()
        live_price.index.name='ticker'
        live_price.columns=['live_price']
        return live_price

    def load_full_spx_history(self):
        raw_spx = self.tiingo_data_tool.raw_load_tiingo_data( ticker='SPY',
            start_date='2020-01-01',
            end_date=(datetime.datetime.now()+datetime.timedelta(2)).strftime('%Y-%m-%d'),
        )
        raw_spx['date']=pd.to_datetime(raw_spx['date'].apply(lambda x: x.strftime('%Y-%m-%d')))
        return raw_spx

    def generate_simple_equity_aligned_return_frame(self):
        fx_hist = self.output_full_historical_half_hourly_forex_history(update=True)
        fx_hist['simple_date']=pd.to_datetime(fx_hist['simple_date'])
        spx_history= self.load_full_spx_history()
        valid_spx_dates = list(spx_history['date'].unique())
        open_history = fx_hist[fx_hist['hour']=='09:30:00'].copy().groupby(['ticker','simple_date']).last()[['open']]
        
        close_history = fx_hist[fx_hist['hour']=='16:00:00'].copy().groupby(['ticker','simple_date']).last()[['open']]
        close_history.columns=['close']
        history = pd.concat([open_history, close_history],axis=1)
        history['spx_date']=history.index.get_level_values(1).isin(valid_spx_dates)
        history.sort_index(inplace=True)
        history['tick_copy']=history.index.get_level_values(0)
        history['tick_is1']= np.where(history['tick_copy']==history['tick_copy'].shift(1),1,np.nan)
        history['tick_is63']= np.where(history['tick_copy']==history['tick_copy'].shift(63),1,np.nan)
        history['tick_is126']= np.where(history['tick_copy']==history['tick_copy'].shift(63),1,np.nan)
        history['tick_is21']= np.where(history['tick_copy']==history['tick_copy'].shift(21),1,np.nan)
        history['tick_is252']= np.where(history['tick_copy']==history['tick_copy'].shift(21),1,np.nan)
        
        
        history['spx_date']= np.where(history.index.get_level_values(1)==
                 datetime.datetime.now().strftime('%Y-%m-%d'),True, history['spx_date'])
        sliced = history[history['spx_date']==True].copy()
        sliced['nt__z']=((sliced['ntRet']-sliced['ntRet'].fillna(0).rolling(126).mean())/sliced['ntRet'].fillna(0).rolling(126).std())*history['tick_is126']
        sliced['dt__z']=((sliced['dtRet']-sliced['dtRet'].fillna(0).rolling(126).mean())/sliced['dtRet'].fillna(0).rolling(126).std())*history['tick_is126']
        return history