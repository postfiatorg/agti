#from basic_utilities import regression as reg
from agti.utilities import regression as reg
from agti.data.bloomberg.standard_pulls import BloombergDailyDataTool
from agti.data.wikipedia.timeseries import WikipediaDataTool
from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.google_sheet_manager import GoogleSheetManager
import sqlalchemy
import pandas as pd
import itertools
import datetime
from agti.data.apple.apple_pricing import AppleProductRequester
import numpy as np 
class MosaicCreator:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.wikipedia_data_tool = WikipediaDataTool()
        self.bloomberg_daily_data_tool= BloombergDailyDataTool(pw_map=self.pw_map,
                       bloomberg_connection=True)
        self.google_sheet_manager = GoogleSheetManager(prod_trading=False)
        
        self.core_historical_data = self.load_combined_historical_timeseries_data()
        self.eps_expectations = self.load_eps_expectations_frame()

    def output_full_real_time_df_for_tickers(self, bbgTickers, field):
        bdp_df = self.bloomberg_daily_data_tool.BDP(bbgTickers=bbgTickers, field=field, overrides={}).copy()
        bdp_df['value']=bdp_df['value'].astype(float)
        bdp_df['date']=pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d'))
        all_bdh_history = []
        for xticker in bbgTickers:
            try:
                all_bdh_history.append(self.bloomberg_daily_data_tool.BDH(bbgTicker=xticker, field=field,
                                                   startDate='2006-01-01',overrides={}, 
                                                   endDate=datetime.datetime.now().strftime('%Y-%m-%d')))
            except:
                pass
        full_real_time_df = pd.concat([pd.concat(all_bdh_history).reset_index(),bdp_df.reset_index()])
        full_real_time_df = full_real_time_df.groupby(['bbgTicker','date']).last().sort_index()
        full_real_time_df['value']= full_real_time_df['value'].astype(float)
        return full_real_time_df

        
    def load_combined_historical_timeseries_data(self):
        bdh_to_cache = self.google_sheet_manager.load_google_sheet_as_df(workbook='manyasone',
                                                     worksheet='mosaic')
        mosaic_sheet = bdh_to_cache[(bdh_to_cache['data_source']
                     =='bloomberg')&(bdh_to_cache['report']
                     =='global_mosaic')].copy()
        mosaic_sheet['ticker_lower']=mosaic_sheet['ticker'].apply(lambda x: x.lower())
        core_ticks_to_load = list(set(mosaic_sheet[mosaic_sheet['ticker_lower']!='']['ticker_lower']))
        bdh_to_cache['ticker_lower']=bdh_to_cache['ticker'].apply(lambda x: x.lower())
        total_return_indicators= bdh_to_cache[bdh_to_cache['ret_type']=='tot_return_index_gross_dvds'].copy()
        last_price_indicators= bdh_to_cache[bdh_to_cache['ret_type']=='px_last'].copy()
        all_price_indicators_to_load = list(last_price_indicators['ticker_lower'].unique())
        all_total_return_indicators_to_load = list(total_return_indicators['ticker_lower'].unique())
        print('LOADING REAL TIME PX_LAST DATA')
        real_time_price_df  =self.output_full_real_time_df_for_tickers(bbgTickers=all_price_indicators_to_load, field='px_last')
        print("LOADING TOTAL RETURN DATA")
        tret_indicators  =self.output_full_real_time_df_for_tickers(bbgTickers=all_total_return_indicators_to_load, field='px_last')
        combined_historical_total_rets = pd.concat([tret_indicators, real_time_price_df])['value'].unstack(0).ffill()
        return combined_historical_total_rets

    def load_eps_expectations_frame(self):
        self.country_equity_index ={'US':'SPX INDEX',
                          'Canada': 'SPTSX INDEX',
                          'UK':'UKX INDEX',
                          'Europe':'SX5E INDEX',
                          'Switzerland':'SMI INDEX',
                          'Norway': 'OBX INDEX',
                          'Sweden': 'OMX INDEX',
                          'Japan':'NKY INDEX',
                          'Australia':'AS51 INDEX',
                          'New Zealand':'NZSE INDEX',
                          'US':'SPX INDEX',
                          'Canada': 'SPTSX INDEX',
                          'UK':'UKX INDEX',
                          'Europe':'SX5E INDEX',
                          'Switzerland':'SMI INDEX',
                          'Norway': 'OBX INDEX',
                          'Sweden': 'OMX INDEX',
                          'Japan':'NKY INDEX',
                          'Australia':'AS51 INDEX',
                          'New Zealand':'NZSE INDEX',
                          'China':'SHCOMP INDEX',
                          'Singapore':'STI INDEX',
                          'Korea':'KOSPI INDEX',
                          'India':'SENSEX INDEX',
                          'Thailand': 'SET50 INDEX',
                          'Taiwan': "TWSE INDEX",
                          'Czech': "PX INDEX",
                          'Hungary':'BUX INDEX',
                          'Russia':'IMOEX INDEX',
                          'Turkey': 'XU100 INDEX',
                          'South Africa':'JALSH INDEX',
                          'Brazil':'IBOV INDEX',
                          'Mexico':'MEXBOL INDEX',
                          'Chile': 'IPSA INDEX',
                          'Colombia':'IGBC INDEX',
                          'Poland': 'WIG20 INDEX'}
        
        yarr=[]
        for ind_to_work in self.country_equity_index.values():
            try:
                last_year=(datetime.datetime.now()-datetime.timedelta(365*2)).strftime('%Y-%m-%d')
                today=(datetime.datetime.now()).strftime('%Y-%m-%d')
                eps_hist=self.bloomberg_daily_data_tool.BDH(bbgTicker=ind_to_work.lower(), 
                                                            field='BEST_EPS',startDate=last_year, 
                                                            endDate=today,overrides={'BEST_FPERIOD_OVERRIDE':'1BF'})
                yarr.append(eps_hist)
            except:
                print(ind_to_work)
                pass
        
        full_df = pd.concat(yarr)
        full_df['value']=full_df['value'].astype(float)
        eps_expectations= full_df.reset_index().groupby(['date',
                                       'bbgTicker']).last()['value'].unstack().fillna(method='pad')
    
        return eps_expectations
        
    def generate_regional_reports(self):
        core_historical_data=self.core_historical_data
        mosaic = self.google_sheet_manager.load_google_sheet_as_df(workbook='manyasone',
                                                     worksheet='mosaic')
        mosaic['ticker']=mosaic['ticker'].apply(lambda x: x.lower())
        cix_constructor= mosaic[mosaic['data_source'] == 'cix'].copy()
        def convert_cix_ticker_to_ts(ticker_to_work = cix_constructor['ticker'].loc[1]):
        
            if '/' in ticker_to_work:
                ticker1= ticker_to_work.split('/')[0].lower()
                ticker2= ticker_to_work.split('/')[1].lower()
                op=core_historical_data[ticker1]/core_historical_data[ticker2]
        
            if '-' in ticker_to_work:
                ticker1= ticker_to_work.split('-')[0].lower()
                ticker2= ticker_to_work.split('-')[1].lower()
                op=core_historical_data[ticker1]-core_historical_data[ticker2]
            opx = pd.DataFrame(op)
            opx.columns=[ticker_to_work.lower()]
            return opx
        yarr=[]
        for xcix in list(cix_constructor['ticker']):
            try:
                yarr.append(convert_cix_ticker_to_ts(xcix))
            except:
                print(xcix)
                pass
        output_df = pd.concat(yarr,axis=1)
        all_indics = pd.concat([core_historical_data,output_df],axis=1)
        z_table = pd.concat([reg.rolling_z(all_indics,30)[-1:].mean(),
        reg.rolling_z(all_indics,90)[-1:].mean(),
        reg.rolling_z(all_indics,360)[-1:].mean()],axis=1)
        z_table.index.name='ticker'
        z_table.columns=['30','90','360']
        z_table['blended']=z_table[['30','90','360']].mean(1)
        classification_frame = z_table.sort_values('blended').reset_index()
        mosaic['ticker_lower']=mosaic['ticker'].apply(lambda x: x.lower())
        classification_frame['simple_name']=classification_frame['ticker'].map(mosaic.groupby('ticker_lower').last()['name'])
        classification_frame['region_report']=classification_frame['ticker'].map(mosaic.groupby('ticker_lower').first()['region'])
        classification_frame['blended_abs']=classification_frame['blended'].abs()
        americas_frame = classification_frame[classification_frame['region_report']=='Americas'].copy()
        americas_df = americas_frame.sort_values('blended_abs',ascending=False).head(10).copy()
        
        europe_frame = classification_frame[classification_frame['region_report']=='Europe'].copy()
        europe_frame = europe_frame.sort_values('blended_abs',ascending=False).head(10).copy()
        europe_frame['rank']=range(1,len(europe_frame)+1)
        
        
        asia_frame = classification_frame[classification_frame['region_report']=='Asia'].copy()
        asia_frame = asia_frame.sort_values('blended_abs',ascending=False).head(10).copy()
        asia_frame['rank']=range(1,len(asia_frame)+1)
        
        overall_frame = classification_frame[classification_frame['region_report'].apply(lambda x: x 
                                                                         in ['Europe',
                                                                             'Americas',
                                                                             'Asia'])].sort_values('blended_abs',
                                                                                                   ascending=False).head(10)
        overall_frame['region_report']='Overall'
        overall_report = pd.concat([americas_df,europe_frame,asia_frame,overall_frame])
        overall_report['create_date']=datetime.datetime.now()
        overall_report['rank']=range(1,len(overall_report)+1)
        return overall_report

    def generate_momentum_reports(self):
        
        core_historical_data=self.core_historical_data
        mosaic = self.google_sheet_manager.load_google_sheet_as_df(workbook='manyasone',
                                                     worksheet='mosaic')
        mosaic['ticker']=mosaic['ticker'].apply(lambda x: x.lower())
        cix_constructor= mosaic[mosaic['data_source'] == 'cix'].copy()
        def convert_cix_ticker_to_ts(ticker_to_work = cix_constructor['ticker'].loc[1]):
        
            if '/' in ticker_to_work:
                ticker1= ticker_to_work.split('/')[0].lower()
                ticker2= ticker_to_work.split('/')[1].lower()
                op=core_historical_data[ticker1]/core_historical_data[ticker2]
        
            if '-' in ticker_to_work:
                ticker1= ticker_to_work.split('-')[0].lower()
                ticker2= ticker_to_work.split('-')[1].lower()
                op=core_historical_data[ticker1]-core_historical_data[ticker2]
            opx = pd.DataFrame(op)
            opx.columns=[ticker_to_work.lower()]
            return opx
        yarr=[]
        for xcix in list(cix_constructor['ticker']):
            try:
                yarr.append(convert_cix_ticker_to_ts(xcix))
            except:
                print(xcix)
                pass
        output_df = pd.concat(yarr,axis=1)
        all_indics = pd.concat([core_historical_data,output_df],axis=1)
        z_table = pd.concat([reg.rolling_z(all_indics,30)[-1:].mean(),
        reg.rolling_z(all_indics,90)[-1:].mean(),
        reg.rolling_z(all_indics,360)[-1:].mean()],axis=1)
        z_table.index.name='ticker'
        z_table.columns=['30','90','360']
        z_table['blended']=z_table[['30','90','360']].mean(1)
        
        #bloomberg_mosaic1=pd.concat([americas_df, europe_frame,asia_frame,overall_frame])
        country_ref_frame = mosaic[mosaic['country_ref']!=''].copy()
        country_ref_frame['30']=country_ref_frame['ticker'].map(z_table['30'])
        country_ref_frame['90']=country_ref_frame['ticker'].map(z_table['90'])
        country_ref_frame['360']=country_ref_frame['ticker'].map(z_table['360'])
        country_ref_frame['blended']=country_ref_frame['ticker'].map(z_table['blended'])
        a=country_ref_frame[country_ref_frame['report']=='em_equity'][['country_ref','blended']].set_index('country_ref')
        b=country_ref_frame[country_ref_frame['report']=='em_fxtr'][['country_ref','blended']].set_index('country_ref')
        c=country_ref_frame[country_ref_frame['report']=='em_fxvol'][['country_ref','blended']].set_index('country_ref')
        d=country_ref_frame[country_ref_frame['report']=='em_rates'][['country_ref','blended']].set_index('country_ref')
        summary_tab= pd.concat([a,b,c,d],axis=1)
        summary_tab.columns=['equity','fx','vol','rates']
        a_dm=country_ref_frame[country_ref_frame['report']=='g10_equity'][['country_ref','blended']].groupby('country_ref').last()
        b_dm=country_ref_frame[country_ref_frame['report']=='g10fx'][['country_ref','blended']].groupby('country_ref').last()
        c_dm=country_ref_frame[country_ref_frame['report']=='g10_fxvol'][['country_ref','blended']].groupby('country_ref').last()
        d_dm=country_ref_frame[country_ref_frame['report']=='g10_rates'][['country_ref','blended']].groupby('country_ref').last()
        summary_tabx= pd.concat([a_dm,b_dm,c_dm,d_dm],axis=1)
        summary_tabx.columns=['equity','fx','vol','rates']
        movement = pd.concat([summary_tab,summary_tabx])
        country_equity_index=self.country_equity_index
        xdfx= pd.DataFrame(country_equity_index,index=[0]).transpose()
        xdfx.index.name='country'
        xdfx.columns=['equity_index']
        xdfx['equity_index']=xdfx['equity_index'].apply(lambda x: x.lower())
        
        eps_expects_resampled = self.eps_expectations.resample('D').last().fillna(method='pad')
        zscore_eps=(reg.rolling_z(eps_expects_resampled,30)+reg.rolling_z(eps_expects_resampled,90)+reg.rolling_z(eps_expects_resampled,365))/3
        def reverse_map(my_map):
            reversed_map = {}
            for key, value in my_map.items():
                valuex = value.lower()
                reversed_map[valuex] = key
            return reversed_map
        reversed_map = reverse_map(country_equity_index)
        zscore_eps.columns= [reversed_map[i] for i in zscore_eps]
        movement['eps']=zscore_eps[-1:].mean()
        movement['create_date']=datetime.datetime.now()
        return movement.reset_index()

    def generate_vw_ret_report(self):
            timeseries_display = self.core_historical_data[['spy us equity','tlt us equity','gld us equity',
                                       'dbc us equity','dxy index','xbtusd index']].copy()
            timeseries_display.columns=['S&P','30Y','GLD','COM','USD','BTC']
            hist_ret = timeseries_display.pct_change(1)[-366*2:]
            historical_ts = (((.01/hist_ret.rolling(90).std().shift(2)) * hist_ret)[-366:]+1).cumprod()[-366:]
            return historical_ts.reset_index()
    
    def generate_correl_report(self):
        timeseries_display = self.core_historical_data[['spy us equity','tlt us equity','gld us equity',
                                   'dbc us equity','dxy index','xbtusd index']].copy()
        timeseries_display.columns=['S&P','30Y','GLD','COM','USD','BTC']
        hist_ret = timeseries_display.pct_change(1)[-366*2:]
        correl_report = (hist_ret.tail(90).corr()+hist_ret.tail(365).corr())/2
        correl_report['create_date']=datetime.datetime.now()
        correl_report.index.name='asset'
        return correl_report.reset_index()
        
    def generate_core_statistics_report(self):
        equity_indices = [i.lower() for i in list(self.country_equity_index.values())]

        best_pe_map = self.bloomberg_daily_data_tool.BDP(equity_indices, 
                                           field='best_pe_ratio',overrides={})['value'].astype(float)
        #xdfx['best_pe_ratio']=xdfx['equity_index'].map(best_pe_map)
        core_historical_data=self.core_historical_data
        mosaic = self.google_sheet_manager.load_google_sheet_as_df(workbook='manyasone',
                                                     worksheet='mosaic')
        country_constructor= pd.DataFrame([i for i in list(set(mosaic['country_ref'])) if i!=''])
        country_constructor.columns=['country']
        country_constructor['equity_index']=country_constructor['country'].map(self.country_equity_index)
        country_constructor['equity_index']=country_constructor['equity_index'].apply(lambda x: x.lower())
        rate_df =mosaic[mosaic['report'].apply(lambda x: 'rate' in x.lower())].copy()
        rate_df['name']=rate_df['name'].apply(lambda x: x.replace('2','5'))
        rate_df['rate']=rate_df['ticker'].apply(lambda x: x.replace('2','5'))

        country_constructor['5y_rate']=country_constructor['country'].map(rate_df.groupby('country_ref').first()['rate'])
        country_constructor['5y_rate']=country_constructor['5y_rate'].apply(lambda x: str(x).lower())
        country_constructor['5y_rate']=np.where(country_constructor['country']=='Taiwan','gvtwtl5 index',country_constructor['5y_rate'])
        country_constructor['5y_rate']=np.where(country_constructor['country']=='Brazil','gebr5y index',country_constructor['5y_rate'])
        country_constructor['5y_rate']=np.where(country_constructor['country']=='Turkey','iecm5y index',country_constructor['5y_rate'])

        rate_xdf = self.bloomberg_daily_data_tool.BDP(list(country_constructor['5y_rate']),
                                           field='px_last')
        country_constructor['5y_yield']=country_constructor['5y_rate'].map(rate_xdf['value'].astype(float))/100
        best_pe_ratio_df= self.bloomberg_daily_data_tool.BDP(list(country_constructor['equity_index']),
                                           field='best_pe_ratio')

        country_constructor['equity_pe_ratio']=country_constructor['equity_index'].map(best_pe_ratio_df['value'].astype(float))
        country_constructor['erp']=(1/country_constructor['equity_pe_ratio'])-country_constructor['5y_yield']

        revny_xdf = self.bloomberg_daily_data_tool.BDP(list(country_constructor['equity_index']),
                                           field='IDX_EST_SALES_NXT_YR')

        revcy_xdf = self.bloomberg_daily_data_tool.BDP(list(country_constructor['equity_index']),
                                           field='IDX_EST_SALES_CURR_YR')
        growth =(revny_xdf['value'].astype(float)-revcy_xdf['value'].astype(float))/revcy_xdf['value'].astype(float)
        country_constructor['growth']=country_constructor['equity_index'].map(growth)

        fx_constructor = mosaic[(mosaic['ticker'].apply(lambda x: 'usd' in x.lower())) &
              (mosaic['country_ref'].apply(lambda x: x!=''))].groupby('country_ref').first()

        country_to_fx = 'usd'+fx_constructor['ticker'].apply(lambda x: x[0:6]).apply(lambda x: str(x).lower().replace('usd','')) + ' curncy'
        country_constructor['fx_conversion']=country_constructor['country'].map(country_to_fx)

        country_constructor['big_mac']=country_constructor['country'].map(mosaic[mosaic['report']=='bigmac'].groupby('country_ref').first()['ticker'])
        fx_rate_conv= self.bloomberg_daily_data_tool.BDP(list(country_constructor['fx_conversion']),
                                           field='px_last')
        country_constructor['fx_rate']=country_constructor['fx_conversion'].map(fx_rate_conv['value'].astype(float)).fillna(1)
        big_mac_local = self.bloomberg_daily_data_tool.BDP(list(country_constructor['big_mac']),
                                           field='px_last')



        country_constructor['local_bigmac']=country_constructor['big_mac'].apply(lambda x: x.lower()).map(big_mac_local['value'].astype(float))
        country_constructor['bigmacusd']=country_constructor['local_bigmac']/country_constructor['fx_rate']
        bigmacusd = country_constructor[country_constructor['country']=='US']['bigmacusd'].mean()
        country_constructor['ppp_premium_vsUSD']=(country_constructor['bigmacusd']-bigmacusd)/bigmacusd
        apple_product_requester=AppleProductRequester()
        price_df = apple_product_requester.create_full_apple_product_price_df(product_line='ipad',
                                                                   product_name = 'ipad-pro')
        uniform_skus = price_df.groupby(['sku','country']).last()['price'].unstack().astype(float)
        has_all_sku = uniform_skus.dropna().copy()
        ind_to_select = has_all_sku.index[int(round(len(has_all_sku)/2,0))]
        median_sku_price_local=has_all_sku.loc[ind_to_select]
        country_constructor['apple_product']=country_constructor['country'].map(median_sku_price_local).astype(float)
        country_constructor['apple_product__usd']=country_constructor['apple_product']/country_constructor['fx_rate']
        appleprodusd = country_constructor[country_constructor['country']=='US']['apple_product__usd'].mean()
        country_constructor['ppp_premium_vsUSD__apple']= (country_constructor['apple_product__usd']-appleprodusd)/appleprodusd
        country_constructor['ppp_premium_vsUSD__apple']=reg.cap_df(country_constructor[['ppp_premium_vsUSD__apple']],.8)
        ppp_constructor = country_constructor[['country','ppp_premium_vsUSD__apple','ppp_premium_vsUSD']].set_index('country')
        weighter =ppp_constructor.apply(lambda x: np.sign(x+50)) * {'ppp_premium_vsUSD__apple':.2,'ppp_premium_vsUSD':.8}
        proper_ppp_weight=weighter.divide(weighter.sum(1),axis=0)
        country_constructor['ppp_pctp']=country_constructor['country'].map((proper_ppp_weight*ppp_constructor).sum(1))
        core_statistics= country_constructor[['country',
                             'equity_pe_ratio',
                             '5y_yield',
                             'erp','growth',
                             'ppp_pctp']].sort_values('ppp_pctp')
        core_statistics['create_date']=datetime.datetime.now()
        core_statistics['apple_product_ref']='ipad__ipad-pro'
        return core_statistics
    def generate_wikipedia_trends_report(self):

        trend_block_df = self.google_sheet_manager.load_google_sheet_as_df(workbook='manyasone',
                                                                           worksheet='wiki_trend_mosaic')
        trend_block_df['trend_split']=trend_block_df['Trend Block'].apply(lambda x: x.split('|'))

        memes= list(set(list(itertools.chain.from_iterable(list(trend_block_df['Trend Block'].apply(lambda x: 
                                                                                    x.split('|')))))))
        warr=[]
        for memex in memes:
            try:
                xdfw = self.wikipedia_data_tool.get_recent_wiki_trends_for_page_url(page_url=memex)
                warr.append(xdfw)
            except:
                print(memex)
                pass

        all_trends= pd.concat(warr).groupby(['date','page_name']).last()['value'].unstack()
        trend_to_block = trend_block_df.groupby('Trend Name').first()['trend_split']
        yarr=[]
        for trend_to_work in list(trend_to_block.index):
            trend_df = pd.DataFrame(all_trends[[i for i in all_trends.columns 
                                                if i in trend_to_block[trend_to_work]]].sum(1))
            trend_df.columns=[trend_to_work]
            yarr.append(trend_df)
        mega_trends =pd.concat(yarr,axis=1)
        trend_smoothed= mega_trends.fillna(0).rolling(28).sum()
        yoy_growth = trend_smoothed.pct_change(365)[-2:].mean()
        qoq_growth = trend_smoothed.pct_change(90)[-2:].mean()
        total_momentum =trend_smoothed[-2:].mean()-trend_smoothed[-365:].mean()
        trend_dex = pd.concat([yoy_growth,qoq_growth,total_momentum],axis=1)
        trend_dex.index.name='Trend'
        trend_dex.columns=['YoY','QoQ','Scale']
        trend_scoring = ((trend_dex.rank()-trend_dex.rank().mean())/trend_dex.rank().std())*58.98
        trend_scoring['Overall']=trend_scoring[['YoY','QoQ','Scale']].mean(1)
        toutput = trend_scoring.sort_values('Overall',ascending=False).head(15).copy()
        toutput['create_date']=datetime.datetime.now()
        return toutput.reset_index()

    def write_all_macro_reports(self):
        #wiki_name = 'uugoodalexander___wikimosaic_raw'
        #wikimosaic_report= self.generate_wikipedia_trends_report()
        #dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection()
        #wikimosaic_report.to_sql(wiki_name, dbconnx, if_exists='replace', index=False)
        #print('wrote goodalexander macro wikimosaic report')
        
        correl_name='uugoodalexander___macrocorrel_raw'
        correl_report= self.generate_correl_report()
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        correl_report.to_sql(correl_name, dbconnx, if_exists='replace', index=False)
        print('wrote goodalexander macro correlation report')
        
        vwret_name='uugoodalexander___macrovwret_raw'
        vw_ret_report= self.generate_vw_ret_report()
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        vw_ret_report.to_sql(vwret_name, dbconnx, if_exists='replace', index=False)
        print('wrote goodalexander macro vw return report')
        
        momentum_name='uugoodalexander___macromomentum_raw'
        momentum_report= self.generate_momentum_reports()
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        momentum_report.to_sql(momentum_name, dbconnx, if_exists='append', index=False)
        print('wrote goodalexander momentum report')
        
        regional_name='uugoodalexander___macroregional_raw'
        regional_reports = self.generate_regional_reports()
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        regional_reports.to_sql(regional_name, dbconnx, if_exists='append', index=False)
        print('wrote goodalexander macro regional report')
        
        core_name='uugoodalexander___macrocorestats_raw'
        core_stats_report = self.generate_core_statistics_report()
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        core_stats_report.to_sql(core_name, dbconnx, if_exists='append', index=False)
        print('wrote goodalexander macro core stats report')
            
    def output_full_macro_report_map(self): 
        report_map = {}
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        correl_name='uugoodalexander___macrocorestats_raw'
        report_map['macrocorestats_raw']= pd.read_sql(correl_name, dbconnx)
        
        dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        macrovw='uugoodalexander___macrovwret_raw'
        report_map['macrovwret_raw']= pd.read_sql(macrovw, dbconnx)
        
        #momentum_name='uugoodalexander___macromomentum_raw'
        # 'wikimosaic_raw'
        for xreport in ['macromomentum_raw','macroregional_raw','macrocorestats_raw','macrocorrel_raw']:
            sql_query = f'SELECT * FROM uugoodalexander___{xreport} WHERE create_date = (SELECT MAX(create_date) FROM uugoodalexander___{xreport});'
            #momentum_report= self.generate_momentum_reports()
            dbconnx= self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
            report_map[xreport]= pd.read_sql_query(sql_query,dbconnx)
        return report_map