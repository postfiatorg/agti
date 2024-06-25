import pandas as pd
import numpy as np

''' the purpose of this library is to expose commonly used regression functions'''

def float_or_die(x):
    ret = np.nan
    try:
        ret = float(x)
    except:
        pass
    return ret

def spearmanAcrossDf(x):
    return x.rank(1).apply(lambda x: (x-x.mean())/x.std(),1)


def spearman_z_df(df):
    ''' takes a dataframe and spearman zs the things'''
    tdf=df.rank(1)
    tdf2=tdf.subtract(tdf.mean(1),axis=0)
    tdf3=tdf2.divide(tdf.std(1), axis=0)
    return tdf3

def z_df(df):
    tdf= df.subtract(df.mean(1),axis=0)
    tdf2=tdf.divide(df.std(1), axis=0)
    return tdf2

def mean_multiple_fields(original_panel,field_list,date_field):
    arr=[]
    for x in field_list:
        arr.append(original_panel[x])
    resultant=pd.concat(arr)
    meaned=resultant.reset_index().groupby(date_field).mean()
    return meaned

def cap_df(xdf,cap):
    capped=np.where(xdf.abs() > cap,xdf.apply(lambda x:np.sign(x)) * cap ,xdf) * xdf.apply(lambda x: np.sign(x).abs())
    return capped

def rolling_z(xdf,window):
    a=xdf.subtract(xdf.rolling(window).mean(), axis=0)
    b= a.divide(xdf.rolling(window).std(), axis=0)
    return b

def zAcc(x):
    return x.apply(lambda x: (x-x.mean())/x.std(),1)

def make_df_date_mapper(df_to_work,date_string_column):
    '''enter a dataframe and a column with a list of dates in it and get back
    a mapping between those dates and datetimes
    example use: make_df_date_mapper(df_to_work=temp_df, date_string_column='Day')
    '''
    mapper_df=df_to_work[[date_string_column]].drop_duplicates()
    mapper_df['data_date']=pd.to_datetime(mapper_df[date_string_column])
    return mapper_df.set_index(date_string_column)['data_date']


def rawChg(tsX):
    return (tsX-tsX.shift(1)).fillna(0)

def multiframe_z(ts, window_views = [21,42,63]):
    arr = []
    for x in window_views:
        arr.append(rolling_z(ts, x))
    output=pd.concat(arr,1).mean(1)
    return output


def indicator_timseseries_strategy(asset_ret,
                                   ref_timeseries,
                                   z_indicator, 
                                   size_up_by_indic,
                                   window,
                                   enter_thresh,
                                   exit_thresh,
                                   lag_days,
                                   tcost_bps):

    '''
    asset_ret = to_trade_df.adjClose.pct_change(1)
    This is the assets total return series
    ref_timeseries = -reg.rolling_z(to_trade_df.adjClose.pct_change(21),252)
    This is the indicator to trade the asset with
    z_indicator= False
    This is whether or not to make the indicator into a z score 
    window = 252
    The window is only relevant if you opt for z score
    enter_thresh = 1.5
    This is the threshhold at which you enter a long position or a short
    exit_thresh = .4
    This is the threshhold at which you exit said position
    lag_days = 5
    This is how many days the indicator is lagged
    tCostBps = 1
    This is your basis points transaction cost assumption 
    '''

    def raw_chg(ts):
        return (ts-ts.shift(1)).fillna(0)
    ref_timeseries=ref_timeseries.shift(lag_days)
    z= ref_timeseries
    if z_indicator == True:
        # z score of indicator
        #z = (ref_timeseries - ref_timeseries.rolling(window))/pd.rolling_std(ref_timeseries, window)
        z= (ref_timeseries - ref_timeseries.rolling(window).mean())/(ref_timeseries.rolling(window).std())
        z = z
    cross_above_top = np.sign(raw_chg(np.sign(z - enter_thresh)) -2)+1
    ''' cross above top signals that you're betting on mean reversion ''' 
    cross_below_top = np.abs(np.sign(raw_chg(np.sign(z - exit_thresh)) + 2) - 1)
    x1 = pd.DataFrame(cross_above_top - cross_below_top)
    #x1 = np.where(x1 == 0,np.nan,x1)
    x1[1] = -1
    #x1 = x1.fillna(method = 'pad')
    temp_cross=pd.concat([cross_above_top, cross_below_top],1)
    temp_cross.columns = ['cross_above_top', 'cross_below_top']
    temp_cross['integ'] = temp_cross['cross_above_top'] - temp_cross['cross_below_top']
    temp_cross['integ']=temp_cross.integ.apply(lambda x: np.where(x == 0,np.nan, x))
    temp_cross['integ'][1] = -1
    temp_cross['integ'] = temp_cross.integ.fillna(method='pad')
    x1=temp_cross['integ']

    enter_days= np.sign(raw_chg(x1) -2)+1
    exit_days= np.abs(np.sign(raw_chg(x1)+2) -1)
    enters_and_exits=pd.concat([enter_days,exit_days],axis = 1)
    buys_and_sells = enter_days - exit_days
    hold_periods = buys_and_sells.cumsum()

    cross_below_bottom = np.abs(np.sign(raw_chg(np.sign(z + enter_thresh)) +2) - 1)
    cross_above_bottom = np.sign(raw_chg(np.sign(z + exit_thresh)) - 2) + 1
    temp_below_cross = pd.concat([cross_below_bottom, cross_above_bottom],1)
    temp_below_cross.columns = ['cross_below_bottom','cross_above_bottom']
    temp_below_cross['integ_down']= temp_below_cross.cross_below_bottom - temp_below_cross.cross_above_bottom
    temp_below_cross['integ_down'] = np.where(temp_below_cross['integ_down'] == 0, np.nan,temp_below_cross['integ_down'])
    temp_below_cross['integ_down'][1] = -1
    temp_below_cross['integ_down']= temp_below_cross['integ_down'].fillna(method='pad')
    y1= temp_below_cross['integ_down']
    short_enter_days = np.sign(raw_chg(y1)-2) + 1
    short_exit_days = np.abs(np.sign(raw_chg(y1) +2)-1)
    short_and_covers = short_enter_days - short_exit_days
    hold_short_periods = short_and_covers.cumsum()
    total_hold_periods = hold_periods - hold_short_periods
    
        
    long_profit_series = (hold_periods*asset_ret) - ((tcost_bps/10000) * enter_days)
    short_profit_series = (hold_short_periods * -asset_ret) - ((tcost_bps/10000) * short_enter_days)
    total_profit_series = long_profit_series + short_profit_series
    if size_up_by_indic ==True:
        long_profit_series = (hold_periods*asset_ret * z.abs()) - ((tcost_bps* z.abs()/10000) * enter_days)
        short_profit_series = (hold_short_periods * -asset_ret* z.abs()) - ((tcost_bps * z.abs()/10000) * short_enter_days)
        total_profit_series = long_profit_series + short_profit_series
    
        
    df = pd.concat([total_profit_series, z, short_enter_days, 
                    short_exit_days, enter_days, exit_days, 
                    total_hold_periods,
                    long_profit_series,short_profit_series], axis=1)
    df.columns = ['pnl', 'z', 'short_enter', 'short_exit', 'enter', 'exit', 'current_position','long_pnl','short_pnl']
    return df
def max_dd_index(pnl_index):
    xs=pnl_index 
    i = np.argmax(np.maximum.accumulate(xs) - xs) # end of the period
    j = np.argmax(xs[:i]) # start of period
    return(xs[i]-xs[j])/xs[i]

def rolling_daily_sharpe(xdf, window):
    ''' get the rolling sharpe ratio for a window''' 
    r = (xdf.rolling(window).mean() * np.sqrt(252))/(xdf.rolling(window).std())
    return r

def sharpe_ratio_ts(xts):
    return (xts.mean() * np.sqrt(252))/ (xts.std())



def xper_sum_dropna(xdf,xwin=4): 
    arr=[]
    for xcol in xdf.columns:
        tdf = xdf[xcol].dropna().rolling(xwin).sum()
        arr.append(tdf)
    xdf2 = pd.concat(arr,1)
    return xdf2

def xper_mean_dropna(xdf,xwin=4): 
    arr=[]
    for xcol in xdf.columns:
        tdf = xdf[xcol].dropna().rolling(xwin).mean()
        arr.append(tdf)
    xdf2 = pd.concat(arr,1)
    return xdf2

def xper_std_dropna(xdf,xwin=4): 
    arr=[]
    for xcol in xdf.columns:
        tdf = xdf[xcol].dropna().rolling(xwin).std()
        arr.append(tdf)
    xdf2 = pd.concat(arr,1)
    return xdf2

def xper_delta_dropna(xdf,xwin=4): 
    arr=[]
    for xcol in xdf.columns:
        tdfa = xdf[xcol].dropna()
        tdf = tdfa-tdfa.shift(4)
        arr.append(tdf)
    xdf2 = pd.concat(arr,1)
    return xdf2


def xper_pct_change_dropna(xdf,xwin=4): 
    arr=[]
    for xcol in xdf.columns:
        tdfa = xdf[xcol].dropna()
        tdf = (tdfa-tdfa.shift(xwin))/tdfa.shift(xwin)
        arr.append(tdf)
    xdf2 = pd.concat(arr,1)
    return xdf2

def xper_z_dropna(xdf,xwin=4): 
    xdf2 = (xdf - xper_mean_dropna(xdf= xdf, xwin=xwin))/xper_std_dropna(xdf= xdf, xwin=xwin)
    return xdf2

def calculate_rolling_beta_of_ts(ts_to_measure,market_ts,win=63):
    '''example ts_to_measure=daily_pnl__pct,market_ts = 'SPY',
                                 ret_type='tRet', ret_frame=divver'''

    cov = ts_to_measure.rolling(win).cov(market_ts)
    mvar= market_ts.rolling(win).var()
    rolling_beta= cov/mvar
    return rolling_beta

def calculate_rolling_beta_of_ts_with_min(ts_to_measure,market_ts,win=63,min_beta=.2):
    '''example ts_to_measure=daily_pnl__pct,market_ts = 'SPY',
                                 ret_type='tRet', ret_frame=divver'''

    cov = ts_to_measure.rolling(win).cov(market_ts)
    mvar= market_ts.rolling(win).var()
    rolling_beta= cov/mvar
    return rolling_beta.apply(lambda x: max(x, min_beta))

def conv_column_to_dates_fast(tdf, date_col, output_name):
    ''' adds a mapped date on to the original dataframe, tdf
    using a string of date col -- uses set to make more eff
    
    tdf, date_col, output_name '''
    date_df = pd.DataFrame(set(tdf[date_col]))
    date_df['dt']=pd.to_datetime(date_df[0])
    datestr_to_datetime_map =date_df.groupby(0).last()['dt']
    tdf[output_name]=tdf[date_col].map(datestr_to_datetime_map)