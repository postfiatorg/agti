import numpy as np
from agti.utilities import regression as reg
from agti.live_trading.universe_generation.fast_equity_pull import FastEquityPull
import pandas as pd
from agti.data.fmp.market_data import FMPMarketDataRetriever
import datetime
class ETradeRiskTool:
    def __init__(self, etrade_tool, pw_map):
        # pass in an initialized etrade risk tool 
        self.etrade_tool = etrade_tool
        self.pw_map = pw_map
        self.fast_equity_pull= FastEquityPull(pw_map=pw_map)
        self.fmp_market_data_retriever = FMPMarketDataRetriever(pw_map=pw_map)
    def load_positional_risk_frame(self):
        key_list_of_betas = ['SPY','IWM','GLD','USO','EFA','FXI','QQQ','UUP']
        current_positions =self.etrade_tool.get_current_position_df()
        current_mvalue = current_positions.groupby('symbolDescription').first()['marketValue']
        full_ticker_list=list(set(list(current_mvalue.index)+key_list_of_betas))
        full_hist = self.fast_equity_pull.get_full_equity_history_for_ticker_list(list_of_tickers=full_ticker_list, start_date='2020-01-01')
        full_hist['tick_copy']=full_hist.index.get_level_values(0)
        full_hist['tick_is1']=np.where(full_hist['tick_copy']==full_hist['tick_copy'].shift(1),1,np.nan)
        full_hist['ntRet']=(full_hist['openadj']-full_hist['closeadj'].shift(1))/full_hist['closeadj'].shift(1)*full_hist['tick_is1']
        full_hist['dtRet']=(full_hist['closeadj']-full_hist['openadj'])/full_hist['openadj']
        full_hist['portfolio_alloc'] = full_hist.index.get_level_values(0).map(current_mvalue)
        return full_hist
    def output_overnight_risk_frame(self):
        full_hist = self.load_positional_risk_frame()
        key_list_of_betas = ['SPY','IWM','GLD','USO','EFA','FXI','QQQ','UUP']
        nt_pnl=(full_hist['portfolio_alloc']*full_hist['ntRet']).groupby('date').sum()
        dt_pnl=(full_hist['portfolio_alloc']*full_hist['dtRet']).groupby('date').sum()
        ticker_to_beta='SPY'
        xmap = {}
        for ticker_to_beta in key_list_of_betas:
            beta_to_work = reg.calculate_rolling_beta_of_ts(ts_to_measure=nt_pnl, 
                                             market_ts=full_hist['ntRet'].loc[ticker_to_beta], win=63)
            xmap[ticker_to_beta] = beta_to_work[-1:].mean()
        beta_frame = pd.DataFrame(xmap, index=[0]).transpose()
        beta_frame.columns=['value']
        beta_frame.index = [i+' BETA' for i in beta_frame.index]
        one_year_max_dd = nt_pnl[-252:].min()
        three_year_max_dd = nt_pnl[-252*3:].min()
        all_time_max_dd = nt_pnl.min()
        risk_df = pd.DataFrame({'one_year_max_dd':one_year_max_dd,
                                'three_year_max_dd':three_year_max_dd,
         'all_time_max_dd':all_time_max_dd}, index=[0]).transpose()
        risk_df.columns=['value']
        etrade_overnight_risk = pd.concat([beta_frame, risk_df])
        etrade_overnight_risk.index.name='ETrade Overnight Risk'
        etrade_overnight_risk['date']=datetime.datetime.now()
        etrade_overnight_risk.index.name = 'field'
        etrade_overnight_risk['brokerage']='etrade'
        return etrade_overnight_risk
    
    def output_morning_realized_pnl_df(self):
        executed_order_df = self.etrade_tool.output_executed_order_df()
        this_mornings_fills = executed_order_df[(executed_order_df['execution_hour']==9)&(executed_order_df['has_execution_price']==True)
        &(executed_order_df['execution_date']==datetime.datetime.now().strftime("%Y-%m-%d"))].copy()
        rt_px_fm = self.etrade_tool.tiingo_data_tool.output_tiingo_real_time_price_frame()
        all_symbols_to_work = list(this_mornings_fills['symbol'].unique())
        all_syms= self.fmp_market_data_retriever.retrieve_batch_equity_data(symbols=all_symbols_to_work,batch_size=500)
        symbol_to_open= all_syms.groupby('symbol').first()['open']
        this_mornings_fills['recorded_open']=this_mornings_fills['symbol'].map(symbol_to_open)
        ticker_to_close = rt_px_fm.groupby('ticker').last()['prevClose']
        this_mornings_fills['previous_close']=this_mornings_fills['symbol'].map(ticker_to_close)
        this_mornings_fills['pnl_direction'] = this_mornings_fills['orderAction'].map({'BUY_TO_COVER':-1, 'SELL':1, 'SELL_SHORT':1, 'BUY':-1})
        this_mornings_fills['pnl']=(this_mornings_fills['averageExecutionPrice']-this_mornings_fills['previous_close']) * this_mornings_fills['filledQuantity']*this_mornings_fills['pnl_direction']
        this_mornings_fills['slippage']=(this_mornings_fills['averageExecutionPrice']
                                        -this_mornings_fills['recorded_open'])* this_mornings_fills['filledQuantity']*-this_mornings_fills['pnl_direction']
        return this_mornings_fills