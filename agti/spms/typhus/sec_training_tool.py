from agti.utilities.google_sheet_manager import GoogleSheetManager
from agti.utilities.db_manager import DBConnectionManager
import pandas as pd
import numpy as np
import time
from agti.data.sec_methods.sec_filing_update import SECFilingUpdateManager
class SECSkeletonTool:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        self.sec_filing_update_manager = SECFilingUpdateManager(pw_map=pw_map, user_name='agti_corp')
        self.google_sheet_manager = GoogleSheetManager(prod_trading=False)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        sharadar_ticker_table = self.load_sharadar_ticker_table()
        self.sharadar_dexed= sharadar_ticker_table.groupby('ticker').first()
        self.initial_ticker = self.check_initial_ticker()
        
    def load_ticker_full_financial_history(self,ticker_value = 'ORCL'):
        dbconnx =self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        full_df_hist = pd.read_sql(f"""select fcf,netinc,capex,ncfo,ticker,datekey,calendardate 
        from sharadar__sf1 where (ticker = '{ticker_value}') 
        AND (dimension='ARQ') AND (calendardate > '2014-01-01')""",dbconnx)# .sort_values('datekey').groupby('calendardate').first().reset_index().set_index('datekey')
        full_df_hist['datekey']=pd.to_datetime(full_df_hist['datekey'])
        full_df_hist=full_df_hist.sort_values('datekey').groupby('calendardate').first().reset_index().set_index('datekey')
        xdf = full_df_hist[[i for i in full_df_hist.columns if ('ticker' not in i)&('calendardate' not in i)]]
        varr=[]
        xdf1= xdf.copy()
        for iterator in [2,3,4]:
            ydf = xdf.rolling(iterator).sum()
            ydf.columns=[i+f'__{iterator}' for i in ydf.columns]
            varr.append(ydf)
        xdf1.columns=[i+'__1' for i in xdf1.columns]
        ydf = pd.concat([xdf1,pd.concat(varr,axis=1)],axis=1)
        return ydf
    
    def output_ticker_joined_df(self,ticker = 'ORCL'):
        
        full_fin_history= self.load_ticker_full_financial_history(ticker_value=ticker)
        full_transcript_history = self.sec_filing_update_manager.load_historical_sec_docs_for_ticker(ticker=ticker, 
                                                                                                     get_history=True)
        def join_financial_and_transcript_data(full_fin_history, full_transcript_history):
            """
            Join financial history with transcript history based on closest reportDate.
            
            Args:
            full_fin_history (pd.DataFrame): DataFrame with financial history, index is datekey
            full_transcript_history (pd.DataFrame): DataFrame with transcript history
            
            Returns:
            pd.DataFrame: Joined DataFrame with closest matching reportDate and url
            """
            # Filter and prepare transcript history
            transcript_data = full_transcript_history[full_transcript_history['eightk_eps']==True][['reportDate','url']]
            transcript_data['reportDate'] = pd.to_datetime(transcript_data['reportDate'])
            transcript_data = transcript_data.sort_values('reportDate')
        
            # Ensure index is datetime
            full_fin_history.index = pd.to_datetime(full_fin_history.index)
        
            # Function to find closest date
            def find_closest_date(date, dates):
                return dates.iloc[np.argmin(np.abs(dates - date))]
        
            # Find closest reportDate for each datekey
            full_fin_history['closest_report_date'] = full_fin_history.index.map(
                lambda x: find_closest_date(x, transcript_data['reportDate'])
            )
        
            # Merge with transcript data
            result = pd.merge_asof(
                full_fin_history.reset_index(),
                transcript_data,
                left_on='closest_report_date',
                right_on='reportDate',
                direction='nearest'
            )
        
            # Clean up and set index back to datekey
            result = result.set_index('datekey')
        
            return result
        
        # Example usage:
        joined_data = join_financial_and_transcript_data(full_fin_history, full_transcript_history)
        return joined_data
    def load_sharadar_ticker_table(self):
        sharadar_ticker_table = pd.read_sql('sharadar__tickers',self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp'))
        #sharadar_ticker_table['sector'].unique()
        shar_mapping  = {'Healthcare':'fcf', 'Basic Materials':'fcf', 'Financial Services':'netinc',
               'Industrials':'fcf', 'Communication Services':'fcf', 'Consumer Defensive':'fcf',
               'Technology':'fcf', 'Real Estate':'fcf', 'Consumer Cyclical':'fcf', 'Energy':'fcf',
               'Utilities':'netinc'}
        sharadar_ticker_table['income_type'] =sharadar_ticker_table['sector'].map(shar_mapping)
        return sharadar_ticker_table

    def check_initial_ticker(self):
        ticker_to_work = self.google_sheet_manager.load_google_sheet_as_df(workbook='manyasone', 
                                                          worksheet='ticker_audit').columns[1]
        return ticker_to_work

    def run_sheet_refresh_if_stale(self):
        ticker_to_work = self.check_initial_ticker()
        runner = self.initial_ticker == ticker_to_work
        if runner == True:
            print('No change in ticker')
        if runner == False:
            xdf = self.output_ticker_joined_df(ticker=ticker_to_work)
            income_type = self.sharadar_dexed['income_type'].loc[ticker_to_work]
            tail_df = xdf[['closest_report_date','url',f'{income_type}__1',f'{income_type}__2',f'{income_type}__3',f'{income_type}__4']].tail(12)
            self.google_sheet_manager.clear_worksheet(workbook='manyasone', worksheet='ticker_feeder')
            #tail_df
            self.google_sheet_manager.write_dataframe_to_sheet(workbook='manyasone', worksheet='ticker_feeder', df_to_write=tail_df)
            self.initial_ticker= ticker_to_work

    def run_continuously(self, interval=60):
        """
        Runs the sheet refresh process continuously with a given interval.

        Args:
            interval (int): Time in seconds to wait between checks.
        """
        while True:
            try:
                self.run_sheet_refresh_if_stale()
            except Exception as e:
                print(f"An error occurred: {e}")
            time.sleep(interval)
