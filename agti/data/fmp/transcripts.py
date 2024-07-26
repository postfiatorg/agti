
from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
import pandas as pd
import requests 
class FMPDataTool:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.db_connection_manager= DBConnectionManager(pw_map=self.pw_map)
        #self.sharadar_data_tool= SharadarDataTool(pw_map=self.pw_map, force=False)
        self.fmp_api_key=self.pw_map['financialmodelingprep']
        # select from sharadar___daily_raw where the date is in the last week
        #db_query = '# Corrected SQL query
        db_query = """ SELECT * FROM sharadar__daily
        WHERE CAST(date AS date) > CURRENT_DATE - INTERVAL '7 days';
        """ 
        dbconn = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        daily_df = pd.read_sql(db_query, dbconn)
        self.daily_ev_map = daily_df.groupby('ticker').last()['ev']*1_000_000
    def get_symbol_universe(self):
        '''
        This outputs every symbol available in the FMP API but does not neccesarily include things with transcripts
        '''
        #https://financialmodelingprep.com/api/v3/available-traded/list?apikey=120ae1119460285a555b168d85f1e392
        #logging.info("Retrieving symbol universe...")
        url = f'https://financialmodelingprep.com/api/v3/available-traded/list?apikey={self.fmp_api_key}'
        response = requests.get(url)
        return response.json()
    
    def output_full_fmp_universe_with_ev_calcs(self):
        full_fmp_universe = pd.DataFrame(self.get_symbol_universe())
        stocks_only = full_fmp_universe[full_fmp_universe['type']=='stock'].copy()

        stocks_only['daily_enterprise_value']=stocks_only['symbol'].map(self.daily_ev_map)
        all_stocks_with_enterprise_value = stocks_only.dropna()
        return all_stocks_with_enterprise_value
    
    def get_earnings_call_transcript_list(self,ticker):
        '''
        Gets a list of transcripts available for a company
        '''
        #logString = "Retrieving earnings call transcript list for " + ticker
        #logging.info(logString)
        #logString = "fmp api key is: " + self.fmp_api_key
        #logging.info(logString)
        url = f'https://financialmodelingprep.com/api/v4/earning_call_transcript?symbol={ticker}&apikey={self.fmp_api_key}'
        response = requests.get(url)
        op_json= response.json()
        earnings_df = pd.DataFrame(op_json)
        earnings_df.columns=['quarter','year','upload_time']
        earnings_df['upload_date']=earnings_df['upload_time'].apply(lambda x: str(x).split(' ')[0])
        earnings_df['ticker'] = ticker
        earnings_df['transcript_code'] = earnings_df['ticker'] + '__earnings_call_transcript__' + earnings_df['upload_date']
        return earnings_df
    def generate_transcript_df_for_minimum_ev(self, min_ev_to_consider=250_000_000):
        ''' transcript ''' 
        minimum_enterprise_value_for_active_universe = min_ev_to_consider
        ev_calcs = self.output_full_fmp_universe_with_ev_calcs()
        active_df = ev_calcs[ev_calcs['daily_enterprise_value']>minimum_enterprise_value_for_active_universe].copy()
        all_tickers_to_work = list(set(active_df['symbol']))
        yarr=[]
        for ticker in all_tickers_to_work:
            try:
                yarr.append(self.get_earnings_call_transcript_list(ticker))
            except:
                print(ticker)
                pass
        full_transcript_code = pd.concat(yarr)
        return full_transcript_code
    
    def get_earnings_call_transcript(self,ticker, quarter, year):
        '''
        Actually requests the full text of the transcript of a company
        '''
        #logString = "Retrieving earnings call transcript for " + ticker + " quarter is " + str(quarter) + " year is " + str(year)
        #logging.info(logString)
        output =[{'content':''}]
        try:
            url = f'https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}?quarter={quarter}&year={year}&apikey={self.fmp_api_key}'
            response = requests.get(url)
            output = response.json()
        except:
            print('failed to get transcript for ' + ticker + ' quarter is ' + str(quarter) + ' year is ' + str(year))
            pass
        return output
    
    def output_already_loaded_transcript_codes(self):
        transcript_codes=[]
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection(collective=True)
            already_loaded_transcript_df = pd.read_sql('fmp___earnings_call_transcripts', dbconnx)
            transcript_codes = list(already_loaded_transcript_df['transcript_code'])
        except:
            print('transcript codes not loaded')
            pass
        return transcript_codes
    
    def update_fmp_database_with_fresh_info(self):
        all_transcripts_to_work = self.generate_transcript_df_for_minimum_ev(min_ev_to_consider=250_000_000)
        all_transcripts_to_work['upload_time']= pd.to_datetime(all_transcripts_to_work['upload_time'])
        all_transcripts_to_work=all_transcripts_to_work.set_index('transcript_code')

        all_transcript_codes = list(all_transcripts_to_work.index)
        already_loaded_codes = self.output_already_loaded_transcript_codes()


        # Convert already_loaded_codes to a set for faster lookup
        already_loaded_codes_set = set(already_loaded_codes)

        # Efficiently create transcript_codes_to_work using set difference
        transcript_codes_to_work = list(set(all_transcript_codes) - already_loaded_codes_set)

        # Split into chunks of 100
        chunks = [transcript_codes_to_work[x:x+100] for x in range(0, len(transcript_codes_to_work), 100)]
        for transcript_codes_to_work in chunks:

            transcript_block_to_work = all_transcripts_to_work.loc[transcript_codes_to_work].copy()

            transcript_block_to_work['content']=transcript_block_to_work.apply(lambda x: self.get_earnings_call_transcript(x['ticker'],
                                                                    x['quarter'],
                                                                    x['year'])[0]['content'],axis=1)
            transcript_block_to_write = transcript_block_to_work[transcript_block_to_work['content'].apply(lambda x: 
                len(x))>100].copy()
            transcript_block_to_write=transcript_block_to_write.reset_index()
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
            transcript_block_to_write.to_sql('fmp___earnings_call_transcripts', dbconnx, if_exists='append', index=False)
            dbconnx.dispose()
            
    def output_all_historical_fmp_transcripts(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='agti_corp')
        already_loaded_transcript_df = pd.read_sql('fmp___earnings_call_transcripts', dbconnx)
        return already_loaded_transcript_df

