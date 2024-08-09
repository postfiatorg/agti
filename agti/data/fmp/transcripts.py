
from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
import pandas as pd
import requests 
import datetime 
class FMPDataTool:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.db_connection_manager= DBConnectionManager(pw_map=self.pw_map)
        self.fmp_api_key=self.pw_map['financialmodelingprep']
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
        url = f'https://financialmodelingprep.com/api/v4/earning_call_transcript?symbol={ticker}&apikey={self.fmp_api_key}'
        response = requests.get(url)
        op_json= response.json()
        earnings_df = pd.DataFrame(op_json)
        earnings_df.columns=['quarter','year','upload_time']
        earnings_df['upload_date']=earnings_df['upload_time'].apply(lambda x: str(x).split(' ')[0])
        earnings_df['ticker'] = ticker
        earnings_df['transcript_code'] = earnings_df['ticker'] + '__earnings_call_transcript__' + earnings_df['upload_date']+'__'+earnings_df['quarter'].astype(str)
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

    def get_earnings_call_transcript(self,ticker,quarter,year):
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

    def output_information_dataframe_for_transcript_code(self, transcript_code):
        """ 
        Example:
        transcript_code = 'AMZN__earnings_call_transcript__2024-04-30__1'
        """ 
        ticker = transcript_code.split('__')[0]
        date = transcript_code.split('transcript__')[1].split('__')[0]
        quarter = transcript_code.split('__')[-1:][0]
        year = date[0:4]
        xtranscript_map = self.get_earnings_call_transcript(ticker=ticker, quarter=quarter, year=year)
        simplified_df = pd.DataFrame(xtranscript_map)
        simplified_df['simple_date'] = pd.to_datetime(date)
        simplified_df['transcript_code'] = transcript_code
        return simplified_df

    def get_all_loaded_transcript_codes(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        loaded_transcripts = list(pd.read_sql('select transcript_code from fmp___earnings_call_transcripts;', 
                                              dbconnx)['transcript_code'].unique())
        return loaded_transcripts

    def write_full_fmp_history_for_x_years(self,years_to_work=6):
        start_date = datetime.datetime.now()-datetime.timedelta(365*years_to_work)
        # the following generates a dataframe of transcript codes that need to be updated
        ev_history = self.generate_transcript_df_for_minimum_ev(min_ev_to_consider=250_000_000)
        ev_history['upload_date']= pd.to_datetime(ev_history['upload_date'])
        full_history_of_transcripts = ev_history[ev_history['upload_date'] > start_date].sort_values('upload_date')
        full_list_of_transcript_codes = list(full_history_of_transcripts['transcript_code'].unique())
        all_codes_loaded = self.get_all_loaded_transcript_codes()
        total_codes_loaded = len(all_codes_loaded)
        print(f"Total Codes Loaded: {total_codes_loaded}")
        all_codes_to_load =[i for i in full_list_of_transcript_codes if i not in all_codes_loaded]
        for xcode in all_codes_to_load:
            try:
                ydfx = self.output_information_dataframe_for_transcript_code(transcript_code=xcode)
                dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
                ydfx.to_sql('fmp___earnings_call_transcripts', dbconnx,if_exists='append')
                dbconnx.dispose()
            except:
                print(f'Failed {xcode}')
                pass

    def write_full_fmp_history_for_tickers(self, tickers, years_to_work=6):
        """
        Write full FMP history for a list of tickers for a specified number of years.

        Args:
        self: The FMPDataTool instance
        tickers (list): List of ticker symbols
        years_to_work (int): Number of years of history to retrieve (default: 6)
        Returns:
        None
        """
        start_date = datetime.datetime.now() - datetime.timedelta(365 * years_to_work)
        all_codes_loaded = self.get_all_loaded_transcript_codes()
        for ticker in tickers:
            try:
                ev_history = self.get_earnings_call_transcript_list(ticker)
                ev_history['upload_date'] = pd.to_datetime(ev_history['upload_date'])
                full_history_of_transcripts = ev_history[ev_history['upload_date'] > start_date].sort_values('upload_date')
                full_list_of_transcript_codes = list(full_history_of_transcripts['transcript_code'].unique())
                codes_to_load = [code for code in full_list_of_transcript_codes if code not in all_codes_loaded]
                for xcode in codes_to_load:
                    try:
                        ydfx = self.output_information_dataframe_for_transcript_code(transcript_code=xcode)
                        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
                        ydfx.to_sql('fmp___earnings_call_transcripts', dbconnx, if_exists='append', index=False)
                        dbconnx.dispose()
                        all_codes_loaded.append(xcode)  # Update the list of loaded codes
                    except Exception as e:
                        print(f'Failed to load {xcode}: {str(e)}')
                print(f"Completed processing for {ticker}")
            except Exception as e:
                print(f"Failed to process {ticker}: {str(e)}")

    def get_loaded_transcripts_for_tickers(self, tickers, force_update=False):
        """
        Retrieve all transcripts for a list of tickers from the fmp___earnings_call_transcripts database.
        Args:
        tickers (list): List of ticker symbols
        db_connection_manager (DBConnectionManager): Instance of DBConnectionManager
        Returns:
        pandas.DataFrame: DataFrame containing all transcripts for the specified tickers
        """
        if force_update == True:
            self.write_full_fmp_history_for_tickers(tickers=tickers, years_to_work=6)
        # Create a database connection
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name='spm_typhus')
        # Construct the SQL query
        tickers_str = "', '".join(tickers)
        query = f"""
        SELECT *
        FROM fmp___earnings_call_transcripts
        WHERE symbol IN ('{tickers_str}')
        ORDER BY symbol, date DESC
        """
        # Execute the query and return the results as a DataFrame
        transcripts_df = pd.read_sql(query, dbconnx)
        # Close the database connection
        dbconnx.dispose()
        return transcripts_df