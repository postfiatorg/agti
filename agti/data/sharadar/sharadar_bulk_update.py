from utilities.settings import CredentialManager
from utilities.db_manager import DBConnectionManager
import quandl
import sys
import json
import zipfile
import io
import pandas as pd
import datetime
import sqlalchemy
import os

class SharadarDataUpdate:
    def __init__(self, pw_map, user_name):
        self.pw_map = pw_map
        self.quandl_api_key = self.pw_map['quandl']
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()
        self.user_name = user_name
    def bulk_fetch_quandl_table(self, table_to_load):
        """ Gets bulk file and outputs """
        table_to_load = table_to_load.lower()
        quandl.ApiConfig.api_key = self.pw_map['quandl']
        api_key = self.pw_map['quandl']

        url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.json?qopts.export=true&api_key=%s' % (table_to_load, api_key)

        def bulk_fetch(url=url):
            version = sys.version.split(' ')[0]
            if version < '3':
                import urllib2
                fn = urllib2.urlopen
            else:
                import urllib
            fn = urllib.request.urlopen

            valid = ['fresh', 'regenerating']
            invalid = ['generating']
            status = ''

            while status not in valid:
                Dict = json.loads(fn(url).read())
                status = Dict['datatable_bulk_download']['file']['status']
                link = Dict['datatable_bulk_download']['file']['link']
                print(status)
            if status not in valid:
                time.sleep(60)

            zipString = fn(link).read()
            print('fetched')
            return zipString

        op = bulk_fetch()
        return op
        
    def output_sharadar_table_raw_df(self,table_to_load="TICKERS"):
        
        bulk_fetch = self.bulk_fetch_quandl_table(table_to_load=table_to_load)
        
        # Unzip and read the CSV content
        with zipfile.ZipFile(io.BytesIO(bulk_fetch)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f)      
        return df

    def check_if_table_exists(self,table_to_load = 'sf1'):
        
        standardized_name = f'sharadar__{table_to_load}'
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        table_exists = standardized_name in sqlalchemy.inspect(dbconnx).get_table_names()
        dbconnx.dispose()
        return table_exists
    def check_how_stale_table_is(self, table_to_load):
        standardized_name = f'sharadar__{table_to_load}'
        seconds_stale = 1_000_000_000
        try:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
            sql_query = f"""SELECT MAX(update_date) AS most_recent_date FROM {standardized_name};"""
            sql_output = pd.read_sql(sql_query, dbconnx)
            seconds_stale = (datetime.datetime.now()-list(sql_output['most_recent_date'])[0]).total_seconds()
        except:
            pass
        minutes_stale = seconds_stale/60
        hours_stale =  minutes_stale/60
        days_stale = hours_stale /24
        op_map = {'seconds_stale': seconds_stale,
                  'minutes_stale': minutes_stale,
                  'hours_stale': hours_stale,
                  'days_stale': days_stale}
                  
        return op_map
## step 1 - quick and dirty, uploads ticker table 
    def force_update_sharadar_tickers_table(self):
        table_to_load='tickers'
        raw_table_df = self.output_sharadar_table_raw_df(table_to_load=table_to_load)
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        standardized_name = f'sharadar__{table_to_load}'
        raw_table_df['update_date']=datetime.datetime.now()
        raw_table_df.to_sql(standardized_name, dbconnx, if_exists='replace')
        dbconnx.dispose()
        
    def update_sharadar_tickers_table_if_stales(self):
        staleness = self.check_how_stale_table_is(table_to_load='tickers')

        if staleness['days_stale'] <1:
            print('Sharadar Ticker Table is up to date')
        if staleness['days_stale']>1:
            self.force_update_sharadar_tickers_table()

    def kick_off_sharadar_table_bulk_load(self,table_to_load = 'sep'):
        """ This is the full postgres load""" 
        table_to_load= table_to_load.lower()
        datadump_path = str(self.credential_manager.datadump_directory_path)
        download_dir = f'{datadump_path}/data/sharadar/{table_to_load}'
        destination_file_ref =f'{download_dir}/{table_to_load}_download.csv.zip' 
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            print('made directories')
        zipString = self.bulk_fetch_quandl_table(table_to_load=table_to_load)
        f = open(destination_file_ref, 'wb')
        f.write(zipString)
        f.close()
        print(f'wrote {table_to_load} to {destination_file_ref}')
        zip_file_ref = destination_file_ref
        zfolder=zipfile.ZipFile(zip_file_ref)
        zip_file=zfolder.filelist[0]
        extracted_file_rename = f'{download_dir}/{table_to_load}_raw.csv'
        extracted_file_name = zfolder.extract(zip_file,download_dir)
        os.replace(extracted_file_name,extracted_file_rename)
        raw_csv_destination = f'/tmp/postgresload/sharadar__{table_to_load}_raw.csv'
        # Extract the directory portion of the path
        directory = os.path.dirname(raw_csv_destination)
        # Ensure the directory exists
        if not os.path.exists(directory):
            os.makedirs(directory)
        if os.name != 'nt':
            shutil.copyfile(extracted_file_rename, raw_csv_destination)
            print('wrote raw csvs to TMP that have been cleaned for postgres loading')
        standardized_name = f'sharadar__{table_to_load}'
        raw_db_init= pd.read_csv(raw_csv_destination, nrows=200)
        #raw_db_init['update_date']=datetime.datetime.now()
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        raw_db_init.to_sql(standardized_name, 
        con=dbconnx, if_exists='replace',index=False)
        print('wrote the initiation postgres tables')
        
        conn = self.db_connection_manager.spawn_psycopg2_db_connection(user_name=self.user_name)
        cur = conn.cursor()
        cur.execute(f"COPY {standardized_name} FROM '{raw_csv_destination}' DELIMITER ',' CSV HEADER;")
        conn.commit()
        cur.close()
        conn.close()
        print(f'wrote {standardized_name} to postgres')

    def get_sharadar_recent_update_time(self,table_name = 'sep'):
        file_path = f'{self.datadump_directory_path}/data/sharadar/{table_name}/{table_name}_download.csv.zip'
        recent_update = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        return recent_update

    def update_all_sharadar_data(self):
        for xtable in ['sep','daily','sf1','sf3','tickers']:
            self.kick_off_sharadar_table_bulk_load(table_to_load=xtable)
        return 
    
    def output_most_recent_update_for_sharadar_table(self,table_name = 'daily'):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER () as rn
            FROM sharadar__{table_name}
        ) a
        WHERE rn = (SELECT COUNT(*) FROM sharadar__daily);
        """
        
        last_update_df=  pd.read_sql_query(query, dbconnx)
        return last_update_df
