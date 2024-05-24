import re
import pandas as pd
from data.sec_methods.request_utility import SECRequestUtility
from utilities.db_manager import DBConnectionManager
import datetime

class SECRecentDataBatchLoad:
    def __init__(self, pw_map, user_name):
        self.pw_map = pw_map
        self.user_name = user_name if user_name else pw_map['node_name']
        self.sec_request_utility = SECRequestUtility(pw_map=self.pw_map)
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.cache = None

    def output_text_for_sec_recent_data_given_start(self, start_number):
        """Fetches the raw text of the recent updates SEC page starting from the given number."""
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&datea=&dateb=&company=&type=8-k&SIC=&State=&Country=&CIK=&owner=include&accno=&start={start_number}&count=100"
        xhtml = self.sec_request_utility.compliant_request(url)
        print('Queried URL:', url)
        return xhtml.text

    def output_recent_sec_updates(self):
        """Gets the recent filings pages and returns a DataFrame of them."""
        df_arr = []
        start_number = 0
        
        while True:
            print('Fetching data from start number:', start_number)
            text_block = self.output_text_for_sec_recent_data_given_start(start_number)

            if self.cache is not None and text_block == self.cache:
                print("No new updates found, stopping the loop.")
                break

            self.cache = text_block

            # Define regex patterns
            cik_pattern = r'CIK=(\d+)'
            date_pattern = r'nowrap="nowrap">([\d-]+)<br>'
            url_pattern = r'<a href="(/Archives/edgar/data/\d+/[\d\w-]+/[\d\w-]+\.txt)">\[text\]</a>'
            html_pattern = r'<a href="(/Archives/edgar/data/\d+/\d+/[^"]+-index\.htm)">\[html\]</a>'
            company_name_pattern = r'action=getcompany&amp;CIK=(\d+)&amp;owner=include&amp;count=100">([^<]+) \(Filer\)</a>'
            report_doc_pattern = r'<td class="small">([\s\S]*?)<\/td>'
            acceptance_date_time_pattern = r'<td nowrap="nowrap">([\d-]+)<br>([\d:]+)</td>'

            # Find matches
            cik_matches = re.findall(cik_pattern, text_block)
            date_matches = re.findall(date_pattern, text_block)
            url_matches = re.findall(url_pattern, text_block)
            html_url_matches = re.findall(html_pattern, text_block)
            company_name_matches = re.findall(company_name_pattern, text_block)
            report_doc_matches = re.findall(report_doc_pattern, text_block)
            acceptance_date_time_matches = re.findall(acceptance_date_time_pattern, text_block)

            # Create DataFrame
            df = pd.DataFrame({
                'CIK': cik_matches,
                'Date': date_matches,
                'Acceptance DateTime': acceptance_date_time_matches,
                'URL': url_matches,
                'html_url': html_url_matches,
                'Company Name': company_name_matches,
                'Document': report_doc_matches,
            })

            # Data Cleaning
            df['Date'] = pd.to_datetime(df['Date'])
            df['html_url'] = 'https://www.sec.gov' + df['html_url'].astype(str)

            # Append the DataFrame to the list
            df_arr.append(df)

            start_number += 100

        # Concatenate all DataFrames
        recent_hist = pd.concat(df_arr)
        recent_hist['URL'] = 'https://www.sec.gov' + recent_hist['URL'].apply(lambda x: str(x))
        
        recent_sec_updates = recent_hist.copy()
        recent_sec_updates['full_datetime'] = pd.to_datetime(recent_sec_updates['Acceptance DateTime'].apply(lambda x: ' '.join(x)))
        recent_sec_updates['raw_date'] = pd.to_datetime(recent_sec_updates['Date'])
        recent_sec_updates['text_url'] = recent_sec_updates['URL']
        recent_sec_updates['html_url'] = recent_sec_updates['html_url']
        recent_sec_updates['simple_name'] = recent_sec_updates['Company Name'].apply(lambda x: x[1].split('(')[0])
        recent_sec_updates['cik'] = recent_sec_updates['CIK']
        recent_sec_updates['items_string'] = recent_sec_updates['Document'].apply(lambda x: x.split('item')[1].split('\n')[0].replace('s ',''))
        recent_sec_updates['is_eps'] = recent_sec_updates['items_string'].apply(lambda x: ('9.' in x) & ('2.' in x))
        
        return recent_sec_updates

    def write_recent_sec_updates(self):
        """Writes recent SEC updates to the database and returns new records."""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        
        # Fetch existing records from the database
        try:
            existing_updates = pd.read_sql('SELECT * FROM sec__update_recent_filings', dbconnx)
            print("Fetched existing records from the database.")
        except Exception as e:
            if "relation \"sec__update_recent_filings\" does not exist" in str(e):
                print("Table sec__update_recent_filings does not exist. Creating a new table.")
                existing_updates = pd.DataFrame()
            else:
                raise

        recent_sec_updates = self.output_recent_sec_updates()
        
        if not existing_updates.empty:
            # Identify new records
            new_records = recent_sec_updates[~recent_sec_updates['html_url'].isin(existing_updates['html_url'])]
            print(f"Identified {len(new_records)} new records.")
        else:
            new_records = recent_sec_updates
            print("No existing records found. All fetched records are new.")

        # Write new records to the database
        if not new_records.empty:
            new_records.to_sql('sec__update_recent_filings', dbconnx, if_exists='append')
            print(f"Wrote {len(new_records)} new records to the database.")
        else:
            print("No new records to write to the database.")

        dbconnx.dispose()
        return new_records
