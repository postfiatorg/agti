from selenium import webdriver
# changing to be native to whatever windows address we are using 
from webdriver_manager.firefox import GeckoDriverManager
import os
import datetime
from agti.utilities.settings import CredentialManager
import pandas as pd
#from basic_utilities.dropbox.dropbox_connector import functions as dropbox_finder
from agti.utilities.settings import *
class ScrapingFileManager:
    def __init__(self):
        print('initiating ScrapingFileManager')
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.datadump_directory_path

    def format_item_file_to_write(self,item_str,storage_dir,file_dir,file_extension):
        todays_date_string=datetime.datetime.today().strftime('%Y-%m-%d')
        format_file_to_write = ('%s/%s/%s/%s___%s.%s') % (self.datadump_directory_path,storage_dir,file_dir,item_str, 
        todays_date_string, file_extension)
        return format_file_to_write

    def get_most_recent_file_for_item_in_dir(self, item_str, storage_dir, file_dir):
        data_directory='%s/%s/%s/' %(self.datadump_directory_path,storage_dir,file_dir)
        #data_directory='/home/ubuntu/dataRepository/marketdata/quarterlyFundamentals/'
        files_to_eval=[i for i in os.listdir(data_directory)]
        files_to_eval=[stringer for stringer in files_to_eval if (stringer.split('___')[0]==item_str)]

        my_list = [datetime.datetime.strptime(i.split('___')[1].split('.')[0],'%Y-%m-%d') for i in files_to_eval]
        max_value = max(my_list)
        max_index = my_list.index(max_value)
        most_recent_file=data_directory+files_to_eval[max_index]
        return most_recent_file

    def determine_how_out_of_date_item_file_is(self, item_str,storage_dir,file_dir):
        days_stale = 10000
        try:
            date_updated_as_of=self.get_most_recent_file_for_item_in_dir(item_str=item_str,
            storage_dir=storage_dir,file_dir=file_dir).split('___')[1].split('.')[0]
            date_updated_datetime=datetime.datetime.strptime(date_updated_as_of,'%Y-%m-%d')
            days_stale=(datetime.datetime.now()-date_updated_datetime).days
        except:
            pass
        return days_stale

    def write_file_if_x_days_stale(self,contents,item_str,storage_dir,file_dir,file_extension,days_stale_max):
        days_stale= self.determine_how_out_of_date_item_file_is(item_str=item_str, 
        storage_dir=storage_dir, file_dir=file_dir)
        file_name_to_write= self.format_item_file_to_write(item_str=item_str, 
        file_dir=file_dir, storage_dir=storage_dir, file_extension=file_extension)
        if days_stale > days_stale_max:
            if file_extension == 'csv':
                contents.to_csv(file_name_to_write)
            if file_extension == 'pkl':
                contents.to_pickle(file_name_to_write)
            if file_extension == 'html':
                f = open(file_name_to_write, "w")
                f.write(contents)
                f.close()


class BasicScrapingTool:
    ''' I am jacks unwillingness to properly use selenium'''
    def __init__(self,pw_map):
        self.pw_map = pw_map
        self.scraping_file_manager = ScrapingFileManager()
        if 'firefox_profile' in self.pw_map:
            xd = self.pw_map['firefox_profile']
            fp = webdriver.FirefoxProfile(xd)
            self.driver = webdriver.Firefox(firefox_profile=fp,
                executable_path=GeckoDriverManager().install())
        if 'firefox_profile' not in self.pw_map:
            print('no firefox profile found, using default')
            self.driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())
    def get(self,url):
        self.driver.get(url)