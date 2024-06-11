from agti.utilities.settings import CredentialManager

import pandas as pd
import os
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
## The default is to have two types of Google Sheet managers
## one for public facing or shared workflows and one for IP protected
## or team only workflows 

import os

class GoogleSheetManager:
    def __init__(self, prod_trading=False):
        self.credential_manager = CredentialManager()
        self.credentials_directory = self.credential_manager.get_credentials_directory()
        self.check_and_prompt_for_credentials()
        self.file_path_of_gsheets = self.get_credential_file_path(prod_trading)
        self.gspread_tool = self.authorize_gspread()

    def check_and_prompt_for_credentials(self):
        required_files = ['prod_trading_google_creds.json', 'public_facing_google_creds.json']
        for file in required_files:
            if not os.path.isfile(os.path.join(self.credentials_directory, file)):
                print(f'Please create the JSON credential file: {file} in the directory: {self.credentials_directory}')

    def get_credential_file_path(self, prod_trading):
        cred_file_path = self.credentials_directory
        if prod_trading:
            file_path_of_gsheets = os.path.join(cred_file_path, 'prod_trading_google_creds.json')
        else:
            file_path_of_gsheets = os.path.join(cred_file_path, 'public_facing_google_creds.json')
        
        if os.path.isfile(file_path_of_gsheets):
            print(f'Loaded gsheet cred file from {file_path_of_gsheets}')
        else:
            print(f'Load the gsheet cred file to {file_path_of_gsheets}')
        
        return file_path_of_gsheets

    def authorize_gspread(self):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.file_path_of_gsheets, scope)
        return gspread.authorize(credentials)

    def load_google_sheet_as_df(self, workbook='odv', worksheet='crm'):
        '''Outputs a worksheet from a workbook as a dataframe'''
        gc = self.gspread_tool
        workbook_to_load = gc.open(workbook)
        worksheet_to_work = workbook_to_load.worksheet(worksheet)
        temp_df = pd.DataFrame(worksheet_to_work.get_all_values())
        column_headers = list(temp_df.head(1).loc[0])
        temp_df = temp_df[1:]
        temp_df.columns = column_headers
        return temp_df

    def write_dataframe_to_sheet(self, workbook, worksheet, df_to_write):
        '''Outputs a worksheet from a workbook as a dataframe'''
        gc = self.gspread_tool
        sh = gc.open(workbook)
        worksheet = sh.worksheet(worksheet)
        range_of_cells = worksheet.range('A2:C1000')  # Select the range you want to clear
        for cell in range_of_cells:
            cell.value = ''
        worksheet.update_cells(range_of_cells)

        # Append data to sheet
        set_with_dataframe(worksheet, df_to_write)

    def create_worksheet_if_does_not_exist(self, workbook_name, worksheet_name):
        wb = self.gspread_tool.open(workbook_name)
        all_sheets = [i.title for i in wb.worksheets()]
        if worksheet_name not in all_sheets:
            wb.add_worksheet(worksheet_name, rows=1000, cols=100)

    def clear_worksheet(self, workbook, worksheet):
        wb = self.gspread_tool.open(workbook)
        ws = wb.worksheet(worksheet)
        ws.clear()
