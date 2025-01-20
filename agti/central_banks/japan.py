
import os
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import urllib
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
import pdfplumber
from sqlalchemy import text








class JapanBankScrapper:
    initial_year = 1998

    def __init__(self, pw_map, user_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()

        self._driver = self._setup_driver()


    def _setup_driver(self):
        driver = webdriver.Firefox()
        return driver
    
    def download_and_read_pdf(self, url: str) -> str:
        filename = os.path.basename(url)

        urllib.request.urlretrieve(url, self.datadump_directory_path / filename)

        with pdfplumber.open(self.datadump_directory_path / filename) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text()

        os.remove(self.datadump_directory_path / filename)

        return text
    
    def get_all_dates_in_db_for_year(self, year: int):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT date_created 
FROM central_banks 
WHERE date_created >= :start_date 
AND date_created < :end_date
""")
        params = {
            "start_date": f"{year}-01-01",
            "end_date": f"{year + 1}-01-01"
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
        

    def convert_EST_to_JPN(self, date: pd.Timestamp):
        return date + pd.Timedelta(hours=14)
    

    def convert_JPN_to_EST(self, date: pd.Timestamp):
        return date - pd.Timedelta(hours=14)
    

    def process_year(self, year: int):

        dates_scraped = self.get_all_dates_in_db_for_year(year)
        jpn_dates = [self.convert_EST_to_JPN(date) for date in dates_scraped]


        
        self._driver.get(self.get_base_url_for_year(year))
        table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl']")
        #caption = table.find_element(By.XPATH, ".//caption").text
        tbody = table.find_element(By.XPATH, ".//tbody")

        result = []
        for row in tbody.find_elements(By.XPATH,".//tr"):
            tds = list(row.find_elements(By.XPATH,".//td"))
            current_date_JPN = pd.to_datetime(tds[0].text)
            if current_date_JPN in jpn_dates:
                print(f"Skipping row: {current_date_JPN}")
                continue
            link = tds[1].find_element(By.XPATH, ".//a")
            # parse link, get href and text
            href = link.get_attribute("href")

            # drop [PDF xxKB] from link text
            link_text = link.text
            # using regex
            link_text = re.sub(r"\[PDF (\d+,)*\d+KB\]", "", link.text)


            if href.endswith("pdf"):
                print("Downloading file:", link_text)
                try:
                    text = self.download_and_read_pdf(href)
                except:
                    print("Error downloading file:", link_text)
                    continue
            elif href.endswith("htm"):
                print("Parsing HTML file:", link_text)
                continue
                #text = self.read_html(href)
            else:
                raise ValueError("Unknown file format")
            
            result.append({
                "file_url": href,
                "full_extracted_text": text,
                "date_created": current_date_JPN,
            })

        df = pd.DataFrame(result)
        # if empty skip
        if df.empty:
            print(f"No new data found for year: {year}")
            return
        
        df["date_created"] = df["date_created"].apply(self.convert_JPN_to_EST)

        df["country_name"] = "Japan"
        df["country_code_alpha_3"] = "JPN"
        df["scraping_machine"] = "Test1"
        df["scraping_ip"] = "127.0.0.1"

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql("central_banks", con=dbconnx, if_exists="append", index=False)



    def process_all_years(self):
        this_year = pd.Timestamp.now().year
        for year in range(JapanBankScrapper.initial_year, this_year + 1):
            self.process_year(year)
    

    def __del__(self):
        self._driver.close()

    
    def read_html(self, url: str):
        raise NotImplementedError("Method not implemented yet")
    






    def get_base_url_for_year(self, year: int) -> str:
        return f"https://www.boj.or.jp/en/mopo/mpmdeci/mpr_{year}/index.htm"