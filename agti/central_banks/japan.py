
import os
import re
import socket
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import urllib
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pdfplumber
from sqlalchemy import text








class JapanBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "JPN"
    COUNTRY_NAME = "Japan"
    INITIAL_YEAR = 1998

    def __init__(self, pw_map, user_name, table_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()
        self.table_name = table_name

        self._driver = self._setup_driver()

    def ip_hostname(self):
        hostname = socket.gethostname()
        IPAddr = socket.gethostbyname(hostname)
        return IPAddr, hostname


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
    
    def get_all_db_urls(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT file_url 
FROM {}
WHERE country_code_alpha_3 = :country_code_alpha_3
""".format(self.table_name))
        params = {
            "country_code_alpha_3": JapanBankScrapper.COUNTRY_CODE_ALPHA_3,
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
    

    def process_year(self, year: int):

        all_urls = self.get_all_db_urls()
        
        self._driver.get(self.get_base_url_for_year(year))
        table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl']")
        #caption = table.find_element(By.XPATH, ".//caption").text
        tbody = table.find_element(By.XPATH, ".//tbody")
        to_process = []
        for row in tbody.find_elements(By.XPATH,".//tr"):
            tds = list(row.find_elements(By.XPATH,".//td"))
            date = pd.to_datetime(tds[0].text)
            link = tds[1].find_element(By.XPATH, ".//a")
            # parse link, get href and text
            href = link.get_attribute("href")
            if href in all_urls:
                print(f"Already processed: {href}")
                continue

            # drop [PDF xxKB] from link text
            #link_text = link.text
            # using regex
            #link_text = re.sub(r"\[PDF (\d+,)*\d+KB\]", "", link.text)

            to_process.append((date, href))


        result = []
        for date, href in to_process:
            if href.endswith("pdf"):
                print("Downloading file:", href)
                text = self.download_and_read_pdf(href)
            elif href.endswith("htm"):
                print("Parsing HTML file:", href)
                text = self.read_html(href)
            else:
                raise ValueError("Unknown file format")
            
            result.append({
                "file_url": href,
                "full_extracted_text": text,
                "date_published": date,
            })

        df = pd.DataFrame(result)
        # if empty skip
        if df.empty:
            print(f"No new data found for year: {year}")
            return
        
        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = JapanBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = JapanBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)



    def process_all_years(self):
        this_year = pd.Timestamp.now().year
        for year in range(JapanBankScrapper.INITIAL_YEAR, this_year + 1):
            self.process_year(year)
    

    def __del__(self):
        self._driver.close()

    
    def read_html(self, url: str):
        self._driver.get(url)
        element = self._driver.find_element(By.CSS_SELECTOR, "div.outline.mod_outer")
        text = element.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        return text
    






    def get_base_url_for_year(self, year: int) -> str:
        return f"https://www.boj.or.jp/en/mopo/mpmdeci/mpr_{year}/index.htm"