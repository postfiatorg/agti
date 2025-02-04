
import os
import re
import socket
import time
import pandas as pd
import requests
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


class SwedenBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "SWE"
    COUNTRY_NAME = "Sweden"

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
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0"
        }
        try:
            r = requests.get(url, headers=headers)

            with open(self.datadump_directory_path / filename, 'wb') as outfile:
                outfile.write(r.content)
        
            with pdfplumber.open(self.datadump_directory_path / filename) as pdf:
                text = ""
                for page in pdf.pages:
                    text += page.extract_text().replace('\x00','')
        except Exception as e:
            print("Error processing pdf from: ", url)
            print("Error: ", e)
            return ""

        os.remove(self.datadump_directory_path / filename)

        return text
    
    def get_all_dates(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT date_published 
FROM {} 
WHERE country_code_alpha_3 = :country_code_alpha_3
""".format(self.table_name))
        params = {
            "country_code_alpha_3": SwedenBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
    

    def __del__(self):
        self._driver.close()


    def process_year(self, year: int):

        all_dates = self.get_all_dates()

        self._driver.get(self.get_base_url_for_year(year))
        # get using xpath div with class="listing-block__body"
        div = self._driver.find_element(By.XPATH, "//div[@class='listing-block__body']")
        # get all a tags
        a_tags = div.find_elements(By.TAG_NAME, "a")
        to_process = []

        for a in a_tags:
            # get span with class="label"
            span = a.find_element(By.TAG_NAME, 'span')# we take the first span
            date = pd.to_datetime(span.text, dayfirst=True)
            to_process.append((date, a.get_attribute("href")))
        output = []
        for date, href in to_process:
            print("Processing: ", date)
            if date in all_dates:
                print("Data already exists for: ", date)
                continue
            self._driver.get(href)
            pdf_href = None
            # find all a tags with class="button button--iconed"
            a_tags = self._driver.find_elements(By.XPATH, "//a[@class='button button--iconed']")
            if len(a_tags) == 0:
                # find h2 with text "Documents"
                h2 = self._driver.find_element(By.XPATH, "//h2[normalize-space(text())='Documents']")
                # get grandparent div
                div = h2.find_element(By.XPATH, "//ancestor::div[@class='linklist-block']")
                
                # find all a tags
                a_tags = div.find_elements(By.TAG_NAME, "a")
                # filter wil text "Monetary Policy Report"
                a_tags = [a for a in a_tags if "Monetary Policy Report".lower() in a.text.lower()]
            for a in a_tags:
                temp_href = a.get_attribute("href")
                if temp_href.endswith(".pdf"):
                    pdf_href = temp_href
                    break
            print("PDF: ", pdf_href)
            output.append({
                "date_published": date,
                "file_url": href,
                "full_extracted_text": self.download_and_read_pdf(pdf_href) if pdf_href else "",
            })


        df = pd.DataFrame(output)
        if df.empty:
            print("No new data found")
            return
        
        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = SwedenBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = SwedenBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)

            


    def process_all_years(self):
        # process new years
        current_year = pd.Timestamp.now().year
        for year in range(2017, current_year + 1):
            self.process_year(year)

        # process archive
        self.process_archive()


    def process_archive(self):
        all_dates = self.get_all_dates()
        self._driver.get(self.get_archive_url())
        # get table tag
        table = self._driver.find_element(By.TAG_NAME, "table")
        # get all tr tags
        trs = list(table.find_elements(By.TAG_NAME, "tr"))
        header_row = trs.pop(0)
        # verify should ahave th Date and th Header
        ths = header_row.find_elements(By.TAG_NAME, "th")
        if len(ths) != 2:
            raise ValueError("Header row does not have 2 columns")
        if ths[0].text.strip() != "Date":
            raise ValueError("First column should be Date")
        if ths[1].text.strip() != "Header":
            raise ValueError("Second column should be Header")
        

        to_process = []
        for tr in trs:
            href = None
            tag_time = tr.find_element(By.TAG_NAME, "time")
            date = pd.to_datetime(tag_time.text, dayfirst=True)
            # NOTE this is not the best because we have multiple documents for the same date
            if date in all_dates:
                print("Data already exists for: ", date)
                continue
            a = tr.find_element(By.TAG_NAME, "a")
            href = a.get_attribute("href")
            to_process.append((date, href))

        output = []
        for date, href in to_process:
            text = ""
            print("Processing: ", date)
            if href.endswith(".pdf"):
                text = self.download_and_read_pdf(href)
            output.append({
                "date_published": date,
                "file_url": href,
                "full_extracted_text": text,
            })

        df = pd.DataFrame(output)
        if df.empty:
            print("No new data found")
            return
        
        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = SwedenBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = SwedenBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)
                



    def get_archive_url(self):
        return "https://archive.riksbank.se/en/Web-archive/Published/Published-from-the-Riksbank/Monetary-policy/Monetary-Policy-Report/index.html@all=1.html"

    def get_base_url_for_year(self, year:int) -> str:
        if year < 2017:
            raise ValueError("No data available for year before 2017")
        return f"https://www.riksbank.se/en-gb/monetary-policy/monetary-policy-report/?year={year}"