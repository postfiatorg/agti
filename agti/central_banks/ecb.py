
import os
import re
import socket
import time
import warnings
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








class ECBBankScrapper:
    """
    We decided to not convert timestamp from CET to EST, becasue ECB provides just date without time.
    and the date will be the same in both timezones.
    """
    INITIAL_YEAR = 1998
    COUNTRY_CODE_ALPHA_3 = "EUE"
    COUNTRY_NAME = "European Union"

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
    
    def get_all_dates_in_db_for_year(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT date_created 
FROM {} 
WHERE country_code_alpha_3 = :country_code_alpha_3
""".format(self.table_name))
        params = {
            "country_code_alpha_3": ECBBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
    

    def pageBottom(self):
        bottom=False
        a=0
        while not bottom:
            new_height = self._driver.execute_script("return document.body.scrollHeight")
            self._driver.execute_script(f"window.scrollTo(0, {a});")
            if a > new_height:
                bottom=True
            time.sleep(0.001)
            a+=5


    def parse_html(self, href: str):
        self._driver.get(href)
        # select all text from dev with class section
        element = self._driver.find_element(By.XPATH, "//main/div[contains(@class, 'section')]")
        text = element.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        return text



    def process_all_years(self):
        dates_scraped = self.get_all_dates_in_db_for_year()
        pd_dates = [pd.to_datetime(date) for date in dates_scraped]

        self._driver.get(self.get_base_url_for_year())
        # scroll to the bottom of the page
        self.pageBottom()
        
        to_process = {}
        
        # select dl by id lazyload-container
        dl = self._driver.find_element(By.ID, "lazyload-container")
        # itarete over all divs inside dl
        for div in dl.find_elements(By.XPATH, ".//div[@data-index]"):
            # find all sub divs with data-index attribute
            data_index = div.get_attribute("data-index")
            print("Processing data-index:", data_index)
            # find dt with isodate attribute
            elements = div.find_elements(By.XPATH, "./*")
            dts = elements[::2]
            dds = elements[1::2]
            assert len(dts) == len(dds), "Number of dt and dd elements is not equal"
            for dt, dd in zip(dts, dds):
                isodate = dt.get_attribute("isodate")
                pd_isodate = pd.to_datetime(isodate)
                if pd_isodate in pd_dates:
                    print("Skipping date:", isodate)
                    continue 
                print("isodate:", isodate)
                a_element = dd.find_element(By.XPATH, "./div[@class='ecb-langSelector']/span/a")
                lang = a_element.get_attribute("lang")
                href = a_element.get_attribute("href")
                if lang != "en":
                    warnings.warn(f"Language is not English: {lang} for date {isodate}")

                to_process[isodate] = {
                    "file_url": href,
                }


        for date, data in to_process.items():
            print("Processing date:", date)
            text = self.parse_html(data["file_url"])
            to_process[date]["full_extracted_text"] = text

        df = pd.DataFrame(to_process).T.reset_index(names=["date_created"])
        df["country_code_alpha_3"] = ECBBankScrapper.COUNTRY_CODE_ALPHA_3
        df["country_name"] = ECBBankScrapper.COUNTRY_NAME

        ipaddr, hostname = self.ip_hostname()
        df["scraping_ip"] = ipaddr
        df["scraping_machine"] = hostname


        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)
    

    def __del__(self):
        self._driver.close()
    

    def get_base_url_for_year(self) -> str:
        return f"https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html"