
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


class FEDBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "USA"
    COUNTRY_NAME = "United States of America"

    def __init__(self, pw_map, user_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()

        self._driver = self._setup_driver()

    def ip_hostname(self):
        hostname = socket.gethostname()
        IPAddr = socket.gethostbyname(hostname)
        return IPAddr, hostname

    def _setup_driver(self):
        driver = webdriver.Firefox()
        return driver

    def get_all_dates_in_db_for_year(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(
            user_name=self.user_name)
        query = text("""
SELECT date_created
FROM central_banks
WHERE country_code_alpha_3 = :country_code_alpha_3
""")
        params = {
            "country_code_alpha_3": FEDBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]

    def parse_html(self, href: str):
        self._driver.get(href)
        # get div id article
        div = self._driver.find_element(By.XPATH, "//div[@id='article']")
        text = div.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        return text

    def process_all_years(self):
        dates_scraped = self.get_all_dates_in_db_for_year()
        pd_dates = [pd.to_datetime(date) for date in dates_scraped]

        self._driver.get(self.get_base_url_for_year())

        to_process = {}

        # select dl by id lazyload-container
        div = self._driver.find_element(
            By.XPATH, "//div[@id='article']/div/div[@class='row']/div")
        # itarete over all divs inside dl
        elements = list(div.find_elements(By.XPATH, "./*"))
        h4s = elements[::3]
        ps = elements[1::3]
        hrs = elements[2::3]  # we can ignore these
        for h4, p in zip(h4s, ps):
            # get year
            year = int(h4.text)
            print("Processing year:", year)
            for line in p.text.split('\n'):
                month_word = line.split(':')[0]
                urls = p.find_elements(By.XPATH, ".//a")
                testemony_href = urls[0].get_attribute("href")
                html_href = urls[1].get_attribute("href")
                pdf_href = urls[2].get_attribute("href")
                chart_data_href = urls[3].get_attribute("href")
                date = pd.to_datetime(f"{month_word} {year}", format='%B %Y')
                print(date)
                print("HTML href", html_href)

                to_process[date] = {
                    "file_url": html_href,
                }

            if year <= 2017:
                break

        for date, data in to_process.items():
            print("Processing date:", date)
            text = self.parse_html(data["file_url"])
            to_process[date]["full_extracted_text"] = text

        df = pd.DataFrame(to_process).T.reset_index(names=["date_created"])
        df["country_code_alpha_3"] = FEDBankScrapper.COUNTRY_CODE_ALPHA_3
        df["country_name"] = FEDBankScrapper.COUNTRY_NAME

        ipaddr, hostname = self.ip_hostname()
        df["scraping_ip"] = ipaddr
        df["scraping_machine"] = hostname

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(
            user_name=self.user_name)
        df.to_sql("central_banks", con=dbconnx,
                  if_exists="append", index=False)

    def __del__(self):
        self._driver.close()

    def get_base_url_for_year(self) -> str:
        return f"https://www.federalreserve.gov/monetarypolicy/publications/mpr_default.htm"
