
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
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf



__all__ = ["JapanBankScrapper"]




class JapanBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "JPN"
    COUNTRY_NAME = "Japan"
    INITIAL_YEAR = 1998

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
                text = download_and_read_pdf(href, self.datadump_directory_path)
            elif href.endswith("htm"):
                print("Parsing HTML file:", href)
                text = self.read_html(href)
            else:
                raise ValueError("Unknown file format")
            
            result.append({
                "file_url": href,
                "full_extracted_text": text,
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
            })

        self.add_to_db(result)



    def process_all_years(self):
        this_year = pd.Timestamp.now().year
        for year in range(JapanBankScrapper.INITIAL_YEAR, this_year + 1):
            self.process_year(year)

    
    def read_html(self, url: str):
        self._driver.get(url)
        element = self._driver.find_element(By.CSS_SELECTOR, "div.outline.mod_outer")
        text = element.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        return text
    

    def get_base_url_for_year(self, year: int) -> str:
        return f"https://www.boj.or.jp/en/mopo/mpmdeci/mpr_{year}/index.htm"