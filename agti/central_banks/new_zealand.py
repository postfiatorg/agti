
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


class NewZealandBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "NZL"
    COUNTRY_NAME = "New-Zealand"

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
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.140 Safari/537.36"
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
            "country_code_alpha_3": NewZealandBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
    

    def __del__(self):
        self._driver.close()


    def extract_list(self):
        wait = WebDriverWait(self._driver, 10)
        output = []
        # get all coveo-list-layout CoveoResult
        xpath = '//div[@class="coveo-list-layout CoveoResult"]'
        wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="coveo-list-layout CoveoResult"]')))
        elements = self._driver.find_elements(By.XPATH, xpath)
        for element in elements:
            # get a tag with href
            a = element.find_element(By.TAG_NAME, "a")
            href = a.get_attribute("href")
            # get date span with class containint "listing-card__datetime"  and child span
            xpath = './/span[contains(@class, "listing-card__datetime")]/span'
            date_span = element.find_element(By.XPATH, xpath)
            date = pd.to_datetime(date_span.text)
            output.append((date, href))
        return output


            

    def process_all_years(self):

        all_dates = self.get_all_dates()

        wait = WebDriverWait(self._driver, 10)
        # pages count from 0
        self.get(self.get_base_url_for_page())
        # span with class="coveo-highlight coveo-highlight-total-count"
        xpath_number_of_pages = '//span[@class="coveo-highlight coveo-highlight-total-count"]'
        xpath_results_loaded = '//div[@class="coveo-result-list-container coveo-list-layout-container"]'
        # wait for the element to be present
        wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath_results_loaded)))
        span = self._driver.find_element(By.XPATH, xpath_number_of_pages)
        num_pages =  int(span.text) // 100 + 1


        to_process = []
        for page in range(num_pages):
            print(page)
            to_process.extend(self.extract_list())
            self.get(self.get_base_url_for_page(page + 1))
            # presense of div with class="coveo-result-list-container coveo-list-layout-container
            wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath_results_loaded)))


        output = []
        for date, href in to_process:
            if date in all_dates:
                continue
            print("Processing: ", date, href)
            self.get(href)
            # find all a tags containing "download-card__link" in class
            xpath = '//a[contains(@class, "download-card__link")]'
            a_tags = self._driver.find_elements(By.XPATH, xpath)
            if len(a_tags) == 0:
                print("No pdf files found")
                continue
            total_text = ""
            for a in a_tags:
                pdf_href = a.get_attribute("href")
                if href.endswith(".pdf"):
                    pdf_text =  self.download_and_read_pdf(pdf_href)
                    total_text += "\n######## PDF FILE START ########\n" + pdf_text + "\n######## PDF FILE END ########\n"
            output.append({
                "date_published": date,
                "file_url": href,
                "full_extracted_text": total_text,
            })
                

        df = pd.DataFrame(output)
        if df.empty:
            print("No new data found")
            return
        
        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = NewZealandBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = NewZealandBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)


    def get_base_url_for_page(self, page: int = 0) -> str:
        first = page * 100
        first_txt = ""
        if first != 0:
            first_txt = f"first={first}&"
        return f"https://www.rbnz.govt.nz/monetary-policy/monetary-policy-statement/monetary-policy-statement-filtered-listing-page#{first_txt}sort=%40computedsortdate%20descending&numberOfResults=100"

    def get(self, url):
        self._driver.get(url)
        time.sleep(5)
        