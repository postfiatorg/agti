
import os
import re
import socket
import warnings
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


class SwitzerlandBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "CHE"
    COUNTRY_NAME = "Switzerland"

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
    
    def get_all_db_urls(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT file_url 
FROM {} 
WHERE country_code_alpha_3 = :country_code_alpha_3
""".format(self.table_name))
        params = {
            "country_code_alpha_3": SwitzerlandBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
    

    def __del__(self):
        self._driver.close()


    def add_to_db(self, output):
        df = pd.DataFrame(output)
        if df.empty:
            print("No new data found")
            return
        
        # drop row with all NaN values
        df = df.dropna(how="all")
        
        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = SwitzerlandBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = SwitzerlandBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)

            
    def find_and_download_pdf(self, url: str) -> dict:
        self._driver.get(url)
        # span with class="h-typo-tiny"
        span_date = self._driver.find_element(By.XPATH, "//span[@class='h-typo-tiny']")
        # December 18, 2024
        date = pd.to_datetime(span_date.text, format="%B %d, %Y")
        print("Processing: ", url)

        # xpath to find a tag with span with text "Download"
        try:
            download_button = self._driver.find_element(By.XPATH, "//a[span[normalize-space(text())='Download']]")
        except:
            print("No download button found, trying german or french")
            # we can try to find german or french instead
            try:
                # we find one. Sometime there are two, we take the first one
                download_button = self._driver.find_element(By.XPATH, "//a[span[normalize-space(text())='german' or normalize-space(text())='french']]")
            except:
                print("No German or French download button found")
                return {
                    "date_published": date,
                    "file_url": url,
                    "full_extracted_text": "",
                }

        pdf_href = download_button.get_attribute("href")
        text = self.download_and_read_pdf(pdf_href)
        return {
                "date_published": date,
                "file_url": url,
                "full_extracted_text": text,
            }
    

    def process_teasor_list(self, func_target_url):
        found_any = False
        to_process = []
        all_db_urls = self.get_all_db_urls()
        page = 1
        while True:
            url = func_target_url(page)
            if url is None:
                break
            self._driver.get(url)
            # get "link-teaser-list" ul tag
            a_tags = self._driver.find_elements(By.XPATH, "//ul[contains(@class, 'link-teaser-list') or contains(@class, 'publication-link-list')]//a")
            if len(a_tags) == 0:
                break
            else:
                found_any = True
            for a in a_tags:
                # get a tag
                href = a.get_attribute("href")
                if href in all_db_urls:
                    print("Data already exists for: ", href)
                    continue
                to_process.append(href)
            page += 1
        if not found_any:
            raise ValueError("No data found")
        output = []
        for url in to_process:
            if url.endswith(".pdf"):
                text = self.download_and_read_pdf(url)
                output.append({
                    "date_published": None,
                    "file_url": url,
                    "full_extracted_text": text,
                })
            else:
                output.append(self.find_and_download_pdf(url))
            
        self.add_to_db(output)
    
    def process_annual_report(self):
        self._driver.get(self.get_url_annual_report())
        # xpath get all as from ul tag with class="sitemap-linklist"
        a_tags = self._driver.find_elements(By.XPATH, "//ul[@class='sitemap-linklist']//a")
        if len(a_tags) == 0:
            raise ValueError("No data found for annual report")
        to_process = []
        all_db_urls = self.get_all_db_urls()
        for a in a_tags:
            href = a.get_attribute("href")
            if href in all_db_urls:
                print("Data already exists for: ", href)
                continue
            to_process.append(href)

        output = []
        for href in to_process:
            self._driver.get(href)
            if href == "https://www.snb.ch/en/news-publications/annual-report/annual-report-1996-2017":
                def f_url(page: int) -> str:
                    if page == 1:
                        return "https://www.snb.ch/en/news-publications/annual-report/annual-report-1996-2017"
                    return None
                self.process_teasor_list(f_url)
                
            elif href == "https://www.snb.ch/en/news-publications/annual-report/annual-report-1907-1995":
                a_tags = self._driver.find_elements(By.XPATH, "//ul[@class='link-teaser-list']//a")
                for a in a_tags:
                    href = a.get_attribute("href")
                    if href in all_db_urls:
                        print("Data already exists for: ", href)
                        continue
                    print("Processing: ", href)
                    text = self.download_and_read_pdf(href)
                    output.append({
                        "date_published": None,
                        "file_url": href,
                        "full_extracted_text": text,
                    })
            else:
                a = self._driver.find_element(By.XPATH, "//a[.//span[contains(text(), 'Complete annual report')]]")
                href2 = a.get_attribute("href")
                if href2 in all_db_urls:
                    print("Data already exists for: ", href2)
                    continue
                output.append(self.find_and_download_pdf(href2))
        if len(output) == 0:
            return      
        self.add_to_db(output)


        

    def process_all_years(self):
        # based on https://www.snb.ch/en/news-publications and
        # https://www.snb.ch/en/news-publications/order-publications
        
        print("Process Annual Report")
        self.process_annual_report()

        print("Processing Quarterly Bulletin")
        self.process_teasor_list(self.get_url_quarterly_bulletin)
        print("Processing Business Cycle Signals")
        self.process_teasor_list(self.get_url_business_cycle_signals)

        print("Processing Financial Stability Report")
        self.process_teasor_list(self.get_url_financial_stability_report)

        print("Processing SNB Economic Studies")
        self.process_teasor_list(self.get_url_SNB_economic_studies)
        print("Processing SNB Working Papers")
        self.process_teasor_list(self.get_url_SNB_working_papers)

        # we skip monthly statistical bulletin

        # we skip monthly bulletin of banking statistics

        # we skip Banks in Switzerland reports (discontinued)


        # additional staff we fetch
        print("Processing SNB Quarterly Bulletin Studies")
        self.process_teasor_list(self.get_url_SNB_quarterly_bulletin_studies)
        print("Processing Speeches")
        self.process_teasor_list(self.get_url_speeches)

        

    def get_url_quarterly_bulletin(self, page: int) -> str:
        return f"https://www.snb.ch/en/news-publications/economy/quarterly-bulletin/quarterly-bulletin~page-1={page}~"
    def get_url_financial_stability_report(self, page: int) -> str:
        return f"https://www.snb.ch/en/news-publications/economy/report-financial-stability~page-1={page}~"
    
    # SNB
    def get_url_SNB_quarterly_bulletin_studies(self, page: int) -> str:
        if page == 1:
            return f"https://www.snb.ch/en/news-publications/economy/quarterly-bulletin/quarterly-bulletin-studies"
        return None
    def get_url_SNB_economic_studies(self, page: int) -> str:
        if page == 1:
            return f"https://www.snb.ch/en/news-publications/economy/economic-studies"
        return None
    def get_url_SNB_working_papers(self, page: int) -> str:
        if page == 1:
            return f"https://www.snb.ch/en/news-publications/economy/working-papers"
        return None
    def get_url_SNB_economic_notes(self, page: int) -> str:
        if page == 1:
            return f"https://www.snb.ch/en/news-publications/economy/economic-notes"
        return None
    
    # speeches
    def get_url_speeches(self, page:int) -> str:
        return f"https://www.snb.ch/en/news-publications/speeches~page-0={page}~"
    
    def get_url_annual_report(self) -> str:
        return f"https://www.snb.ch/en/news-publications/annual-report-overview"
    
    def get_url_business_cycle_signals(self, page:int) -> str:
        return f"https://www.snb.ch/en/news-publications/business-cycle-signals~page-2={page}~"
    
