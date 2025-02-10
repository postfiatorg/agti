
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


class CanadaBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "CAN"
    COUNTRY_NAME = "Canada"

    SPECIAL_CASE_HREF = "https://www.imf.org/en/Publications/CR/Issues/2019/06/24/Canada-Financial-System-Stability-Assessment-47024"

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
            "country_code_alpha_3": CanadaBankScrapper.COUNTRY_CODE_ALPHA_3
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

        df["country_name"] = CanadaBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = CanadaBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)

        

    def process_all_years(self):
        pass

    def find_avaible_pdf(self, divs):
        for div in divs:
            a_tags = div.find_elements(By.TAG_NAME, "a")
            for a in a_tags:
                a_href = a.get_attribute("href")
                if a_href.endswith(".pdf"):
                    return self.download_and_read_pdf(a_href)
        return None
    
    def extract_main_content(self):
        # find xpath main with id "main-content"
        try:
            main_content = self._driver.find_element(By.XPATH, "//main[@id='main-content']")
            return main_content.text
        except:
            return None

    def process_publications(self):
        wait = WebDriverWait(self._driver, 10)

        all_urls = self.get_all_db_urls()
        
        page = 1
        to_process = []
        while True:
            self._driver.get(self.get_url_publications(page))
            # get span with class "num-results"
            wait.until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='num-results']")))
            span = self._driver.find_element(By.XPATH, "//span[@class='num-results']")
            num_results = int(span.text)
            if num_results == 0:
                break

            xpath_results = "//article[@class='media']"
            articles = self._driver.find_elements(By.XPATH, xpath_results)
            for article in articles:
                # each article has multiple content types
                date = pd.to_datetime(
                    article.find_element(By.XPATH, ".//div[@class='media-body']/span").text
                )
                a_tag = article.find_element(By.XPATH,".//div[@class='media-body']/h3/a")
                href = a_tag.get_attribute("href")
                if href in all_urls:
                    print("Data already exists for: ", href)
                    continue
                to_process.append((date, href))
            page += 1


        output = []
        for date, href in to_process:
            print("processing: ", href)
            if href == self.SPECIAL_CASE_HREF:
                self._driver.get(href)
                # find a tag with "publication-actions__btn btn publication-actions__btn-primary" class
                a_tag = self._driver.find_element(By.XPATH, "//a[@class='publication-actions__btn btn publication-actions__btn-primary']")
                pdf_href = a_tag.get_attribute("href")
                output.append({
                    "file_url": href,
                    "date_published": date,
                    "full_extracted_text": self.download_and_read_pdf(pdf_href)
                })
            elif href.endswith(".pdf"):
                # Note there can be multiple other pdf files as well on the page
                pdf_href = href
                text = self.download_and_read_pdf(pdf_href)
                output.append({
                    "file_url": href,
                    "date_published": date,
                    "full_extracted_text": text
                })
            else:
                self._driver.get(href)
                # find all divs containing "Available as:"
                divs = self._driver.find_elements(By.XPATH, "//div[contains(text(),'Available as:')]")
                if (text := self.find_avaible_pdf(divs)) is not None:
                    output.append({
                        "file_url": href,
                        "date_published": date,
                        "full_extracted_text": text
                    })
                elif (text := self.extract_main_content()) is not None:
                    output.append({
                        "file_url": href,
                        "date_published": date,
                        "full_extracted_text": text
                    })
                else:
                    print("No pdf or main content found for: ", href)

        self.add_to_db(output)        
            

        

    def get_url_publications(self, page: int) -> str:
        return f"https://www.bankofcanada.ca/publications/browse/?mt_page={page}"
    
