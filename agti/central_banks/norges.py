import os
import socket
import pandas as pd
import pdfplumber
import requests

from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.settings import CredentialManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from sqlalchemy import text


class NorgesBankScrapper:
    COUNTRY_CODE_ALPHA_3 = "NOR"
    COUNTRY_NAME = "Norway"


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


    def get_all_db_urls(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(
            user_name=self.user_name)
        query = text("""
SELECT file_url
FROM {}
WHERE country_code_alpha_3 = :country_code_alpha_3
""".format(self.table_name))
        params = {
            "country_code_alpha_3": NorgesBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
        

    def download_and_read_pdf(self, url: str) -> str:
        filename = os.path.basename(url)

        r = requests.get(url)

        with open(self.datadump_directory_path / filename, 'wb') as outfile:
            outfile.write(r.content)
        with pdfplumber.open(self.datadump_directory_path / filename) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text()
        os.remove(self.datadump_directory_path / filename)

        return text
    


    def load_main_page(self):
        wait = WebDriverWait(self._driver, 1)
        while True:
            try:
                load_more_button = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "_jsNewsListLoadMore_newslist"))
                )
                wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "_jsNewsListLoadMore_newslist")))

                self._driver.execute_script("arguments[0].click();", load_more_button)
            except TimeoutException:
                # TODO add verify that something has loaded
                break
    


    def process_all_years(self):
        all_urls = self.get_all_db_urls()
        self._driver.get(self.get_base_url())
        self.load_main_page()


        news_list_div = WebDriverWait(self._driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "_jsNewsListResultList_newslist")))

        articles = news_list_div.find_elements(By.TAG_NAME, "article")
        subsites = []
        for article in articles:
            h3_element = article.find_element(By.TAG_NAME, "h3")
            href = h3_element.find_element(By.TAG_NAME, "a").get_attribute("href")
            if href in all_urls:
                print("Skipping", href)
                continue
            subsites.append(href)

        # process links
        output = []
        for href in subsites:
            self._driver.get(href)
            # extract timestamp
            # locate div meta-container
            meta_container = self._driver.find_element(By.CLASS_NAME, "meta-container")
            meta = meta_container.find_element(By.CLASS_NAME, "meta")

            # drop "published " from text 
            timestamp_text = meta.text[10:]
            timestamp = pd.to_datetime(timestamp_text)
            print(timestamp_text)

            # get link to pdf
            pdf_link = None
            links = list(self._driver.find_elements(By.CLASS_NAME,"download-link"))
            if len(links) == 0:
                # they are some special pages with different elements, we use that
                links = self._driver.find_elements(By.CLASS_NAME, "publication-start__body")

            if len(links) == 0:
                print("No links found")
                continue
            print("Number of links found:", len(links))
            pdf_link = links[0].find_element(By.TAG_NAME, "a").get_attribute("href")
            print("PDF link:", pdf_link)

            text = self.download_and_read_pdf(pdf_link)
            output.append(
                {
                    "date_published": timestamp,
                    "file_url": href,
                    "full_extracted_text": text,
                }
            )

        df = pd.DataFrame(output)
        if df.empty:
            print("No new data found")
            return

        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = NorgesBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = NorgesBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr


        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)




    def __del__(self):
        self._driver.close()

    def get_base_url(self):
        return "https://www.norges-bank.no/en/news-events/news-publications/Reports/Monetary-Policy-Report/"