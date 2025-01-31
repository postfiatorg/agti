
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
from selenium.common.exceptions import NoSuchElementException
import pdfplumber
from sqlalchemy import text








class AustraliaBankScrapper:
    """
    We decided to not convert timestamp from CET to EST, becasue ECB provides just date without time.
    and the date will be the same in both timezones.
    
    In summary, the Decisions focus on immediate outcomes, while the Minutes provide a deeper context behind the decisions.
    That is why we fetch the minutes only.

    """
    COUNTRY_CODE_ALPHA_3 = "AUS"
    COUNTRY_NAME = "Australia"

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
    
    def get_all_dates_in_db_for_year(self, year:int):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT date_created 
FROM {} 
WHERE date_created >= :start_date 
AND date_created < :end_date            
AND country_code_alpha_3 = :country_code_alpha_3
""".format(self.table_name))
        params = {
            "start_date": f"{year}-01-01",
            "end_date": f"{year + 1}-01-01",
            "country_code_alpha_3": AustraliaBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]



    def parse_html(self, href: str):
        self._driver.get(href)
        # select div id="content" and section
        content = self._driver.find_element(By.ID, "content")
        sections = content.find_elements(By.TAG_NAME, "section")
        if len(sections) == 1:
            return sections[0].text
        if len(sections) > 1:
            raise ValueError("More than one section found")
        # sections == 0
        # we need to extract text from content, but we need to ignore <aside> and <div> tags

        # get all children
        children = content.find_elements(By.XPATH, "./*")
        text = ""
        for child in children:
            if child.tag_name == "aside" or child.tag_name == "div":
                continue
            text += child.text
        return text
    

    def process_year(self, year:int):
        dates_scraped = self.get_all_dates_in_db_for_year(year)
        self._driver.get(self.get_base_url_monetary_policy_minutes_year(year))
        # get class "list-articles"
        try:
            ul = self._driver.find_element(By.CLASS_NAME, "list-articles")
        except NoSuchElementException:
            print(f"No data found for year: {year}")
            return
        # iterate over all li elements
        to_process = {}
        for li in ul.find_elements(By.XPATH, "./*"):
            # find a element
            a = li.find_element(By.XPATH, ".//a")
            href = a.get_attribute("href")
            text = a.text
            date = pd.to_datetime(text)
            if date in dates_scraped:
                print("Skipping decision date:", date)
                continue

            to_process[date] = {
                "file_url": href,
            }
        result = []
        for date, data in to_process.items():
            print("Processing date:", date)
            text = self.parse_html(data["file_url"])
            result.append({
                "date_created": date,
                "file_url": data["file_url"],
                "full_extracted_text": text
            })

        df = pd.DataFrame(result)
        # if empty skip
        if df.empty:
            print(f"No new data found for year: {year}")
            return
        
        # we do not convert timestamp, because we do not get hours

        ipaddr, hostname = self.ip_hostname()

        df["country_name"] = AustraliaBankScrapper.COUNTRY_NAME
        df["country_code_alpha_3"] = AustraliaBankScrapper.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)
            



    def process_all_years(self):
        current_year = pd.Timestamp.now().year
        for year in range(2006, current_year + 1):
            self.process_year(year)
    

    def __del__(self):
        self._driver.close()
    

    def get_base_url(self) -> str:
        return f"https://www.rba.gov.au/monetary-policy"
    
    def get_base_url_monetary_policy_minutes_year(self, year:int) -> str:
        return f"{self.get_base_url()}/rba-board-minutes/{year}/"
    

    def get_base_url_monetary_policy_decision_year(self, year:int) -> str:
        return f"{self.get_base_url()}/int-rate-decisions/{year}/"