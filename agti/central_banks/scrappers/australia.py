
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
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf
from sqlalchemy import text


__all__ = ["AustraliaBankScrapper"]





class AustraliaBankScrapper(BaseBankScraper):
    """
    We decided to not convert timestamp from CET to EST, becasue ECB provides just date without time.
    and the date will be the same in both timezones.
    
    In summary, the Decisions focus on immediate outcomes, while the Minutes provide a deeper context behind the decisions.
    That is why we fetch the minutes only.

    """
    COUNTRY_CODE_ALPHA_3 = "AUS"
    COUNTRY_NAME = "Australia"



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
        all_urls = self.get_all_db_urls()
        self._driver.get(self.get_base_url_monetary_policy_minutes_year(year))
        # get class "list-articles"
        try:
            ul = self._driver.find_element(By.CLASS_NAME, "list-articles")
        except NoSuchElementException:
            print(f"No data found for year: {year}")
            return
        # iterate over all li elements
        to_process = []
        for li in ul.find_elements(By.XPATH, "./*"):
            # find a element
            a = li.find_element(By.XPATH, ".//a")
            href = a.get_attribute("href")
            text = a.text
            date = pd.to_datetime(text)
            if href in all_urls:
                print("Skipping href:", href)
                continue

            to_process.append([date, href])
        result = []
        for date, href in to_process:
            print("Processing date:", date)
            text = self.parse_html(href)
            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": href,
                "full_extracted_text": text
            })

        self.add_to_db(result)
            



    def process_all_years(self):
        current_year = pd.Timestamp.now().year
        for year in range(2006, current_year + 1):
            self.process_year(year)
    

    def get_base_url(self) -> str:
        return f"https://www.rba.gov.au/monetary-policy"
    
    def get_base_url_monetary_policy_minutes_year(self, year:int) -> str:
        return f"{self.get_base_url()}/rba-board-minutes/{year}/"
    

    def get_base_url_monetary_policy_decision_year(self, year:int) -> str:
        return f"{self.get_base_url()}/int-rate-decisions/{year}/"