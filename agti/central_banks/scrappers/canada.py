
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
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf
from sqlalchemy import text


class CanadaBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "CAN"
    COUNTRY_NAME = "Canada"

    SPECIAL_CASE_HREF = "https://www.imf.org/en/Publications/CR/Issues/2019/06/24/Canada-Financial-System-Stability-Assessment-47024"

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

    def process_all_years(self):
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
                text = download_and_read_pdf(pdf_href, self.datadump_directory_path)
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
    
