import logging
import time
from urllib.parse import urlparse
import pandas as pd
from agti.utilities.db_manager import DBConnectionManager
from agti.utilities.settings import CredentialManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf


logger = logging.getLogger(__name__)

__all__ = ["NorgesBankScrapper"]

class NorgesBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "NOR"
    COUNTRY_NAME = "Norway"
    NETLOC = "www.norges-bank.no"


    def initialize_cookies(self, go_to_url = False):
        current_url = self.driver_manager.driver.current_url
        # go to main page
        parsed_current_url = urlparse(current_url)
        # if we are under /api/NewsList/LoadMoreAndFilter,  go to main page
        if parsed_current_url.path.startswith("/api/NewsList/LoadMoreAndFilter") or go_to_url:
            # go to main page
            self.driver_manager.driver.get(f"https://{self.NETLOC}/")

        self.driver_manager.driver.execute_script("CookieInformation.submitConsent()")
        time.sleep(0.1)
        self.cookies = self.driver_manager.driver.get_cookies()
        if parsed_current_url.path.startswith("/api/NewsList/LoadMoreAndFilter") and not go_to_url:
            self.driver_manager.driver.get(current_url)


    
    


    def process_all_years(self):
        # monetary policy
        self.process_id(11404, [Categories.MONETARY_POLICY])
    def process_id(self, id: int, categories: list[Categories]):
        all_urls = self.get_all_db_urls()
        # Process a single ID
        logger.info(f"Processing ID: {id}")
        page = 1
        output = []
        while True:
            page_url = self.api_url(id, page)
            logger.info(f"Fetching page {page} from URL: {page_url}")
            self.get(page_url)
            xpath_articles = "//article[@class='article-list__item']"
            articles = self.driver_manager.driver.find_elements(By.XPATH, xpath_articles)
            if len(articles) == 0:
                break
            for article in articles:
                # we can ignore tags
                date_str = article.find_element(By.XPATH, ".//div[@class='meta']")
                date = pd.to_datetime(date_str.text)
                a_tag = article.find_element(By.XPATH, ".//h3/a")
                href = a_tag.get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    continue
                output.append(
                    (href, date)
                )
            
            page += 1


        # process
        result = []
        total_links = []
        total_categories = []
        for href, date in output:
            logger.info(f"Processing: {href}")
            self.get(href)
            xpath_start = "//div[@class='article publication-start']"
            content = self.driver_manager.driver.find_element(By.XPATH, xpath_start)
            article_text = content.text
            # process links
            links = content.find_elements(By.XPATH, ".//a")
            for link in links:
                link_text = None
                link_href = link.get_attribute("href")
                if link_href.endswith(".pdf"):
                    link_text = download_and_read_pdf(link_href,self.datadump_directory_path, self)
                total_links.append({
                    "file_url": href,
                    "link_url": link_href,
                    "link_name": link.text,
                    "full_extracted_text": link_text,
                })
            result.append({
                "file_url": href,
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "full_extracted_text": article_text,
            })
            total_categories.extend([
                {
                    "file_url": href,
                    "category_name": cat.value,
                } for cat in categories
            ])
        self.add_all_atomic(result, total_categories, total_links)

    @staticmethod
    def api_url(id: int, page: int):
        # API URL for Norges Bank
        return f"https://www.norges-bank.no/api/NewsList/LoadMoreAndFilter?currentPageId={id}&page={page}&clickedCategoryFilter=0&clickedYearFilter=0&language=en"