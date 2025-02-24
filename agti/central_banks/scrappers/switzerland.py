import pandas as pd
import logging
from selenium.webdriver.common.by import By
import urllib
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf

logger = logging.getLogger(__name__)
__all__ = ["SwitzerlandBankScrapper"]

class SwitzerlandBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "CHE"
    COUNTRY_NAME = "Switzerland"

            
    def find_and_download_pdf(self, url: str) -> dict:
        self._driver.get(url)
        # span with class="h-typo-tiny"
        span_date = self._driver.find_element(By.XPATH, "//span[@class='h-typo-tiny']")
        # December 18, 2024
        date = pd.to_datetime(span_date.text, format="%B %d, %Y")

        # xpath to find a tag with span with text "Download"
        try:
            download_button = self._driver.find_element(By.XPATH, "//a[span[normalize-space(text())='Download']]")
        except:
            logger.info("No download button found, trying german or french", extra={"url": url})
            # we can try to find german or french instead
            try:
                # we find one. Sometime there are two, we take the first one
                download_button = self._driver.find_element(By.XPATH, "//a[span[normalize-space(text())='german' or normalize-space(text())='french']]")
            except:
                logger.error("No German or French download button found", extra={"url": url})
                return {
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": "",
                }

        pdf_href = download_button.get_attribute("href")
        text = download_and_read_pdf(pdf_href,self.datadump_directory_path)
        return {
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
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
                    logger.debug(f"Href is already in db: {href}")
                    continue
                to_process.append(href)
            page += 1
        if not found_any:
            raise ValueError("No data found")
        output = []
        for url in to_process:
            logger.info(f"Processing: {url}")
            if url.endswith(".pdf"):
                text = download_and_read_pdf(url, self.datadump_directory_path)
                output.append({
                    "date_published": None,
                    "scraping_time": pd.Timestamp.now(),
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
                logger.debug(f"Href is already in db: {href}")
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
                        logger.debug(f"Href is already in db: {href}")
                        continue
                    logger.info(f"Processing: {href}")
                    text = download_and_read_pdf(href, self.datadump_directory_path)
                    output.append({
                        "date_published": None,
                        "scraping_time": pd.Timestamp.now(),
                        "file_url": href,
                        "full_extracted_text": text,
                    })
            else:
                a = self._driver.find_element(By.XPATH, "//a[.//span[contains(text(), 'Complete annual report')]]")
                href2 = a.get_attribute("href")
                if href2 in all_db_urls:
                    logger.info(f"Href is already in db: {href2}")
                    continue
                logger.info(f"Processing: {href2}")
                output.append(self.find_and_download_pdf(href2))
        if len(output) == 0:
            return      
        self.add_to_db(output)


        

    def process_all_years(self):
        # based on https://www.snb.ch/en/news-publications and
        # https://www.snb.ch/en/news-publications/order-publications
        
        logger.info("Processing Annual Report")
        self.process_annual_report()

        logger.info("Processing Quarterly Bulletin")
        self.process_teasor_list(self.get_url_quarterly_bulletin)
        logger.info("Processing Business Cycle Signals")
        self.process_teasor_list(self.get_url_business_cycle_signals)

        logger.info("Processing Financial Stability Report")
        self.process_teasor_list(self.get_url_financial_stability_report)

        logger.info("Processing Economic Studies")
        self.process_teasor_list(self.get_url_SNB_economic_studies)
        logger.info("Processing SNB Working Papers")
        self.process_teasor_list(self.get_url_SNB_working_papers)

        # we skip monthly statistical bulletin

        # we skip monthly bulletin of banking statistics

        # we skip Banks in Switzerland reports (discontinued)


        # additional staff we fetch
        logger.info("Processing SNB Quarterly Bulletin Studies")
        self.process_teasor_list(self.get_url_SNB_quarterly_bulletin_studies)
        logger.info("Processing Speeches")
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
    
