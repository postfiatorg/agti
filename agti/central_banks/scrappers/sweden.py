import logging
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf

logger = logging.getLogger(__name__)
__all__ = ["SwedenBankScrapper"]

class SwedenBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "SWE"
    COUNTRY_NAME = "Sweden"


    def process_year(self, year: int):

        all_urls = self.get_all_db_urls()

        self._driver.get(self.get_base_url_for_year(year))
        # get using xpath div with class="listing-block__body"
        div = self._driver.find_element(By.XPATH, "//div[@class='listing-block__body']")
        # get all a tags
        a_tags = div.find_elements(By.TAG_NAME, "a")
        to_process = []

        for a in a_tags:
            # get span with class="label"
            span = a.find_element(By.TAG_NAME, 'span')# we take the first span
            date = pd.to_datetime(span.text, dayfirst=True)
            href = a.get_attribute("href")
            if href in all_urls:
                logger.info(f"Href is already in db: {href}")
                continue
            to_process.append((date, href))
        output = []
        for date, href in to_process:
            logger.info(f"Processing: {href}")
            self._driver.get(href)
            pdf_href = None
            # find all a tags with class="button button--iconed"
            a_tags = self._driver.find_elements(By.XPATH, "//a[@class='button button--iconed']")
            if len(a_tags) == 0:
                # find h2 with text "Documents"
                h2 = self._driver.find_element(By.XPATH, "//h2[normalize-space(text())='Documents']")
                # get grandparent div
                div = h2.find_element(By.XPATH, "//ancestor::div[@class='linklist-block']")
                
                # find all a tags
                a_tags = div.find_elements(By.TAG_NAME, "a")
                # filter wil text "Monetary Policy Report"
                a_tags = [a for a in a_tags if "Monetary Policy Report".lower() in a.text.lower()]
            for a in a_tags:
                temp_href = a.get_attribute("href")
                if temp_href.endswith(".pdf"):
                    pdf_href = temp_href
                    break
            output.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": href,
                "full_extracted_text": download_and_read_pdf(pdf_href, self.datadump_directory_path) if pdf_href else None,
            })


        self.add_to_db(output)

            


    def process_all_years(self):
        # process new years
        current_year = pd.Timestamp.now().year
        for year in range(2017, current_year + 1):
            self.process_year(year)

        # process archive
        self.process_archive()


    def process_archive(self):
        all_urls = self.get_all_db_urls()
        self._driver.get(self.get_archive_url())
        # get table tag
        table = self._driver.find_element(By.TAG_NAME, "table")
        # get all tr tags
        trs = list(table.find_elements(By.TAG_NAME, "tr"))
        header_row = trs.pop(0)
        # verify should ahave th Date and th Header
        ths = header_row.find_elements(By.TAG_NAME, "th")
        if len(ths) != 2:
            raise ValueError("Header row does not have 2 columns")
        if ths[0].text.strip() != "Date":
            raise ValueError("First column should be Date")
        if ths[1].text.strip() != "Header":
            raise ValueError("Second column should be Header")
        

        to_process = []
        for tr in trs:
            tag_time = tr.find_element(By.TAG_NAME, "time")
            date = pd.to_datetime(tag_time.text, dayfirst=True)
            # NOTE this is not the best because we have multiple documents for the same date
            a = tr.find_element(By.TAG_NAME, "a")
            href = a.get_attribute("href")
            if href in all_urls:
                logger.info(f"Href is already in db: {href}")
                continue
            to_process.append((date, href))

        output = []
        for date, href in to_process:
            text = None
            logger.info(f"Processing: {href}")
            if href.endswith(".pdf"):
                text = download_and_read_pdf(href, self.datadump_directory_path)
            output.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": href,
                "full_extracted_text": text,
            })

        self.add_to_db(output)
                

    def get_archive_url(self):
        return "https://archive.riksbank.se/en/Web-archive/Published/Published-from-the-Riksbank/Monetary-policy/Monetary-Policy-Report/index.html@all=1.html"

    def get_base_url_for_year(self, year:int) -> str:
        if year < 2017:
            raise ValueError("No data available for year before 2017")
        return f"https://www.riksbank.se/en-gb/monetary-policy/monetary-policy-report/?year={year}"