
import re
import pandas as pd
import logging
from selenium.webdriver.common.by import By
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..utils import download_and_read_pdf
from ..base_scrapper import BaseBankScraper
import pdfplumber

logger = logging.getLogger(__name__)

__all__ = ["FEDBankScrapper"]

class FEDBankScrapper(BaseBankScraper):
    """
    We use  "For use at" initial text to detect correct tolerances for pdfplumber.
    Plus, we use it for extracting exact datetime.
    """
    COUNTRY_CODE_ALPHA_3 = "USA"
    COUNTRY_NAME = "United States of America"

    def get_pdf_links(self, text):
        pattern = r'<a\s+href="([^"]+)">([^<]+)</a>'
        a_elements = re.findall(pattern, text)
        out = []
        for href, text in a_elements:
            # it can be "PDF" or " PDF" (with space :) ).
            if "PDF" in text:
                out.append(href)
        if len(out) == 0:
            return None
        if len(out) > 1:
            raise ValueError("Multiple PDF links found in text")
        return out[0]

    def get_exact_date(self, text):
        pattern = r"For use at (\d{1,2}:\d{2}) (a\.m\.|p\.m\.)\,? (EST|EDT|E\.S\.T\.|E\.D\.T\.)\s([a-zA-Z]+|[a-zA-Z]+\s[a-zA-Z]+) (\d|\d\d), (\d\d\d\d)"
        initial_text = text[:200]
        groups = re.findall(pattern, initial_text)[0]
        clock = groups[0]  # 10:00
        ampm = groups[1]  # a.m.
        month = groups[3].split('\n')[1] if '\n' in groups[3] else groups[3]
        day = groups[4]
        year = groups[5]
        # convert to pandas with clock
        return pd.to_datetime(f"{month} {day}, {year} {clock} {ampm}")

    @staticmethod
    def evaluate_tolerances(pdf_path):
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            for x_tol in range(1, 10):
                for y_tol in range(1, 10):
                    text = page.extract_text(
                        x_tolerance=x_tol, y_tolerance=y_tol)
                    if "For use at" in text:
                        return x_tol, y_tol
        raise ValueError("No correct tolerances found")

    def process_all_years(self):
        all_urls = self.get_all_db_urls()

        self._driver.get(self.get_base_url_years())

        to_process = []

        # select dl by id lazyload-container
        div = self._driver.find_element(
            By.XPATH, "//div[@id='article']/div/div[@class='row']/div")
        # iterate over all divs inside dl
        elements = list(div.find_elements(By.XPATH, "./*"))
        h4s = elements[::3]
        ps = elements[1::3]
        # hrs = elements[2::3]  # we can ignore these
        for h4, p in zip(h4s, ps):
            # get year
            year = int(h4.text.strip())
            # get inner html of p
            html_p = p.get_attribute("innerHTML")
            for line in html_p.split("<br>"):
                month_word = line.split(':')[0].strip()
                logger.debug(f"Checking {month_word} {year}")
                pdf_url_path = self.get_pdf_links(line)
                if pdf_url_path is None:
                    logger.warning("No PDF link found for date:",
                          f"{month_word} {year}")
                    continue
                href = self.get_base_url() + pdf_url_path
                if href in all_urls:
                    logger.info(f"Href is already in db: {href}")
                    continue
                to_process.append(href)

        output = []

        for href in to_process:
            logger.info(f"Processing: {href}")
            text = download_and_read_pdf(href, self.datadump_directory_path, evaluate_tolerances=self.evaluate_tolerances)
            exact_datetime = self.get_exact_date(text)
            output.append({
                    "file_url": href,
                    "date_published": exact_datetime,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text,
                })

        self.add_to_db(output)

    def get_base_url(self):
        return "https://www.federalreserve.gov"

    def get_base_url_years(self) -> str:
        return self.get_base_url() + "/monetarypolicy/publications/mpr_default.htm"
