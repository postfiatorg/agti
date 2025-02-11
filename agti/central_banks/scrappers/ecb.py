import warnings
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import pageBottom




__all__ = ["ECBBankScrapper"]



class ECBBankScrapper(BaseBankScraper):
    """
    We decided to not convert timestamp from CET to EST, becasue ECB provides just date without time.
    and the date will be the same in both timezones.
    """
    INITIAL_YEAR = 1998
    COUNTRY_CODE_ALPHA_3 = "EUE"
    COUNTRY_NAME = "European Union"


    def parse_html(self, href: str):
        self._driver.get(href)
        # select all text from dev with class section
        element = self._driver.find_element(By.XPATH, "//main/div[contains(@class, 'section')]")
        text = element.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        return text



    def process_all_years(self):
        all_urls = self.get_all_db_urls()

        self._driver.get(self.get_base_url_for_year())
        # scroll to the bottom of the page
        pageBottom(self._driver)
        
        to_process = []
        
        # select dl by id lazyload-container
        dl = self._driver.find_element(By.ID, "lazyload-container")
        # itarete over all divs inside dl
        for div in dl.find_elements(By.XPATH, ".//div[@data-index]"):
            # find all sub divs with data-index attribute
            data_index = div.get_attribute("data-index")
            print("Processing data-index:", data_index)
            # find dt with isodate attribute
            elements = div.find_elements(By.XPATH, "./*")
            dts = elements[::2]
            dds = elements[1::2]
            assert len(dts) == len(dds), "Number of dt and dd elements is not equal"
            for dt, dd in zip(dts, dds):
                isodate = dt.get_attribute("isodate")
                pd_isodate = pd.to_datetime(isodate)
                a_element = dd.find_element(By.XPATH, "./div[@class='ecb-langSelector']/span/a")
                lang = a_element.get_attribute("lang")
                href = a_element.get_attribute("href")
                if lang != "en":
                    warnings.warn(f"Language is not English: {lang} for date {isodate}")

                if href in all_urls:
                    print("URL already in DB: ", href)
                    continue

                to_process.append((pd_isodate, href))

        output = []
        for date, href in to_process:
            print("Processing href:", href)
            text = self.parse_html(href)
            output.append({
                    "file_url": href,
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text
                })

        self.add_to_db(output)
    

    def get_base_url_for_year(self) -> str:
        return f"https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html"