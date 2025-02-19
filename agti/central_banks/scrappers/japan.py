import pandas as pd
from selenium.webdriver.common.by import By
import logging
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf
from ..utils import Categories
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

__all__ = ["JapanBankScrapper"]



# NOTE! before running read_html check for any pdf links and download them
# there can be also some zips or any other files, but we are not going to handle them
class JapanBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "JPN"
    COUNTRY_NAME = "Japan"


    ##########################
    # Monetery Policy processing
    ##########################
    def process_monetery_policy_meeting(self):
        # Monetary Policy Meeting
        pass

    def process_monetery_policy_releases(self):
        # Monetary Policy Releases
        logger.info("Processing monetary policy releases")
        all_urls = self.get_all_db_urls()
        this_year = pd.Timestamp.now().year
        for year in range(1998, this_year + 1):
            logger.info(f"Processing year: {year}")
            self._driver.get(f"https://www.boj.or.jp/en/mopo/mpmdeci/mpr_{year}/index.htm")
            table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl']")
            #caption = table.find_element(By.XPATH, ".//caption").text
            tbody = table.find_element(By.XPATH, ".//tbody")
            to_process = []
            for row in tbody.find_elements(By.XPATH,".//tr"):
                tds = list(row.find_elements(By.XPATH,".//td"))
                date = pd.to_datetime(tds[0].text)
                link = tds[1].find_element(By.XPATH, ".//a")
                # parse link, get href and text
                href = link.get_attribute("href")
                if href in all_urls:
                    logger.info(f"Href is already in db: {href}")
                    continue
                to_process.append((date, href))
            
            self.process_raw_data(to_process, [Categories.MONETARY_POLICY.value])

    def process_monetery_policy_measures(self):
        # Monetary Policy Measures
        pass

    def process_monetery_policy_outlook(self):
        # Outlook for Economic Activity and Prices
        pass

    def process_monetery_policy_diet(self):
        # Reports to the Diet
        pass

    def process_monetery_policy_research_speech_statement(self):
        # Research Papers, Reports, Speeches and Statements Related to Monetary Policy
        all_urls = self.get_all_db_urls()


        ##########################
        ## Statements
        ##########################
        logger.info("Processing MP statements")
        self._driver.get("https://www.boj.or.jp/en/mopo/r_menu_dan/index.htm")
        table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl']")
        #caption = table.find_element(By.XPATH, ".//caption").text
        tbody = table.find_element(By.XPATH, ".//tbody")
        to_process = []
        for row in tbody.find_elements(By.XPATH,".//tr"):
            tds = list(row.find_elements(By.XPATH,".//td"))
            date = pd.to_datetime(tds[0].text)
            link = tds[1].find_element(By.XPATH, ".//a")
            # parse link, get href and text
            href = link.get_attribute("href")
            if href in all_urls:
                logger.info(f"Href is already in db: {href}")
                continue

            # drop [PDF xxKB] from link text
            #link_text = link.text
            # using regex
            #link_text = re.sub(r"\[PDF (\d+,)*\d+KB\]", "", link.text)

            to_process.append((date, href))
        
        self.process_raw_data(to_process, [Categories.MONETARY_POLICY.value, Categories.NEWS_AND_EVENTS.value])


        ## Reserach Papers
        logger.info("Processing MP research papers")
        i = 0
        to_process = []
        while True:
            self._driver.get(f"https://www.boj.or.jp/en/mopo/r_menu_ron/index.htm?mylist={i*50 +1}")
            table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl']")
            #caption = table.find_element(By.XPATH, ".//caption").text
            tbody = table.find_element(By.XPATH, ".//tbody")
            table_rows = tbody.find_elements(By.XPATH,".//tr")
            if len(table_rows) == 0:
                break
            for row in table_rows:
                tds = list(row.find_elements(By.XPATH,".//td"))
                date = pd.to_datetime(tds[0].text)
                link = tds[1].find_element(By.XPATH, ".//a")
                # parse link, get href and text
                href = link.get_attribute("href")
                if href in all_urls:
                    logger.info(f"Href is already in db: {href}")
                    continue

                # drop [PDF xxKB] from link text
                #link_text = link.text
                # using regex
                #link_text = re.sub(r"\[PDF (\d+,)*\d+KB\]", "", link.text)

                to_process.append((date, href))
            i += 1

        self.process_raw_data(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])
            
        ## Speeches
        logger.info("Processing MP speeches")
        i = 0
        to_process = []
        while True:
            self._driver.get(f"https://www.boj.or.jp/en/mopo/r_menu_koen/index.htm?mylist={i*50 +1}")
            table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl']")
            #caption = table.find_element(By.XPATH, ".//caption").text
            tbody = table.find_element(By.XPATH, ".//tbody")
            table_rows = tbody.find_elements(By.XPATH,".//tr")
            if len(table_rows) == 0:
                break
            for row in table_rows:
                tds = list(row.find_elements(By.XPATH,".//td"))
                date = pd.to_datetime(tds[0].text)
                # NOTE speaches has 3 columns
                # tds[1] is Speaker
                link = tds[2].find_element(By.XPATH, ".//a")
                # parse link, get href and text
                href = link.get_attribute("href")
                if href in all_urls:
                    logger.info(f"Href is already in db: {href}")
                    continue

                # drop [PDF xxKB] from link text
                #link_text = link.text
                # using regex
                #link_text = re.sub(r"\[PDF (\d+,)*\d+KB\]", "", link.text)

                to_process.append((date, href))
            i += 1

        self.process_raw_data(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value, Categories.NEWS_AND_EVENTS.value])




    ##########################
    # Financial system processing
    ##########################

    ##########################
    # Payments and Markets
    ########################## 

    ##########################
    # Banknotes, The Bank's Treasury Funds and JGS Services
    ##########################

    ##########################
    # International Finance
    ########################## 

    ##########################
    # Research and Studies
    ##########################

    ##########################
    # Statistics
    ##########################

    ##########################
    # Helper function
    ########################## 
    def read_html(self, url: str):
        self._driver.get(url)
        url_parsed = urlparse(url)
        element = self._driver.find_element(By.ID, "contents")
        text = element.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        # find all links and download them
        links = element.find_elements(By.XPATH, ".//a")
        links_output = []
        for link in links:
            link_href = link.get_attribute("href")
            link_href_parsed = urlparse(link_href)
            link_text = None
            if link_href_parsed.fragment != '':
                if url_parsed[:3] == link_href_parsed[:3]:
                    # we ignore links to the same page (fragment identifier)
                    continue
                # NOTE: we do not parse the text yet
            elif link_href.endswith("pdf"):
                link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
            # NOTE add support for different file types
            links_output.append({
                "file_url": url,
                "link_url": link_href,
                "link_name": link.text,
                "full_extracted_text": link_text,
            })
        return text, links_output
    
    def process_raw_data(self, to_process, tags):
        result = []
        total_tags = []
        total_links = []
        for date, href in to_process:
            logger.info(f"Processing: {href}")
            if href.endswith("pdf"):
                text = download_and_read_pdf(href, self.datadump_directory_path)
            elif href.endswith("htm"):
                text, links_output = self.read_html(href)
                total_links.extend(links_output)
            else:
                raise ValueError("Unknown file format")
            
            result.append({
                "file_url": href,
                "full_extracted_text": text,
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
            })
            total_tags.extend(
                [{
                "file_url": href,
                "category_name": tag,
                } for tag in tags]
            )
        self.add_all_atomic(result,total_tags,total_links)
    
    def process_all_years(self):
        self.process_monetery_policy_releases()
        self.process_monetery_policy_research_speech_statement()
    
