import pandas as pd
import selenium
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

    def process_outline_of_monetary_policy(self):
        # NOTE we can ignore Outline of Monetary Policy
        # "https://www.boj.or.jp/en/mopo/outline/index.htm"
        pass


    def process_monetery_policy_meeting(self):
        # Monetary Policy Meeting
        
        ##########################
        ## Summary of Opinions
        ##########################
        logger.info("Processing MP meeting summary of opinions")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/mopo/mpmsche_minu/opinion_{}/index.htm", 
                2016,
                2
            ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        ##########################
        ## Minutes
        ##########################
        logger.info("Processing MP meeting minutes")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/mopo/mpmsche_minu/minu_{}/index.htm",
                1998,
                2
            ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        ##########################
        ## Others
        ##########################
        logger.info("Processing MP meeting others")
        self._driver.get("https://www.boj.or.jp/en/mopo/mpmsche_minu/m_ref/index.htm")
        to_process = self.process_href_table(self.get_all_db_urls(), 2)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        

    def process_monetery_policy_releases(self):
        # Monetary Policy Releases
        logger.info("Processing monetary policy releases")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/mopo/mpmdeci/mpr_{}/index.htm", 
                1998,
                2
            ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

    def process_monetery_policy_measures(self):
        # NOTE: we can ignore
        # based on our check everything is in the releases
        # https://www.boj.or.jp/en/mopo/measures/index.htms
        pass 

    def process_monetery_policy_outlook(self):
        # Outlook for Economic Activity and Prices
        all_urls = self.get_all_db_urls()
        # we can ignore boxes, because they are part of outlooks
        # we can ignore higlihts, because they are part of outlooks

        ##########################
        # Outlook for Economic Activity and Prices
        ##########################
        # they are 2 tables, we let the first one to be parsed by process_href_table
        # and the second one we parse it here
        logger.info("Processing outlook for economic activity and prices")
        self._driver.get("https://www.boj.or.jp/en/mopo/outlook/index.htm")
        to_process = self.process_href_table(self.get_all_db_urls(), 2)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])

        # find the second table
        tables = self._driver.find_elements(By.XPATH, "//table[@class='js-tbl' or @class='STDtable TAB_top']")
        if len(tables) != 2:
            raise ValueError("Expected 2 tables in outlook Japan bank")
        
        to_process = []
        table = tables[1]
        tbody = table.find_element(By.XPATH, ".//tbody")
        table_rows = tbody.find_elements(By.XPATH,".//tr")
        for row in table_rows:
            tds = list(row.find_elements(By.XPATH,".//td"))
            date = pd.to_datetime(tds[0].text)
            # try tds[2], if text is empty, try tds[1]
            if tds[2].text == "":
                link = tds[1].find_element(By.XPATH, ".//a")
            else:
                link = tds[2].find_element(By.XPATH, ".//a")
            href = link.get_attribute("href")
            if href in all_urls:
                logger.info(f"Href is already in db: {href}")
                continue

            to_process.append((date, href))
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Monthly Report of Recent Economic and Financial Developments
        ##########################
        logger.info("Processing monthly report of recent economic and financial developments")
        for to_process in self.find_hrefs_tab_table_iter(
            "https://www.boj.or.jp/en/mopo/gp_{}/index.htm",
            1998,
            2
        ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])
        

    def process_monetery_policy_diet(self):
        
        ##########################
        # Semiannual Report on Currency and Monetary Control
        ##########################
        self._driver.get("https://www.boj.or.jp/en/mopo/diet/d_report/index.htm")
        to_process = self.process_href_table(self.get_all_db_urls(), 2)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Statement concerning the Report to the Diet
        ##########################
        self._driver.get("https://www.boj.or.jp/en/mopo/diet/d_state/index.htm")
        to_process = self.process_href_table(self.get_all_db_urls(), 2)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.NEWS_AND_EVENTS.value])

    def process_monetery_policy_research_speech_statement(self):
        # Research Papers, Reports, Speeches and Statements Related to Monetary Policy
        all_urls = self.get_all_db_urls()

        ##########################
        ## Statements
        ##########################
        logger.info("Processing MP statements")
        self._driver.get("https://www.boj.or.jp/en/mopo/r_menu_dan/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.NEWS_AND_EVENTS.value])

        ##########################
        ## Reserach Papers
        ##########################
        logger.info("Processing MP research papers")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/mopo/r_menu_ken/index.htm?mylist=", 2)

        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])
        ##########################
        ## Speeches
        ##########################
        logger.info("Processing MP speeches")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/mopo/r_menu_kou/index.htm?mylist=", 3)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value, Categories.NEWS_AND_EVENTS.value])




    ##########################
    # Financial system processing
    ##########################

   
    
    def process_financial_system_reports(self):
        
        all_urls = self.get_all_db_urls()
        # on -site
        # https://www.boj.or.jp/en/finsys/exam_monit/exampolicy/index.htm
        self._driver.get("https://www.boj.or.jp/en/finsys/exam_monit/exampolicy/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value])

        # financial reports (https://www.boj.or.jp/en/finsys/fsr/index.htm)
        # TODO: it is better to fetch it from: (Home>Research and Studies>BOJ Reports & Research Papers>Financial System Report)
        # https://www.boj.or.jp/en/research/brp/fsr/index.htm#p02
        # old markets reports
        self._driver.get("https://www.boj.or.jp/en/research/brp/fmr/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])
        # new system reports and Annex series
        self._driver.get("https://www.boj.or.jp/en/research/brp/fsr/index.htm")
        to_process = self.process_href_table(all_urls, 2, num_tables=2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        # policy
        # https://www.boj.or.jp/en/finsys/fs_policy/index.htm (table)
        # https://www.boj.or.jp/en/finsys/msfs/index.htm
        # https://www.boj.or.jp/en/finsys/spp/index.htm
        # https://www.boj.or.jp/en/finsys/rfs/index.htm
        self._driver.get("https://www.boj.or.jp/en/finsys/fs_policy/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        self._driver.get("https://www.boj.or.jp/en/finsys/msfs/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value])

        self._driver.get("https://www.boj.or.jp/en/finsys/spp/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value,Categories.INSTITUTIONAL_AND_GOVERNANCE.value])

        self._driver.get("https://www.boj.or.jp/en/finsys/rfs/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        # coorination
        # https://www.boj.or.jp/en/finsys/macpru/index.htm
        # https://www.boj.or.jp/en/finsys/cofsa/index.htm (all tables)

        self._driver.get("https://www.boj.or.jp/en/finsys/macpru/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.NEWS_AND_EVENTS.value])

        self._driver.get("https://www.boj.or.jp/en/finsys/cofsa/index.htm")
        to_process = self.process_href_table(all_urls, 2, num_tables=3)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        # seminars Not important ommited
        # https://www.boj.or.jp/en/finsys/c_aft/index.htm (maybe not importnant)
        

        # research papers
        to_process = self.find_hrefs_mylist_table(
            "https://www.boj.or.jp/en/finsys/r_menu_ron/index.htm?mylist=",
            2
        )
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])
        # speeches
        to_process = self.find_hrefs_mylist_table(
            "https://www.boj.or.jp/en/finsys/r_menu_koen/index.htm?mylist=",
            3
        )
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value, Categories.NEWS_AND_EVENTS.value])
        
        # statements
        self._driver.get("https://www.boj.or.jp/en/finsys/r_menu_dan/index.htm")
        to_process = self.process_href_table(all_urls, 2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.NEWS_AND_EVENTS.value])
        





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
            if link_href is None:
                continue
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
    
    def extract_data_update_tables(self, to_process, tags):
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

    def find_hrefs_tab_table_iter(self, f_url, init_year, table_size):
        # Monetary Policy Releases
        all_urls = self.get_all_db_urls()
        this_year = pd.Timestamp.now().year
        for year in range(init_year, this_year + 1):
            logger.info(f"Processing year: {year}")
            self._driver.get(f_url.format(year))
            to_process = self.process_href_table(all_urls, table_size)
            yield to_process

    def find_hrefs_mylist_table(self, url, table_size):
        all_urls = self.get_all_db_urls()
        i = 0
        to_process = []
        while True:
            logger.info(f"Processing mylist: {i*50 +1}")
            self._driver.get(f"{url}{i*50 +1}")
            temp = self.process_href_table(all_urls, table_size)
            if len(temp) == 0:
                break
            to_process.extend(temp)
            i += 1
        return to_process
    

    def process_href_table(self,all_urls,table_size, num_tables=1):
        to_process = []
        # while it can find
        for table_id in range(num_tables):
            try:
                table = self._driver.find_element(By.XPATH, "//table[@class='js-tbl' or @class='STDtable TAB_top']")
            except selenium.common.exceptions.NoSuchElementException:
                logger.warning(f"No table found for {self._driver.current_url}, table_id: {table_id}")
                return to_process
            #caption = table.find_element(By.XPATH, ".//caption").text
            tbody = table.find_element(By.XPATH, ".//tbody")
            table_rows = tbody.find_elements(By.XPATH,".//tr")
            if len(table_rows) == 0:
                return to_process
            for row in table_rows:
                tds = list(row.find_elements(By.XPATH,".//td"))
                date = pd.to_datetime(tds[0].text)
                link = tds[table_size-1].find_element(By.XPATH, ".//a")
                href = link.get_attribute("href")
                if href in all_urls:
                    logger.info(f"Href is already in db: {href}")
                    continue

                to_process.append((date, href))
        return to_process


    
    def process_all_years(self):
        self.process_financial_system_reports()
        # MONETARY POLICY
        self.process_monetery_policy_outlook()
        self.process_monetery_policy_diet()
        self.process_monetery_policy_meeting()
        self.process_monetery_policy_releases()
        self.process_monetery_policy_research_speech_statement()
    
