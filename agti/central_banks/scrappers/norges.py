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


        # News & Events
        ## News and publications (id:107500)
        news_events_categoties = {
            "Press releases": (80,[Categories.NEWS_AND_EVENTS]),
            "New Items": (71,[Categories.NEWS_AND_EVENTS]),
            "Speeches": (69,[Categories.NEWS_AND_EVENTS]),
            "Submissions": (81,[Categories.OTHER, Categories.NEWS_AND_EVENTS]),
            "Balance sheet": (82,[Categories.RESEARCH_AND_DATA, Categories.NEWS_AND_EVENTS]),
            "Circulars": (83,[Categories.OTHER,Categories.NEWS_AND_EVENTS]),
            "Articles and opinion pieces": (68,[Categories.OTHER,Categories.NEWS_AND_EVENTS]),

        }
        for name, (category_number, categories) in news_events_categoties.items():
            logger.info(f"Processing category: {name} ({category_number})")
            self.process_id(107500, categories, category_filter=category_number)
            logger.info(f"Finished processing category: {name} ({category_number})")
        
        ## Publications (id: 107501)
        ### reports
        publications = {
            "Norway's financial system": (115, [
                Categories.FINANCIAL_STABILITY_AND_REGULATION, 
                Categories.RESEARCH_AND_DATA
            ]),
            "Documentation Note": (146, [Categories.OTHER]),
            "Financial Infrastructure Report": (67, [
                Categories.FINANCIAL_STABILITY_AND_REGULATION, 
                Categories.RESEARCH_AND_DATA
            ]),
            "Financial Stability Report": (66, [
                Categories.FINANCIAL_STABILITY_AND_REGULATION, 
                Categories.RESEARCH_AND_DATA
            ]),
            "Management of foreign exchange reserves": (96, [
                Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, 
                Categories.MONETARY_POLICY
            ]),
            "Expectations Survey": (105, [Categories.RESEARCH_AND_DATA]),
            "Market surveys": (97, [
                Categories.RESEARCH_AND_DATA, 
                Categories.FINANCIAL_STABILITY_AND_REGULATION
            ]),
            "Norges Bank Papers": (95, [Categories.RESEARCH_AND_DATA]),
            "Monetary Policy Report": (65, [Categories.MONETARY_POLICY]),
            "Regional Network reports": (100, [
                Categories.RESEARCH_AND_DATA, 
                Categories.INSTITUTIONAL_AND_GOVERNANCE
            ]),
            "Norges Bank’s Survey of Bank Lending": (98, [Categories.RESEARCH_AND_DATA]),
            "Annual Report": (103, [Categories.INSTITUTIONAL_AND_GOVERNANCE])
        }
        for name, (category_number, categories) in publications.items():
            logger.info(f"Processing category: {name} ({category_number})")
            self.process_id(107501, categories, category_filter=category_number)
            logger.info(f"Finished processing category: {name} ({category_number})")

        ### papers
        papers = {
            "Occasional Papers": (101, [Categories.RESEARCH_AND_DATA]),
            "Staff Memo": (64, [Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.OTHER]),
            "Government Debt Management Memo": (133, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE]),
            "Working Papers": (102, [Categories.RESEARCH_AND_DATA]),
            "External evaluations": (116, [Categories.INSTITUTIONAL_AND_GOVERNANCE])
        }
        for name, (category_number, categories) in papers.items():
            logger.info(f"Processing category: {name} ({category_number})")
            self.process_id(107501, categories, category_filter=category_number)
            logger.info(f"Finished processing category: {name} ({category_number})")


        #




        # monetary policy MEETINGS (78157)
        self.process_id(78157, [Categories.MONETARY_POLICY])


        self.process_id(11404, [Categories.MONETARY_POLICY])








    def process_id(self, id: int, categories: list[Categories], category_filter=0):
        all_urls = self.get_all_db_urls()
        all_categories = self.get_all_db_categories()
        # Process a single ID
        logger.info(f"Processing ID: {id}")
        page = 1
        
        while True:
            output = []
            page_url = self.api_url(id, page,category_filter=category_filter)
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
                    # add missing categories
                    total_missing_cat = [
                        {
                            "file_url": href,
                            "category_name": category.value,
                        } for category in categories if (href, category.value) not in all_categories
                    ]
                    if len(total_missing_cat) > 0:
                        self.add_to_categories(total_missing_cat)
                    continue
                output.append(
                    (href, date)
                )
            if page == 1 and len(articles) == 0:
                raise ValueError(f"No articles found for ID: {id} and category_filter: {category_filter}")


            # process
            result = []
            total_links = []
            total_categories = []
            for href, date in output:
                href_parsed = urlparse(href)
                logger.info(f"Processing: {href}")
                self.get(href)
                xpath_start = "//div[@class='article publication-start'] | //article[@class='article']"
                content = self.driver_manager.driver.find_element(By.XPATH, xpath_start)
                article_text = content.text
                # process links
                links = content.find_elements(By.XPATH, ".//a")
                for link in links:
                    link_text = None
                    link_href = link.get_attribute("href")
                    if link_href is None:
                        continue
                    link_href_parsed = urlparse(link_href)
                    if link_href_parsed.fragment != '':
                        if href_parsed[:3] == link_href_parsed[:3]:
                            # we ignore links to the same page (fragment identifier)
                            continue
                        # NOTE: we do not parse the text yet
                    if link_href_parsed.path.lower().endswith('.pdf'):
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
            
            page += 1


        

    @staticmethod
    def api_url(id: int, page: int, category_filter: int = 0) -> str:
        # API URL for Norges Bank
        return f"https://www.norges-bank.no/api/NewsList/LoadMoreAndFilter?currentPageId={id}&page={page}&clickedCategoryFilter={category_filter}&clickedYearFilter=0&language=en"