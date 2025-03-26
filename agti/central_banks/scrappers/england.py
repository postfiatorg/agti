import time
from typing import Dict, Set
from urllib.parse import urlparse
import pandas as pd
import logging
import copy
import selenium
from selenium.webdriver.common.by import By
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf


logger = logging.getLogger(__name__)

__all__ = ["EnglandBankScrapper"]

class EnglandBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "ENG"
    COUNTRY_NAME = "England"
    MAX_OLD_YEAR = 2000

    def cookie_click(self):
        wait = WebDriverWait(self._driver, 10)
        xpath = "//button[@class='cookie__button btn btn-default btn-neutral']"

        success = False
        repeat = 3
        for i in range(repeat):
            try:
                cookie_btn = wait.until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                # click the cookie banner
                cookie_btn.click()
                success = True
                break
            except Exception as e:
                if repeat != (repeat - 1):
                    logger.warning(f"Could not click cookie banner repeating", exc_info=True)
                else:
                    logger.exception(f"Could not click cookie banner", exc_info=True)
                    
        if not success:
            raise Exception("Can not click cookie banner")
        
        wait.until(EC.visibility_of_element_located((By.ID, "SearchResults")))
        logger.debug("Cookie banner clicked")


    def init_filter(self, topic):
        self._driver.get(self.get_base_url())
        wait = WebDriverWait(self._driver, 10,0.2)
    
        def iterate_over_labels(div_element_xpath, filters_list):
            wait.until(EC.visibility_of_all_elements_located((By.XPATH, xpath)))
            filter_div = wait.until(EC.visibility_of_element_located((By.XPATH, div_element_xpath)))
            filter_labels = filter_div.find_elements(By.TAG_NAME, "label")
            for label in filter_labels:
                name = label.text.strip()
                if name in filters_list:
                    #self._driver.execute_script("arguments[0].scrollIntoView(false);", label)
                    label = wait.until(EC.element_to_be_clickable(label))
                    label.click()
                    filters_list.remove(name)
                    return True
            logger.warning(f"Could not find any of {filters_list} in {[x.text for x in filter_labels]}")
            return False
        wait.until(EC.visibility_of_all_elements_located((By.ID, "SearchResults")))

        type_filters_to_check = set(["Research blog", "Event", "News", "Publication", "Speech", "Statistics"])
        xpath = "//div[@class='sidebar-filters type-filters']"
        while len(type_filters_to_check) > 0:
            iterate_over_labels(xpath, type_filters_to_check)
            
        topics = set([topic])
        xpath = "//div[@class='sidebar-filters taxonomy-filters']"
        wait.until(
            lambda driver: driver.find_element(By.ID, "SearchResults").get_dom_attribute("style") in (None, "")
        )
        max_repeats = 3
        while len(topics) > 0 and max_repeats > 0:
            if not iterate_over_labels(xpath, topics):
                max_repeats -= 1
        if max_repeats == 0:
            raise ValueError(f"Could not apply filters for topic: {topic}")

        wait.until(EC.visibility_of_all_elements_located((By.ID, "SearchResults")))
        wait.until(
            lambda driver: driver.find_element(By.ID, "SearchResults").get_dom_attribute("style") in (None, "")
        )
        logger.debug(f"Filters applied to topic: {topic}")
    

    def parse_html(self, url: str):
        # find main with id="main-content"
        self._driver.get(url)
        url_parsed = urlparse(url)
        xpath = "//main[@id='main-content']"
        main = self._driver.find_element(By.XPATH, xpath)
        text = main.text
        if len(text) == 0:
            raise ValueError("No text found in HTML file")
        # all links
        links_output = []
        links = main.find_elements(By.XPATH, ".//a")
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
                link_text = download_and_read_pdf(link_href,self.datadump_directory_path, self._driver)
            # NOTE add support for different file types
            links_output.append({
                "file_url": url,
                "link_url": link_href,
                "link_name": link.text,
                "full_extracted_text": link_text,
            })
        return text, links_output




    def process_all_years(self):
        self._driver.get(self.get_base_url())
        self.cookie_click()
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        topics = [
            "Quarterly Bulletin",
            "News release",
            "Monetary Policy Committee (MPC)",
            "Research",
            "Working Paper",
            "Monetary policy",
            "Financial Policy Committee (FPC)",
            "Weekly report",
            "Minutes",
            "Financial stability"
        ]

       
        
        for topic in topics:
            logger.info(f"Processing topic: {topic}")
            self.init_filter(topic)
            year = pd.Timestamp.now().year
            to_process = []
            while year >= self.MAX_OLD_YEAR:
                # get id = SearchResults div
                search_results = self._driver.find_element(By.ID, "SearchResults")
                # find all elements with class="col3"
                elements = search_results.find_elements(By.XPATH, ".//div[@class='col3']")
                for element in elements:
                    a = element.find_element(By.TAG_NAME, "a")
                    href = a.get_attribute("href")

                    # tag is under a in class="release-tag" div 
                    tag = a.find_element(By.CLASS_NAME, "release-tag-wrap").text

                    # get date using time tag with datetime attribute
                    time_tag = element.find_element(By.TAG_NAME, "time")
                    date = pd.to_datetime(time_tag.get_attribute("datetime"))
                    to_process.append((tag, href, date))
                    year = min(year, date.year)
                if not self.go_to_next_page():
                    break



            result = []
            total_categories = []
            total_links = []
            for tag, href, date in to_process:
                total_categories.extend([
                    {"file_url": href, "category_name": category.value}
                    for category in get_categories(tag)
                    if (href, category.value) not in all_categories
                ])
                if href in all_urls:
                    logger.debug(f"Href is already in db: {href}")
                    continue
                logger.info(f"Processing: {href}")
                text, links = self.parse_html(href)
                total_links.extend(links)
                result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": href,
                    "full_extracted_text": text,
                })
                

            self.add_all_atomic(result,total_categories,total_links)


    def get_current_page_number(self):
        wait = WebDriverWait(self._driver, 10)
        # find list-pagination__link list-pagination__link--page list-pagination__link--is-current
        xpath = "//a[@class='list-pagination__link list-pagination__link--page list-pagination__link--is-current']"
        current_page = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
        # get data-page-link attribute
        return int(current_page.get_attribute("data-page-link"))
    

    def go_to_next_page(self):
        wait = WebDriverWait(self._driver, 30, 0.1)
        current_page_number = self.get_current_page_number()

        current_page_xpath = f"//a[@data-page-link='{current_page_number}']"
        next_page_xpath = f"//a[@data-page-link='{current_page_number + 1}']"
        
        current_page = wait.until(EC.visibility_of_element_located((By.XPATH, current_page_xpath)))
        next_pages = self._driver.find_elements(By.XPATH, next_page_xpath)
        if len(next_pages) == 0:
            logger.debug("No more pages")
            return False
        next_page = next_pages[0]
        wait.until(EC.element_to_be_clickable(next_page))
        
        self._driver.execute_script("arguments[0].click();", next_page)
        
        # wait for finish loading class list-pagination ul
        #wait.until(EC.visibility_of_all_elements_located((By.ID, "SearchResults")))
        # this is slower, but we are sure everything is loaded
        # Phase 1: Wait until the style attribute is non-empty, indicating the change has started.
        wait.until(
            lambda d: d.find_element(By.ID, "SearchResults").get_dom_attribute("style") not in (None, "")
        )
        wait.until(
            lambda driver: driver.find_element(By.ID, "SearchResults").get_dom_attribute("style") in (None, "")
        )
        logger.debug(f"Going to page: {current_page_number + 1}")
        return True



    def get_base_url(self) -> str:
        return "https://www.bankofengland.co.uk/news"
    



def get_categories(tag: str) -> Set[Categories]:
    """
    Return a set of Categories based on the input tag.
    The mapping is based on the Bank of England taxonomy mapped to our unified categories.
    If no mapping is found, returns {Categories.OTHER}.
    """
    # Mapping from tag patterns to a set of Categories.
    mapping = {
        # --- Event Items ---
        "Event": {Categories.NEWS_AND_EVENTS},
        "Event // Agency briefing": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Event // Bank of England agenda for research": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Centre for Central Banking Studies (CCBS)": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Conference": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Parliamentary hearing": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Event // Seminar": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Treasury Select Committee (TSC)": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Event // Webinar": {Categories.NEWS_AND_EVENTS},
        "Event // Workshop": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        
        # --- News Items ---
        "News": {Categories.NEWS_AND_EVENTS},
        "News // Annual Report": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Archive": {Categories.NEWS_AND_EVENTS},
        "News // Banknotes": {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS},
        "News // CBDC Technology Forum minutes": {Categories.MONETARY_POLICY, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Corporate responsibility": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Court minutes": {Categories.NEWS_AND_EVENTS, Categories.MONETARY_POLICY},
        "News // Financial Policy Committee (FPC)": {Categories.MONETARY_POLICY},
        "News // Financial Policy Committee statement": {Categories.MONETARY_POLICY},
        "News // Financial Stability Report (FSR)": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Financial stability": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Financial stability policy": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Fintech": {Categories.NEWS_AND_EVENTS, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Funding for Lending Scheme (FLS)": {Categories.MONETARY_POLICY},
        "News // Independent Evaluation Office (IEO)": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Index-Linked Treasury Stocks": {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS},
        "News // Letter": {Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.NEWS_AND_EVENTS},
        "News // Markets": {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Minutes": {Categories.MONETARY_POLICY},
        "News // Monetary Policy Committee (MPC)": {Categories.MONETARY_POLICY},
        "News // News release": {Categories.NEWS_AND_EVENTS},
        "News // Open Forum": {Categories.NEWS_AND_EVENTS},
        "News // Payment systems": {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Prudential regulation": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Record": {Categories.NEWS_AND_EVENTS},
        "News // Research": {Categories.RESEARCH_AND_DATA},
        "News // Schools competition": {Categories.OTHER},
        "News // Semi-Annual FX Turnover Survey results": {Categories.RESEARCH_AND_DATA, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Staff": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Statement": {Categories.MONETARY_POLICY},
        "News // Statistical article": {Categories.RESEARCH_AND_DATA},
        "News // Statistics": {Categories.RESEARCH_AND_DATA},
        "News // Stress testing": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Treasury Select Committee (TSC)": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        
        # --- Publication Items ---
        "Publication": {Categories.RESEARCH_AND_DATA},
        "Publication // Andy Haldane": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Publication // Approach document": {Categories.MONETARY_POLICY},
        "Publication // Balance sheet": {Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.RESEARCH_AND_DATA},
        "Publication // Centre for Central Banking Studies (CCBS)": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Publication // Consultation paper": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Countercyclical capital buffer (CCyB)": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Discussion paper": {Categories.RESEARCH_AND_DATA},
        "Publication // External MPC Unit Discussion Paper": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Financial Policy Committee (FPC)": {Categories.MONETARY_POLICY},
        "Publication // Financial Stability Paper": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Financial Stability Report (FSR)": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Financial Stability in Focus": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Financial stability": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Independent Evaluation Office (IEO)": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Publication // Inflation Attitudes Survey": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Inflation Report (IR)": {Categories.MONETARY_POLICY},
        "Publication // Minutes": {Categories.MONETARY_POLICY},
        "Publication // Monetary Policy Committee (MPC)": {Categories.MONETARY_POLICY},
        "Publication // Monetary Policy Report (MPR)": {Categories.MONETARY_POLICY},
        "Publication // Paper": {Categories.RESEARCH_AND_DATA},
        "Publication // Policy statement": {Categories.MONETARY_POLICY},
        "Publication // Quarterly Bulletin": {Categories.RESEARCH_AND_DATA},
        "Publication // Report": {Categories.RESEARCH_AND_DATA, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Publication // Research": {Categories.RESEARCH_AND_DATA},
        "Publication // Response to a paper": {Categories.RESEARCH_AND_DATA},
        "Publication // Statement of policy": {Categories.MONETARY_POLICY},
        "Publication // Stress testing": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Systemic Risk Survey Results": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Weekly report": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Working Paper": {Categories.RESEARCH_AND_DATA},
        
        # --- Research Blog Items ---
        "Research blog": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Research blog // Bank Overground": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        
        # --- Speech Items ---
        "Speech": {Categories.NEWS_AND_EVENTS},
        "Speech // Webinar": {Categories.NEWS_AND_EVENTS},
    }
    
    result : Set[Categories] = set()
    
    # Check for an exact match first.
    if tag in mapping:
        result.update(mapping[tag])
    else:
        # Fallback: check if any mapping key is a prefix of the tag.
        for key, cats in mapping.items():
            if tag.startswith(key):
                result.update(cats)
    
    # If no category was matched, assign it to OTHER.
    if not result:
        result.add(Categories.OTHER)
    
    return result