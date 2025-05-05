
import logging
from typing import Set
from urllib.parse import urlparse
import pandas as pd
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from agti.agti.central_banks.types import ExtensionType, URLType
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, classify_extension, download_and_read_pdf

__all__ = ["CanadaBankScrapper"]

logger = logging.getLogger(__name__)

class CanadaBankScrapper(BaseBankScraper):
    IGNORED_PATHS = [
        "/search/",
        "/profile/",
        "/about/",
    ]
    def process_all_years(self):
        wait = WebDriverWait(self.driver_manager.driver, 30)

        page = 1
        while True:
            all_urls = self.get_all_db_urls()
            all_categories = self.get_all_db_categories()
            to_process = []
            logger.debug(f"Current page: {page}")
            self.get(self.get_url_search(page))

            xpath_results = "(//article | //div)[@class='media' and starts-with(@id, 'post-')]"

            #wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath_results)))
            articles = list(self.driver_manager.driver.find_elements(By.XPATH, xpath_results))
            if len(articles) == 0:
                break
            if len(articles) != 10:
                logger.warning(f"Page {page} has {len(articles)} articles")
            for article in articles:
                article_categories = []
                try:
                    a_tag = article.find_element(By.XPATH,".//div[@class='media-body']/h3/a")
                    file_url = a_tag.get_attribute("href")
                    parsed_url = urlparse(file_url)
                    # if path of parsed_url contains any of the ignored paths, skip it
                    if any(ignored_path in parsed_url.path for ignored_path in self.IGNORED_PATHS):
                        logger.debug(f"Skipping ignored path: {file_url}")
                        continue

                except selenium.common.exceptions.NoSuchElementException:
                    logger.warning(f"No href found  at page {page} for article: {article.text}",stack_info=True, exc_info=True)
                    continue

                # each article has multiple content types
                date = None
                date_str = None
                try:
                    date = pd.to_datetime(
                        article.find_element(By.XPATH, ".//div[@class='media-body']/span[contains(concat(' ', normalize-space(@class), ' '), ' media-date ')]").text
                    )
                except selenium.common.exceptions.NoSuchElementException:
                    pass

                if date is None:
                    # we try media-type
                    xpath = ".//div[@class='media-body']/div[@class='media-meta']/span[@class='media-type']"
                    try:
                        date_str = article.find_element(By.XPATH, xpath).text.split(" ")[-1]
                        if "-" not in date_str:
                            # we try to get it from url as last option
                            parsed_file_url = urlparse(file_url)
                            # .com/2025/....
                            date_str = parsed_file_url.path.split("/")[1]
                            # verify if it is 4 digit value
                            if len(date_str) != 4 or not date_str.isdigit():
                                date_str = None
                    except selenium.common.exceptions.NoSuchElementException:
                        pass
                
                
                if date is not None and date > pd.Timestamp.now():
                    logger.debug(f"Skipping future date: {date} at page: {page}")
                    continue
                
                if date is None and date_str is None:
                    logger.warning(f"No date found at page {page} for article: {file_url}")
                    continue

                # get tags, content type, topic
                content_types = []
                topics = []
                try:
                    tags = article.find_element(By.XPATH, ".//div[@class='media-body']/div[@class='media-tags']")
                    content_types = [
                        tag.text for tag in tags.find_elements(By.XPATH, ".//span[@class='content_type taxonomy']/a")
                    ]
                    topics = [
                        tag.text for tag in tags.find_elements(By.XPATH, ".//span[@class='topic taxonomy']/a")
                    ]
                except selenium.common.exceptions.NoSuchElementException:
                    pass
                if "Upcoming events" in content_types:
                    logger.debug(f"Skipping upcoming event: {file_url}")
                    continue
                # TODO categorize based on topics
                article_categories = self.get_categories(content_types, topics)
                # TODO drop Upcoming Events

                if file_url in all_urls:
                    logger.debug(f"Href is already in db: {file_url}")
                    #  drop existing categories from categorization and update the rest
                    article_categories = [
                        {"file_url": file_url, "category_name": category.value}
                        for category in article_categories
                        if (file_url, category.value) not in all_categories
                    ]
                    if len(article_categories) > 0:
                        self.add_to_categories(article_categories) 
                    continue

                to_process.append((date, date_str, file_url, article_categories))
            # process the page
            if len(to_process) > 0:
                self.process_to_process(to_process)
            page += 1

    def process_to_process(self, to_process):
        wait = WebDriverWait(self.driver_manager.driver, 30)
        for date, date_str, file_url, article_categories in to_process:
            year = str(date.year) if date is not None else None
            # if year is none we will use date str
            if year is None and date_str is not None:
                year = date_str.split("-")[0]
            if year is None:
                logger.error(f"No year found for file_url: {file_url}")
                continue
            logger.info(f"Processing: {file_url}")
            total_categories = [
                {"file_url": file_url, "category_name": category.value}
                for category in article_categories
            ]
            urlType, extension = self.clasify_url(file_url)
            extType = classify_extension(extension)
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(file_url, extension, year=str(year))
                if main_id is None:
                    continue
                result = {
                    "file_url": file_url,
                    "date_published": date,
                    "date_published_str": date_str,
                    "scraping_time": pd.Timestamp.now(),
                    "file_id": main_id,
                }
                total_links = []
            elif extType == ExtensionType.WEBPAGE and urlType == URLType.EXTERNAL:
                continue
            elif extType == ExtensionType.WEBPAGE and urlType == URLType.INTERNAL:
                self.get(file_url)
                main_id = self.process_html_page(year)
                result = {
                    "file_url": file_url,
                    "date_published": date,
                    "date_published_str": date_str,
                    "scraping_time": pd.Timestamp.now(),
                    "file_id": main_id,
                }

                def get_links():
                    try:
                        main = wait.until(EC.presence_of_element_located((By.XPATH, "//main[@id='main-content']")))
                    except selenium.common.exceptions.TimeoutException:
                        return []
                    links = []
                    for link in main.find_elements(By.XPATH, ".//a"):
                        try:
                            link_url = link.get_attribute("href")
                            link_text = link.text
                            if link_url is None:
                                continue
                            # filter search links
                            parsed_link = urlparse(link_url)
                            if any(ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS):
                                continue

                            links.append((link_text, link_url))
                        except selenium.common.exceptions.StaleElementReferenceException:
                            pass
                    return links
                links_output = self.process_links(
                    get_links,
                    year=str(year),
                )
                total_links = [
                    {
                        "file_url": file_url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else:
                logger.error(f"Unknown extension type: {extension} for file_url: {file_url}")
                continue

            self.add_all_atomic([result],total_categories,total_links)        
            

        

    def get_url_search(self, page: int, year_from=2000) -> str:
        return f"https://www.bankofcanada.ca/search/?espage={page}&mtf_date_after={year_from}-01-01"
    

    def get_categories(self, content_types: list[str], topics: list[str]) -> Set[Categories]:
        output = set()
        # content type mapping
        for content_typpe in content_types:
            for content_type_list, categories in CONTENT_TYPE_CATEGORY_MAPPING:
                if content_typpe in content_type_list:
                    output.update(categories)
        # topic mapping
        for topic in topics:
            for topic_list, categories in TOPIC_CATEGOTY_MAPPING:
                if topic in topic_list:
                    output.update(categories)
        return output



# List of tuples: (tuple of content type strings, set of Categories)
CONTENT_TYPE_CATEGORY_MAPPING: list[tuple[tuple[str, ...], set[Categories]]] = [
    (("Annual Report", "Bank of Canada Review", "Bank of Canada Review articles", "Quarterly Financial Report", "Souvenir books", "Publications"), {Categories.INSTITUTIONAL_AND_GOVERNANCE}),
    (("Collateral Policy", "Monetary Policy Report", "Opening statements", "Summary of deliberations"), {Categories.MONETARY_POLICY}),
    (("Collateral Policy", "Disclosure of Climate-Related Risks", "Financial Stability Report", "Financial System Hub articles", "Financial System Review articles", "Financial System Survey", "Supervisory guidelines", "Supervisory policies", "Retail payments supervision materials"), {Categories.FINANCIAL_STABILITY_AND_REGULATION}),
    (("Banking and financial statistics", "Books and monographs", "Business Outlook Survey", "Canadian Survey of Consumer Expectations",
      "Conferences and workshops", "Lectures", "Market Participants Survey", "Historical: Banking and Financial Statistics",
      "Historical: Securities and loans", "Historical: Weekly Financial Statistics", "Research newsletters", "Staff analytical notes",
      "Staff discussion papers", "Staff research", "Staff working papers", "Technical reports"), {Categories.RESEARCH_AND_DATA}),
    (("Market notices", "Retail payments supervision materials"), {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS}),
    #((), {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS}),  # No explicit types for currency found.
    (("Announcements", "Conferences and workshops", "Comments", "Market notices", "Media activities", "Media advisories",
      "Meetings", "Opening statements", "Presentations", "Press", "Press releases", "Remarks", "Speech summaries",
      "Speeches and appearances", "Webcasts", "Upcoming events"), {Categories.NEWS_AND_EVENTS}),
    (("Background materials", "Explainers", "Case scenarios", "Other", "The Economy, Plain and Simple","Senior Loan Officer Survey"), {Categories.OTHER}),
]


# List of tuples: (tuple of topic strings, set of Categories)
TOPIC_CATEGOTY_MAPPING: list[tuple[tuple[str, ...], Set[Categories]]] = [
    (("Credibility", "Fiscal policy", "Registration", "Financial services", "Reporting"), {Categories.INSTITUTIONAL_AND_GOVERNANCE}),
    (("Debt management", "Exchange rate regimes", "Monetary aggregates", "Monetary conditions index", "Monetary policy", "Monetary policy and uncertainty",
      "Monetary policy communications", "Monetary policy framework", "Monetary policy implementation", "Monetary policy transmission", "Price stability",
      "Interest rates", "Lender of last resort", "Foreign reserves management"), {Categories.MONETARY_POLICY}),
    (("Credit risk management", "Cyber defenses", "Cyber security", "Enforcement", "Supervision", "Financial stability",
      "Financial system regulation and policies", "Retail payments supervision"), {Categories.FINANCIAL_STABILITY_AND_REGULATION}),
    (("Asset pricing", "Balance of payments and components", "Business fluctuations and cycles", "Central bank research",
      "Credit and credit aggregates", "Development economics", "Domestic demand and components", "Econometric and statistical methods",
      "Economic models", "Exchange rates", "Expectations", "Financial institutions", "Financial markets", "Firm dynamics", "Housing",
      "Inflation and prices", "Inflation targets", "Inflation: costs and benefits", "Interest rates", "International financial markets",
      "International topics", "Labour markets", "Market structure and pricing", "Monetary and financial indicators", "Potential output",
      "Productivity", "Recent economic and financial developments", "Regional economic developments", "Sectoral balance sheet", "Service sector",
      "Trade integration", "Wholesale funding"), {Categories.RESEARCH_AND_DATA}),
    (("Payment clearing and settlement systems", "Digital currencies and fintech"), {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS}),
    (("Bank notes", "Cryptoassets", "Cryptocurrencies"), {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS}),
    (("Accessibility", "Digitalization", "Coronavirus disease (COVID-19)", "Holding funds","Climate change"), {Categories.OTHER}),
]