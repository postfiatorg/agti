
import logging
from typing import Set
from urllib.parse import urlparse
import pandas as pd
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf

__all__ = ["CanadaBankScrapper"]

logger = logging.getLogger(__name__)

class CanadaBankScrapper(BaseBankScraper):

    def process_all_years(self):
        wait = WebDriverWait(self.driver_manager.driver, 30)

        all_urls = self.get_all_db_urls()
        all_categories = self.get_all_db_categories()
        page = 1
        to_process = []
        while True:
            if page % 10 == 0:
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
                except selenium.common.exceptions.NoSuchElementException:
                    logger.warning(f"No href found for article: {article.text}",stack_info=True, exc_info=True)
                    continue

                # each article has multiple content types
                date = None
                try:
                    date = pd.to_datetime(
                        article.find_element(By.XPATH, ".//div[@class='media-body']/span[contains(concat(' ', normalize-space(@class), ' '), ' media-date ')]").text
                    )
                except selenium.common.exceptions.NoSuchElementException:
                    date = None
                

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

                to_process.append((date, file_url, article_categories))
            page += 1

        result = []
        total_categories = []
        total_links = []
        for date, file_url, article_categories in to_process:
            logger.info(f"Processing: {file_url}")
            article_categories = [
                {"file_url": file_url, "category_name": category.value}
                for category in article_categories
            ]
            total_categories.extend(article_categories)
            if urlparse(file_url).path.lower().endswith('.pdf'):
                # Note there can be multiple other pdf files as well on the page
                text = download_and_read_pdf(file_url,self.datadump_directory_path, self)
                result.append({
                    "file_url": file_url,
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text
                })
            else:
                self.get(file_url)
                url_parsed = urlparse(file_url)
                if url_parsed.netloc != self.bank_config.NETLOC:
                    result.append({
                        "file_url": file_url,
                        "date_published": date,
                        "scraping_time": pd.Timestamp.now(),
                        "full_extracted_text": None
                    })
                    continue
                try:
                    main = wait.until(EC.presence_of_element_located((By.XPATH, "//main[@id='main-content']")))
                except selenium.common.exceptions.TimeoutException:
                    logger.warning(f"Timeout for {file_url}")
                    result.append({
                        "file_url": file_url,
                        "date_published": date,
                        "scraping_time": pd.Timestamp.now(),
                        "full_extracted_text": None
                    })
                    continue
                text = main.text
                result.append({
                    "file_url": file_url,
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text
                })
                links = main.find_elements(By.XPATH, ".//a")
                links_data = []
                for temp_link in links:
                    try:
                        link_href = temp_link.get_attribute("href")
                        link_name = temp_link.text
                        links_data.append((link_href, link_name))
                    except selenium.common.exceptions.StaleElementReferenceException:
                        continue

                if len(links_data) != len(links):
                    logger.warning(f"Links length mismatch: Found {len(links)} vs obtained {len(links_data)}")
                
                for link_href, link_name in links_data:
                    if link_href is None:
                        continue
                    link_href_parsed = urlparse(link_href)
                    link_text = None
                    if link_href_parsed.fragment != '':
                        if url_parsed[:3] == link_href_parsed[:3]:
                            # we ignore links to the same page (fragment identifier)
                            continue
                        # NOTE: we do not parse the text yet
                    elif urlparse(link_href).path.lower().endswith('.pdf'):
                        link_text = download_and_read_pdf(link_href,self.datadump_directory_path, self)
                    # NOTE add support for different file types
                    total_links.append({
                        "file_url": file_url,
                        "link_url": link_href,
                        "link_name": link_name,
                        "full_extracted_text": link_text,
                    })

        self.add_all_atomic(result,total_categories,total_links)        
            

        

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