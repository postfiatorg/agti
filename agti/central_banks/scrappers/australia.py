
import logging
from typing import DefaultDict
from urllib.parse import urlparse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import urllib
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf
from sqlalchemy import text


__all__ = ["AustraliaBankScrapper"]


logger = logging.getLogger(__name__)





class AustraliaBankScrapper(BaseBankScraper):
    """
    We decided to not convert timestamp from CET to EST, becasue ECB provides just date without time.
    and the date will be the same in both timezones.
    
    In summary, the Decisions focus on immediate outcomes, while the Minutes provide a deeper context behind the decisions.
    That is why we fetch the minutes only.

    """
    COUNTRY_CODE_ALPHA_3 = "AUS"
    COUNTRY_NAME = "Australia"
    NETLOC = "www.rba.gov.au"

    def initialize_cookies(self, go_to_url=False):
        # we do not need to accept cookies here
        if go_to_url:
            self.get(f"https://{self.NETLOC}/")
        self.cookies = self.driver_manager.driver.get_cookies()



    # Monetary Policy link
    def process_monetary_policy(self):
        ## Agreement on Framework
        logger.info("Processing Monetary Policy Framework")
        all_urls = self.get_all_db_urls()
        self.get("https://www.rba.gov.au/monetary-policy/framework/")
        # we need to prase the date from the link
        xpath = "//div[@id='content']/ul/li/a"
        links = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = [
            link.get_attribute("href") for link in links
        ]
        result = []
        total_links = []
        total_categories = []
        for url in to_process:
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            self.get(url)
            time_tag = self.driver_manager.driver.find_element(By.XPATH, "//time")
            date = pd.to_datetime(time_tag.text)
            main_content = self.driver_manager.driver.find_element(By.XPATH, "//div[@id='content']")
            text = main_content.text
            # all links
            links_output = self.process_links(url, main_content)
            total_links.extend(links_output)
            result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": text,
            })
            # categories
            total_categories.append(
                {
                    "file_url": url,
                    "category_name": Categories.MONETARY_POLICY.value
                }
            )
        self.add_all_atomic(result, total_categories, total_links)
        
        
        ## Monetary Policy Decision
        logger.info("Processing Monetary Policy Decision")
        current_year = pd.Timestamp.now().year
        for year in range(1990, current_year + 1):
            self.process_list_by_year(year, "https://www.rba.gov.au/monetary-policy/int-rate-decisions/{}", [Categories.MONETARY_POLICY, Categories.NEWS_AND_EVENTS])

        ## Media Conferences
        logger.info("Processing Media Conferences")
        current_year = pd.Timestamp.now().year
        for year in range(2024, current_year + 1):
            self.process_list_by_year(year, "https://www.rba.gov.au/monetary-policy/media-conferences/{}", [Categories.MONETARY_POLICY, Categories.NEWS_AND_EVENTS])

        ## Minutes of Monetary Policy Minutes
        logger.info("Processing Minutes of Monetary Policy")
        current_year = pd.Timestamp.now().year
        for year in range(2006, current_year + 1):
            self.process_list_by_year(year, "https://www.rba.gov.au/monetary-policy/rba-board-minutes/{}", [Categories.MONETARY_POLICY])

        ## Statement on Monetary Policy - (also under publications, statement on monetary policy)
        # done under publications


    #  Payments & Infrastructure
    def process_payments_infrastructure(self):
        all_urls = self.get_all_db_urls()
        ################################
        ## central bank digital currency

        ### speeches
        self.get("https://www.rba.gov.au/payments-and-infrastructure/central-bank-digital-currency/speeches.html")
        # xpath id list-speeches/ div with class containing cbdc
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'cbdc')]"
        speeches = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        for speech in speeches:
            url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
            # ul/li/a
            links = speech.find_elements(By.XPATH, ".//ul/li/a")
            founded_links = []
            for link in links:
                link_url = link.get_attribute("href")
                founded_links.append((link_url, link.text))
            to_process.append((date, url, founded_links))
        result = []
        total_links = []
        total_categories = []
        for date, url, temp_links in to_process:
            logger.info(f"Processing: {url}")
            text, links_output = self.parse_html(url)
            links_href = [x["link_url"] for x in links_output]
            for (main_link, main_link_text) in temp_links:
                if main_link not in links_href:
                    parsed_link = urlparse(main_link)
                    # if it is pdf
                    extracted_text = None
                    if parsed_link.path.endswith("pdf"):
                        extracted_text = download_and_read_pdf(main_link,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                    links_output.append({
                        "file_url": url,
                        "link_url": main_link,
                        "link_name": main_link_text,
                        "full_extracted_text": extracted_text,
                    })
            total_links.extend(links_output)
                    
            result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": text,
            })
            # categories
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)

        ### publications
        self.get("https://www.rba.gov.au/payments-and-infrastructure/central-bank-digital-currency/publications.html")
        xpath = "//div[@id='content']/div[@class='list-articles']/div[@class='item']"
        publications = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        for publication in publications:
            url = publication.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date_txt = publication.find_element(By.XPATH, ".//span[@class='date']/span[@class='date']").text
            to_process.append((date_txt, url))

        result = []
        total_links = []
        total_categories = []
        for date_txt, url in to_process:
            logger.info(f"Processing: {url}")
            date = None
            date_txt = date_txt.strip()
            # if date start with number it has day otherwise it has month and year only
            if date_txt[0].isdigit():
                date = pd.to_datetime(date_txt)
                date_txt = None
            url_parsed = urlparse(url)
            extracted_text = None
            if url_parsed.path.endswith("pdf"):
                extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            else:
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
            result.append({
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)

        ########################################
        ## Resources

        ### Media Releases
        self.get("https://www.rba.gov.au/payments-and-infrastructure/resources/media-releases.html")
        xpath = "//div[@id='content']/p"
        media_releases = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        for media_release in media_releases:
            url = media_release.find_element(By.XPATH, ".//a").get_attribute("href")
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date = pd.to_datetime(media_release.text.split("\n")[0].strip())
            to_process.append((date, url))


        result = []
        total_links = []
        total_categories = []
        for (date, url) in to_process:
            logger.info(f"Processing: {url}")
            extracted_text, links_output = self.parse_html(url)
            total_links.extend(links_output)
            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.NEWS_AND_EVENTS.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)


        ### Speeches
        self.get("https://www.rba.gov.au/payments-and-infrastructure/resources/speeches.html")
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'fs')]"
        speeches = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        for speech in speeches:
            try:
                url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            except NoSuchElementException:
                logger.warning(f"No href found for speech: {speech.text} for Payments and Infrastructure - Resources - Speeches")
                continue
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
            # ul/li/a
            links = speech.find_elements(By.XPATH, ".//ul/li/a")
            founded_links = []
            for link in links:
                link_url = link.get_attribute("href")
                founded_links.append((link_url, link.text))
            to_process.append((date, url, founded_links))
        result = []
        total_links = []
        total_categories = []
        for date, url, temp_links in to_process:
            logger.info(f"Processing: {url}")
            text, links_output = self.parse_html(url)
            links_href = [x["link_url"] for x in links_output]
            for (main_link, main_link_text) in temp_links:
                if main_link not in links_href:
                    parsed_link = urlparse(main_link)
                    # if it is pdf
                    extracted_text = None
                    if parsed_link.path.endswith("pdf"):
                        extracted_text = download_and_read_pdf(main_link,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                    links_output.append({
                        "file_url": url,
                        "link_url": main_link,
                        "link_name": main_link_text,
                        "full_extracted_text": extracted_text,
                    })
            total_links.extend(links_output)
                    
            result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": text,
            })
            # categories
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.NEWS_AND_EVENTS.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)

        ### Publications
        self.get("https://www.rba.gov.au/payments-and-infrastructure/resources/publications/")
        xpath = "//div[@id='content']/div[@class='list-articles']/div[@class='item']"
        publications = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        for publication in publications:
            url = publication.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date_txt = publication.find_element(By.XPATH, ".//time").get_attribute("datetime")
            to_process.append((date_txt, url))
        
        result = []
        total_links = []
        total_categories = []
        for date_txt, url in to_process:
            logger.info(f"Processing: {url}")
            date = None
            date_txt = date_txt.strip()
            # if date has just one "-" it is not a date
            if date_txt.count("-") == 2:
                date = pd.to_datetime(date_txt)
                date_txt = None
            url_parsed = urlparse(url)
            extracted_text = None
            if url_parsed.path.endswith("pdf"):
                extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            else:
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
            result.append({
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)


    # Financial Stability
    def process_financial_stability(self):
        all_urls = self.get_all_db_urls()
        # Financial Stability reviews
        # skip becuase it is under publications

        # Publications
        self.get("https://www.rba.gov.au/fin-stability/resources/publications.html")

        to_process = []
        xpath = "//div[@class='box-table']"
        box_tables = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        for box_table in box_tables:
            trs = box_table.find_elements(By.XPATH, ".//tr")
            years = [x.text for x in trs[0].find_elements(By.XPATH, ".//td")]
            links_tds = trs[1].find_elements(By.XPATH, ".//td")
            for year, td_link in zip(years, links_tds):
                a_links = td_link.find_elements(By.XPATH, ".//a")
                for a in a_links:
                    url = a.get_attribute("href")
                    if url in all_urls:
                        logger.debug(f"Href is already in db: {url}")
                        continue
                    to_process.append((
                        f"{a.text} {year}",
                        url
                    ))
        # list articles
        xpath = "//div[@class='list-articles']/div[@class='item']"
        for article in self.driver_manager.driver.find_elements(By.XPATH, xpath):
            url = article.find_element(By.XPATH, ".//h4//a").get_attribute("href")
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            try:
                date_txt =  article.find_element(By.XPATH, ".//p[@class='date']").text
            except NoSuchElementException:
                date_txt = None
            to_process.append((date_txt, url))

        result = []
        total_links = []
        total_categories = []
        for date_txt, url in to_process:
            logger.info(f"Processing: {url}")
            date = None
            if date_txt is not None:
                date_txt = date_txt.strip()
                # if date has just one "-" it is not a date
                if date_txt[0].isdigit():
                    date = pd.to_datetime(date_txt)
                    date_txt = None
            url_parsed = urlparse(url)
            extracted_text = None
            if url_parsed.path.endswith("pdf"):
                extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            else:
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
            result.append({
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)
            
        # Speeches
        self.get("https://www.rba.gov.au/fin-stability/resources/speeches.html")
        to_process = []
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'py')]"
        speeches = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        for speech in speeches:
            try:
                url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            except NoSuchElementException:
                logger.warning(f"No href found for speech: {speech.text} for Payments and Infrastructure - Resources - Speeches")
                continue
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
            # ul/li/a
            links = speech.find_elements(By.XPATH, ".//ul/li/a")
            founded_links = []
            for link in links:
                link_url = link.get_attribute("href")
                founded_links.append((link_url, link.text))
            to_process.append((date, url, founded_links))
        result = []
        total_links = []
        total_categories = []
        for date, url, temp_links in to_process:
            logger.info(f"Processing: {url}")
            text, links_output = self.parse_html(url)
            links_href = [x["link_url"] for x in links_output]
            for (main_link, main_link_text) in temp_links:
                if main_link not in links_href:
                    parsed_link = urlparse(main_link)
                    # if it is pdf
                    extracted_text = None
                    if parsed_link.path.endswith("pdf"):
                        extracted_text = download_and_read_pdf(main_link,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                    links_output.append({
                        "file_url": url,
                        "link_url": main_link,
                        "link_name": main_link_text,
                        "full_extracted_text": extracted_text,
                    })
            total_links.extend(links_output)
                    
            result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": text,
            })
            # categories
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": Categories.NEWS_AND_EVENTS.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)


    def processing_media_releases(self):
        media_releases_url = "https://www.rba.gov.au/media-releases/{}/"
        current_year = pd.Timestamp.now().year
        all_urls = self.get_all_db_urls()
        for year in range(1988, current_year + 1):
            self.get(media_releases_url.format(year))
            to_process = []
            xpath = "//ul[contains(@class, 'list-articles')]/li"
            articles = self.driver_manager.driver.find_elements(By.XPATH, xpath)
            for article in articles:
                article_url = article.find_element(By.XPATH, ".//a").get_attribute("href")
                if article_url in all_urls:
                    logger.debug(f"Href is already in db: {article_url}")
                    continue
                date = pd.to_datetime(article.find_element(By.XPATH, ".//time").get_attribute("datetime"))
                to_process.append((date, article_url))
            
            result = []
            total_links = []
            total_categories = []
            for date, article_url in to_process:
                logger.info(f"Processing: {article_url}")
                extracted_text, links_output = self.parse_html(article_url)
                total_links.extend(links_output)
                result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": article_url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.extend([
                    {
                        "file_url": article_url,
                        "category_name": Categories.NEWS_AND_EVENTS.value
                    }
                ])
            self.add_all_atomic(result, total_categories, total_links)

    def processing_speeches(self):
        main_releases_url = "https://www.rba.gov.au/speeches/{}/"
        current_year = pd.Timestamp.now().year
        all_urls = self.get_all_db_urls()
        xpath_speeches = "//div[contains(@class, 'list-speeches')]/div[contains(@class, 'item')]"
        for year in range(1990, current_year + 1):
            self.get(main_releases_url.format(year))
            speeches = self.driver_manager.driver.find_elements(By.XPATH, xpath_speeches)
            to_process = []
            for speech in speeches:
                try:
                    url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
                except NoSuchElementException:
                    logger.warning(f"No href found for speech: {speech.text} for Payments and Infrastructure - Resources - Speeches")
                    continue
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
                # ul/li/a
                links = speech.find_elements(By.XPATH, ".//ul/li/a")
                founded_links = []
                for link in links:
                    link_url = link.get_attribute("href")
                    founded_links.append((link_url, link.text))
                to_process.append((date, url, founded_links))
            
            result = []
            total_links = []
            total_categories = []
            for date, url, temp_links in to_process:
                logger.info(f"Processing: {url}")
                text, links_output = self.parse_html(url)
                links_href = [x["link_url"] for x in links_output]
                for (main_link, main_link_text) in temp_links:
                    if main_link not in links_href:
                        parsed_link = urlparse(main_link)
                        # if it is pdf
                        extracted_text = None
                        if parsed_link.path.endswith("pdf"):
                            extracted_text = download_and_read_pdf(main_link,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                        links_output.append({
                            "file_url": url,
                            "link_url": main_link,
                            "link_name": main_link_text,
                            "full_extracted_text": extracted_text,
                        })
                total_links.extend(links_output)
                        
                result.append({
                        "date_published": date,
                        "scraping_time": pd.Timestamp.now(),
                        "file_url": url,
                        "full_extracted_text": text,
                })
                # categories
                total_categories.extend([
                    {
                        "file_url": url,
                        "category_name": Categories.NEWS_AND_EVENTS.value
                    }
                ])
            self.add_all_atomic(result, total_categories, total_links)
            
    def process_publications(self):
        all_urls = self.get_all_db_urls()
        # Publications
        ## statement on MP
        
        logger.info("Processing Statement on Monetary Policy")
        main_url = "https://www.rba.gov.au/publications/smp/{}/"
        current_year = pd.Timestamp.now().year
        for year in range(2005,current_year + 1):
            self.get(main_url.format(year))
            to_process = []
            xpath = "//div[@id='content']/section/ul/li"
            data_list = self.driver_manager.driver.find_elements(By.XPATH, xpath)
            for li in data_list:
                a_tag = li.find_element(By.XPATH, ".//a")
                url = a_tag.get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date_txt = a_tag.text
                if "Boxes" in date_txt:
                    continue
                to_process.append((date_txt, url))
            result = []
            total_links = []
            total_categories = []
            for (date_txt,url) in to_process:
                logger.info(f"Processing: {url}")
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
                result.append({
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.extend([
                    {
                        "file_url": url,
                        "category_name": Categories.MONETARY_POLICY.value
                    },
                    {
                        "file_url": url,
                        "category_name": Categories.RESEARCH_AND_DATA.value
                    }
                ])
            self.add_all_atomic(result, total_categories, total_links)
                


        ## financial stability review
        logger.info("Processing Financial Stability Review")
        main_url = "https://www.rba.gov.au/publications/fsr/{}/"
        current_year = pd.Timestamp.now().year
        for year in range(2005,current_year + 1):
            self.get(main_url.format(year))
            to_process = []
            xpath = "//div[@id='content']/section/ul/li"
            data_list = self.driver_manager.driver.find_elements(By.XPATH, xpath)
            for li in data_list:
                a_tag = li.find_element(By.XPATH, ".//a")
                url = a_tag.get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date_txt = a_tag.text
                if "Boxes" in date_txt:
                    continue
                to_process.append((date_txt, url))
            result = []
            total_links = []
            total_categories = []
            for (date_txt,url) in to_process:
                logger.info(f"Processing: {url}")
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
                result.append({
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.extend([
                    {
                        "file_url": url,
                        "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                    }
                ])
            self.add_all_atomic(result, total_categories, total_links)
        
        ## Bulletin
        logger.info("Processing Bulletin")
        current_year = pd.Timestamp.now().year
        main_url = "https://www.rba.gov.au/publications/bulletin/{}/"
        
        categories = set()
        # 2015 -> forward
        for year in range(2015, current_year + 1):
            self.get(main_url.format(year))
            month_xpath = "//div[@class='item rss-bulletin-item']/div[@class='contents']"
            data_list = self.driver_manager.driver.find_elements(By.XPATH, month_xpath)
            to_process = []
            for div in data_list:
                a_tag = div.find_element(By.XPATH, ".//h3//a")
                url = a_tag.get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date = pd.to_datetime(div.find_element(By.XPATH, ".//time").get_attribute("datetime"))
                try:
                    categories = map_bulletin_category(div.find_element(By.XPATH, ".//span[@class='category']").text)
                except NoSuchElementException:
                    categories = sum([
                        map_bulletin_category(a.get_attribute("data-tag-id")) for a in div.find_elements(By.XPATH, ".//ul[@class='tags']//a")
                    ],[])
                to_process.append((date, url, categories))
        
            result = []
            total_links = []
            total_categories = []
            for date, url, categories in to_process:
                logger.info(f"Processing: {url}")
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
                result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.extend([
                    {
                        "file_url": url,
                        "category_name": category.value
                    } for category in categories
                ])
            self.add_all_atomic(result, total_categories, total_links)
        
        
        # 2010 -> 2014
        for year in range(2010, 2015):
            self.get(main_url.format(year))
            month_xpath = "//div[@id='content']//section//li/a"
            data_list = self.driver_manager.driver.find_elements(By.XPATH, month_xpath)
            print(len(data_list))
            to_process = []
            for a_tag in data_list:
                url = a_tag.get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date_txt = a_tag.text
                to_process.append((date_txt, url))
            result = []
            total_links = []
            total_categories = []
            for date_txt, url in to_process:
                logger.info(f"Processing: {url}")
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
                result.append({
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.append({
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                })
            self.add_all_atomic(result, total_categories, total_links)
        
            
        # 1985 -> 2009
        for year in range(1985, 2010):
            self.get(main_url.format(year))
            month_xpath = "//div[@id='content']//h2"
            ul_xpath = "//div[@id='content']//ul"
            data_list = self.driver_manager.driver.find_elements(By.XPATH, ul_xpath)
            months_names = self.driver_manager.driver.find_elements(By.XPATH, month_xpath)
            to_process = []
            for (month_h2, ul) in zip(months_names, data_list):
                month = month_h2.text
                for a_tag in ul.find_elements(By.XPATH, ".//li/div[@class='title']/a"):
                    url = a_tag.get_attribute("href")
                    if url in all_urls:
                        logger.debug(f"Href is already in db: {url}")
                        continue
                    date_txt = f"{month} {year}"
                to_process.append((date_txt, url))

            result = []
            total_links = []
            total_categories = []
            for date_txt, url in to_process:
                logger.info(f"Processing: {url}")
                extracted_text, links_output = self.parse_html(url)
                total_links.extend(links_output)
                result.append({
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.append({
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                })
            self.add_all_atomic(result, total_categories, total_links)
        
        # Reserach
        ## Research Discussion Papers
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        logging.info("Processing Research")
        main_url="https://www.rba.gov.au/publications/rdp/about.html?pp=100&p={}"
        to_process = []
        page_number = 1
        wait = WebDriverWait(self.driver_manager.driver, 10,0.1)
        while True:
            self.get(main_url.format(page_number))
            # get id "resultsInfo" and waits until text is laoded
            wait.until(lambda driver: driver.find_element(By.ID, "resultsInfo").text != "")
            search_result = self.driver_manager.driver.find_element(By.ID, "resultsInfo")
            if "No RDPs found" in search_result.text:
                break
            results = self.driver_manager.driver.find_elements(By.XPATH, "//div[@id='search-results-list']/div[@class='item']/div")
            for result in results:
                a_tag = result.find_element(By.XPATH, ".//h2//a")
                url = a_tag.get_attribute("href")
                categories = sum([
                        map_bulletin_category(a.get_attribute("data-tag-id")) for a in result.find_elements(By.XPATH, ".//ul[@class='tags']//a")
                    ],[])
                # filter out categories that are not in the db
                categories = [x for x in categories if (url, x.value) not in all_categories]
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    if len(categories) != 0:
                        self.add_to_categories([
                            {
                                "file_url": url,
                                "category_name": category.value
                            } for category in categories
                        ])
                    continue
                date_txt = result.find_element(By.XPATH, ".//div[@class='info']/span[@class='date']").text
                to_process.append((date_txt, url, categories))
            page_number += 1
        result = []
        total_links = []
        total_categories = []
        for date_txt, url, categories in to_process:
            logger.info(f"Processing: {url}")
            extracted_text, links_output = self.parse_html(url)
            total_links.extend(links_output)
            date = None
            date_txt = date_txt.strip()
            if date_txt[0].isdigit():
                date = pd.to_datetime(date_txt)
                date_txt = None
            result.append({
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.extend([
                {
                    "file_url": url,
                    "category_name": category.value
                } for category in categories
            ])
        self.add_all_atomic(result, total_categories, total_links)

        
        ## Conferences
        logger.info("Processing Conferences")
        conf_url = "https://www.rba.gov.au/publications/confs/{}/"
        to_process = []
        to_process_links = {}
        # define function to process recursive lis
        def process_lis(ul):
            for li in ul.find_elements(By.XPATH, ".//li"):
                # check if li contains class 'sub'
                if "without-links" in li.get_attribute("class"):
                    ul = li.find_elements(By.XPATH, ".//ul")
                    if len(ul) == 1:
                        process_lis(ul[0])
                    continue
                # we need to find new current url
                a_tags = li.find_elements(By.XPATH, ".//div[@class='title']//a")
                links = li.find_elements(By.XPATH, ".//div[@class='links']//a")
                if len(a_tags) > 0:
                    current_url = a_tags[0].get_attribute("href")
                    # get all links
                    for link in li.find_elements(By.XPATH, ".//div[@class='links']//a"):
                        url = link.get_attribute("href")
                        if current_url not in to_process_links:
                            to_process_links[current_url] = []
                        to_process_links[current_url].append((url, link.text))
                    
                else:
                    if len(links) == 0:
                        continue
                    current_url = links[0].get_attribute("href")
                if current_url in all_urls:
                    logger.debug(f"Href is already in db: {current_url}")
                    return
                to_process.append((date, current_url))

                

        
        
        current_year = pd.Timestamp.now().year
        for year in range(1989, current_year + 1):
            self.get(conf_url.format(year))
            xpath = "//div[@id='content']/section/ul"
            try:
                time_txt = self.driver_manager.driver.find_element(By.XPATH, "//time").get_attribute("datetime")
            except NoSuchElementException:
                try:
                    time_txt = self.driver_manager.driver.find_element(By.XPATH, "//div[@id='content']/section/h1/span[@class='page-subtitle']").text
                    if "–" in time_txt:
                        time_txt = time_txt.split("–")[1].strip()
                except NoSuchElementException:
                    continue
            date = pd.to_datetime(time_txt)
            for ul in self.driver_manager.driver.find_elements(By.XPATH, xpath):
                process_lis(ul)

                
        result = []
        total_links = []
        total_categories = []
        for date, url in to_process:
            logger.info(f"Processing: {url}")
            url_parsed = urlparse(url)
            links_output = []
            if url_parsed.path.endswith("pdf"):
                extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            else:
                extracted_text, links_output = self.parse_html(url)
            if url in to_process_links:
                for link_url, link_text in to_process_links[url]:
                    parsed_link = urlparse(link_url)
                    # if it is pdf
                    extracted_text = None
                    if parsed_link.path.endswith("pdf"):
                        extracted_text = download_and_read_pdf(link_url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                    links_output.append({
                        "file_url": url,
                        "link_url": link_url,
                        "link_name": link_text,
                        "full_extracted_text": extracted_text,
                    })
            total_links.extend(links_output)
            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.append({
                "file_url": url,
                "category_name": Categories.RESEARCH_AND_DATA.value
            })
        self.add_all_atomic(result, total_categories, total_links)
        
        ## Workshops
        worskshop_url = "https://www.rba.gov.au/publications/workshops/research/{}/"
        to_process = []
        current_year = pd.Timestamp.now().year
        for year in range(2007, current_year + 1):
            self.get(worskshop_url.format(year))
            try:
                time_txt = self.driver_manager.driver.find_element(By.XPATH, "//div[@id='content']/h1/span[@class='page-subtitle']").text
            except NoSuchElementException:
                continue
            if "–" in time_txt:
                time_txt = time_txt.split("–")[1].strip()
            date = pd.to_datetime(time_txt)
            ul = self.driver_manager.driver.find_element(By.XPATH, "//div[@id='content']/ul")
            for li in ul.find_elements(By.XPATH, ".//li"):
                a_tags = li.find_elements(By.XPATH, ".//a")
                if len(a_tags) == 0:
                    continue

                url = a_tags[0].get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                to_process.append((date, url))
        result = []
        total_links = []
        for date, url in to_process:
            logger.info(f"Processing: {url}")
            url_parsed = urlparse(url)
            links_output = []
            if url_parsed.path.endswith("pdf"):
                extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            else:
                extracted_text, links_output = self.parse_html(url)
            total_links.extend(links_output)
            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
        self.add_all_atomic(result, [], total_links)



        
        # Reporting and Planning
        ## RBA Annual Report
        rba_ar_url = "https://www.rba.gov.au/publications/annual-reports/rba/{}/"
        current_year = pd.Timestamp.now().year
        result = []
        total_links = []
        total_categories = []
        for year in range(1960, current_year + 1):
            date_txt = f"{year}"
            url = rba_ar_url.format(year)
            if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
            self.get(url)
            # find a tag with "report" or "Annual Report" text 
            a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//div[@id='content']//a[text()='Report' or text()='report' or contains(text(),'Annual Report')]")
            if len(a_tags) > 0:
                url = a_tags[0].get_attribute("href")
                url_parsed = urlparse(url)
                if not url_parsed.path.endswith("pdf"):
                    logger.warning(f"Annual Report is not pdf: {url}")
                    continue
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                logger.info(f"Processing: {url}")
                extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                result.append({
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
            else:
                try:
                    ul = self.driver_manager.driver.find_element(By.XPATH, "//ul[@class='list-contents']")
                except NoSuchElementException:
                    logger.warning(f"No data found for year: {year} and url: {rba_ar_url.format(year)}")
                    continue
                logger.info(f"Processing: {url}")
                extracted_text = ul.text
                links_to_process = []
                for a_tag in ul.find_elements(By.XPATH, ".//a"):
                    a_url = a_tag.get_attribute("href")
                    links_to_process.append((a_url, a_tag.text))
                result.append({
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": url,
                    "full_extracted_text": extracted_text,
                })
                total_categories.append({
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                })
                for a_url, a_text in links_to_process:
                    a_url_parsed = urlparse(a_url)
                    if a_url_parsed.path.endswith("pdf"):
                        link_extracted_text = download_and_read_pdf(a_url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
                    else:
                        link_extracted_text, _ = self.parse_html(a_url)
                    #total_links.extend(link_total_sublinks)
                    total_links.append({
                        "file_url": url,
                        "link_url": a_url,
                        "link_name": a_text,
                        "full_extracted_text": link_extracted_text,
                    })
        self.add_all_atomic(result, total_categories, total_links)



        
        ## PSB Annual Report
        psb_ar_url = "https://www.rba.gov.au/publications/annual-reports/psb/{}/"
        current_year = pd.Timestamp.now().year
        result = []
        total_categories = []
        for year in range(1999,current_year + 1):
            self.get(psb_ar_url.format(year))
            a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//div[@id='content']//a[text()='Report' or text()='report']")
            if len(a_tags) == 0:
                self.get(psb_ar_url.format(year) + '/contents.html')
                a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//div[@id='content']//a[text()='Report' or text()='report']")
            if len(a_tags) == 0:
                logger.warning(f"No PSB Annual Report found for year: {year}")
                continue
            url = a_tags[0].get_attribute("href")
            parsed_url = urlparse(url)
            if not parsed_url.path.endswith("pdf"):
                logger.warning(f"PSB Annual Report is not pdf: {url}")
                continue
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            logger.info(f"Processing: {url}")
            extracted_text = download_and_read_pdf(url,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            result.append({
                "date_published": None,
                "date_published_str": f"{year}",
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": extracted_text,
            })
            total_categories.append({
                "file_url": url,
                "category_name": Categories.RESEARCH_AND_DATA.value
            })
            total_categories.append({
                "file_url": url,
                "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value
            })
        self.add_all_atomic(result, total_categories, [])
            

    

    def process_links(self, url, html_tag):
        url_parsed = urlparse(url)
        links_output = []
        links = html_tag.find_elements(By.XPATH, ".//a")
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
                link_text = download_and_read_pdf(link_href,self.datadump_directory_path, headers=self.get_headers(), cookies=self.get_cookies_for_request())
            # NOTE add support for different file types
            links_output.append({
                "file_url": url,
                "link_url": link_href,
                "link_name": link.text,
                "full_extracted_text": link_text,
            })
        return links_output




    def parse_html(self, url: str):
        self.get(url)
        xpath = "//main[@id='content' or @id='main'] | //div[@id='content' or @id='main']"
        try:
            content = self.driver_manager.driver.find_element(By.XPATH, xpath)
        except NoSuchElementException:
            logger.warning(f"No content found for url: {url}")
            return "", []
        text = content.text
        # all links
        links_output = self.process_links(url, content)
        return text, links_output
    

    def process_list_by_year(self, year:int, f_url, categories):
        all_urls = self.get_all_db_urls()
        self.get(f_url.format(year))
        # get class "list-articles"
        try:
            ul = self.driver_manager.driver.find_element(By.CLASS_NAME, "list-articles")
        except NoSuchElementException:
            logger.debug(f"No data found for year: {year} and url: {f_url.format(year)}")
            return
        # iterate over all li elements
        to_process = []
        for li in ul.find_elements(By.XPATH, "./*"):
            # find a element
            a = li.find_element(By.XPATH, ".//a")
            href = a.get_attribute("href")
            text = a.text
            date = pd.to_datetime(text)
            if href in all_urls:
                logger.debug(f"Href is already in db: {href}")
                continue

            to_process.append([date, href])
        result = []
        total_links = []
        total_categories = []
        for date, href in to_process:
            logger.info(f"Processing: {href}")
            text, links = self.parse_html(href)
            total_links.extend(links)
            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": href,
                "full_extracted_text": text if len(text) > 0 else None,
            })
            total_categories.extend([
                {
                    "file_url": href,
                    "category_name": category.value
                } for category in categories
            ])

        self.add_all_atomic(result, total_categories, total_links)
            



    def process_all_years(self):
        self.process_monetary_policy()
        self.process_payments_infrastructure()
        self.process_financial_stability()
        self.processing_media_releases()
        self.processing_speeches()
        self.process_publications()
    

    def get_base_url(self) -> str:
        return f"https://www.rba.gov.au/monetary-policy"
    
    def get_base_url_monetary_policy_minutes_year(self, year:int) -> str:
        return f"{self.get_base_url()}/rba-board-minutes/{year}/"
    

    def get_base_url_monetary_policy_decision_year(self, year:int) -> str:
        return f"{self.get_base_url()}/int-rate-decisions/{year}/"
    

def map_bulletin_category(tag: str) -> list[str]:
    return BULLETIN_CATEGORY_MAPPING.get(tag, [Categories.OTHER])


# Mapping of tags to categories
BULLETIN_CATEGORY_MAPPING = {
    'balance+sheet': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.RESEARCH_AND_DATA],
    'securities': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS],
    'money': [Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA],
    'central+clearing': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'cryptocurrency': [Categories.MONETARY_POLICY, Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS],
    'income+and+wealth': [Categories.RESEARCH_AND_DATA],
    'global+economy': [Categories.RESEARCH_AND_DATA],
    'funding': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'business': [Categories.RESEARCH_AND_DATA],
    'china': [Categories.RESEARCH_AND_DATA],
    'households': [Categories.RESEARCH_AND_DATA],
    'COVID-19': [Categories.RESEARCH_AND_DATA],
    'capital': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.RESEARCH_AND_DATA],
    'Global Economy': [Categories.RESEARCH_AND_DATA],
    'export': [Categories.RESEARCH_AND_DATA],
    'Payments': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'climate+change': [Categories.RESEARCH_AND_DATA],
    'consumption': [Categories.RESEARCH_AND_DATA],
    'credit': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'banknotes': [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE],
    'mining': [Categories.RESEARCH_AND_DATA],
    'bonds': [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'education': [Categories.RESEARCH_AND_DATA],
    'financial+stability': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'cash+rate': [Categories.MONETARY_POLICY],
    'wages': [Categories.RESEARCH_AND_DATA],
    'Australian Economy': [Categories.RESEARCH_AND_DATA],
    'resources+sector': [Categories.RESEARCH_AND_DATA],
    'payments': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'currency': [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS],
    'inflation': [Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA],
    'debt': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS],
    'services+sector': [Categories.RESEARCH_AND_DATA],
    'regulation': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'Financial Stability': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'liquidity': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'machine+learning': [Categories.RESEARCH_AND_DATA],
    'history': [Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.RESEARCH_AND_DATA],
    'monetary+policy': [Categories.MONETARY_POLICY],
    'interest+rates': [Categories.MONETARY_POLICY, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'finance': [Categories.RESEARCH_AND_DATA, Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'fees': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'commercial+property': [Categories.RESEARCH_AND_DATA],
    'labour+market': [Categories.RESEARCH_AND_DATA],
    'trade': [Categories.RESEARCH_AND_DATA],
    'Finance': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'forecasting': [Categories.RESEARCH_AND_DATA],
    'risk+and+uncertainty': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.RESEARCH_AND_DATA],
    'retail': [Categories.RESEARCH_AND_DATA],
    'global+financial+crisis': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.RESEARCH_AND_DATA],
    'productivity': [Categories.RESEARCH_AND_DATA],
    'international': [Categories.RESEARCH_AND_DATA],
    'China': [Categories.RESEARCH_AND_DATA],
    'lending+standards': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'banking': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'technology': [Categories.RESEARCH_AND_DATA],
    'financial+markets': [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'rba+survey': [Categories.RESEARCH_AND_DATA],
    'commodities': [Categories.RESEARCH_AND_DATA],
    'saving': [Categories.RESEARCH_AND_DATA],
    'emerging+markets': [Categories.RESEARCH_AND_DATA],
    'asset+quality': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'exchange+rate': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS],
    'housing': [Categories.RESEARCH_AND_DATA],
    'First+Nations': [Categories.RESEARCH_AND_DATA],
    'modelling': [Categories.RESEARCH_AND_DATA],
    'investment': [Categories.RESEARCH_AND_DATA],
    'business+cycle': [Categories.RESEARCH_AND_DATA],
    'digital+currency': [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS, Categories.RESEARCH_AND_DATA],
    'fiscal+policy': [Categories.RESEARCH_AND_DATA,Categories.MONETARY_POLICY],
    'insolvency': [Categories.FINANCIAL_STABILITY_AND_REGULATION],
    'open+economy': [Categories.RESEARCH_AND_DATA],
    'terms+of+trade': [Categories.RESEARCH_AND_DATA],
}
