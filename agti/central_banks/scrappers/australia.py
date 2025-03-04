
import logging
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



    # Monetary Policy link
    def process_monetary_policy(self):
        ## Agreement on Framework
        logger.info("Processing Monetary Policy Framework")
        all_urls = self.get_all_db_urls()
        self._driver.get("https://www.rba.gov.au/monetary-policy/framework/")
        # we need to prase the date from the link
        xpath = "//div[@id='content']/ul/li/a"
        links = self._driver.find_elements(By.XPATH, xpath)
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
            self._driver.get(url)
            time_tag = self._driver.find_element(By.XPATH, "//time")
            date = pd.to_datetime(time_tag.text)
            main_content = self._driver.find_element(By.XPATH, "//div[@id='content']")
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
        self._driver.get("https://www.rba.gov.au/payments-and-infrastructure/central-bank-digital-currency/speeches.html")
        # xpath id list-speeches/ div with class containing cbdc
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'cbdc')]"
        speeches = self._driver.find_elements(By.XPATH, xpath)
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
                        extracted_text = download_and_read_pdf(main_link, self.datadump_directory_path)
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
                    "category_name": Categories.MONETARY_POLICY.value
                },
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
        self._driver.get("https://www.rba.gov.au/payments-and-infrastructure/central-bank-digital-currency/publications.html")
        xpath = "//div[@id='content']/div[@class='list-articles']/div[@class='item']"
        publications = self._driver.find_elements(By.XPATH, xpath)
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
                extracted_text = download_and_read_pdf(url, self.datadump_directory_path)
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
                    "category_name": Categories.MONETARY_POLICY.value
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

        ########################################
        ## Resources

        ### Media Releases
        self._driver.get("https://www.rba.gov.au/payments-and-infrastructure/resources/media-releases.html")
        xpath = "//div[@id='content']/p"
        media_releases = self._driver.find_elements(By.XPATH, xpath)
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
                    "category_name": Categories.MONETARY_POLICY.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)


        ### Speeches
        self._driver.get("https://www.rba.gov.au/payments-and-infrastructure/resources/speeches.html")
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'fs')]"
        speeches = self._driver.find_elements(By.XPATH, xpath)
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
                        extracted_text = download_and_read_pdf(main_link, self.datadump_directory_path)
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
                    "category_name": Categories.MONETARY_POLICY.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ])
        self.add_all_atomic(result, total_categories, total_links)

        ### Publications
        self._driver.get("https://www.rba.gov.au/payments-and-infrastructure/resources/publications/")
        xpath = "//div[@id='content']/div[@class='list-articles']/div[@class='item']"
        publications = self._driver.find_elements(By.XPATH, xpath)
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
                extracted_text = download_and_read_pdf(url, self.datadump_directory_path)
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
                    "category_name": Categories.MONETARY_POLICY.value
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


    # Financial Stability
    def process_financial_stability(self):
        all_urls = self.get_all_db_urls()
        # Financial Stability reviews
        # skip becuase it is under publications

        # Publications
        self._driver.get("https://www.rba.gov.au/fin-stability/resources/publications.html")

        to_process = []
        xpath = "//div[@class='box-table']"
        box_tables = self._driver.find_elements(By.XPATH, xpath)
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
        for article in self._driver.find_elements(By.XPATH, xpath):
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
                extracted_text = download_and_read_pdf(url, self.datadump_directory_path)
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
        self._driver.get("https://www.rba.gov.au/fin-stability/resources/speeches.html")
        to_process = []
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'py')]"
        speeches = self._driver.find_elements(By.XPATH, xpath)
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
                        extracted_text = download_and_read_pdf(main_link, self.datadump_directory_path)
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
                link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
            # NOTE add support for different file types
            links_output.append({
                "file_url": url,
                "link_url": link_href,
                "link_name": link.text,
                "full_extracted_text": link_text,
            })
        return links_output




    def parse_html(self, url: str):
        self._driver.get(url)
        xpath = "//main[@id='content' or @id='main'] | //div[@id='content' or @id='main']"
        try:
            content = self._driver.find_element(By.XPATH, xpath)
        except NoSuchElementException:
            logger.warning(f"No content found for url: {url}")
            return "", []
        text = content.text
        # all links
        links_output = self.process_links(url, content)
        return text, links_output
    

    def process_list_by_year(self, year:int, f_url, categories):
        all_urls = self.get_all_db_urls()
        self._driver.get(f_url.format(year))
        # get class "list-articles"
        try:
            ul = self._driver.find_element(By.CLASS_NAME, "list-articles")
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
        #self.process_monetary_policy()
        #self.process_payments_infrastructure()
        self.process_financial_stability()
    

    def get_base_url(self) -> str:
        return f"https://www.rba.gov.au/monetary-policy"
    
    def get_base_url_monetary_policy_minutes_year(self, year:int) -> str:
        return f"{self.get_base_url()}/rba-board-minutes/{year}/"
    

    def get_base_url_monetary_policy_decision_year(self, year:int) -> str:
        return f"{self.get_base_url()}/int-rate-decisions/{year}/"