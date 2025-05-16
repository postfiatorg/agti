
import logging
from typing import DefaultDict
from urllib.parse import urlparse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import urllib
from agti.agti.central_banks.types import ExtensionType, LinkMetadata, MainMetadata
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, classify_extension
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
        for url in to_process:
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            logger.info(f"Processing: {url}")
            self.get(url)
            time_tag = self.driver_manager.driver.find_element(By.XPATH, "//time")
            date = pd.to_datetime(time_tag.text)
            main_content = self.driver_manager.driver.find_element(By.XPATH, "//div[@id='content']")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id = self.process_html_page(main_metadata, str(date.year))
            def f_get_links():
                links = []
                for link in main_content.find_elements(By.XPATH, ".//a"):
                    link_text = link.get_attribute("textContent").strip()
                    link_url = link.get_attribute("href")
                    if link_url is None:
                        continue
                    links.append((link_text, link_url))
                return links
            links_output = self.process_links(main_id, f_get_links, year=date.year)

            result = {
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id
            }
            # categories
            total_categories = [
                {
                    "file_url": url,
                    "category_name": Categories.MONETARY_POLICY.value
                }
            ]
            total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            self.add_all_atomic([result], total_categories, total_links)
        
        
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

        for date, url, temp_links in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id, links_output = self.parse_html(url, str(date.year), main_metadata)
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            # categories
            total_categories = [
                {
                    "file_url": url,
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)

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

        for date_txt, url in to_process:
            logger.info(f"Processing: {url}")
            date = None
            year = None
            date_txt = date_txt.strip()
            # if date start with number it has day otherwise it has month and year only
            if date_txt[0].isdigit():
                date = pd.to_datetime(date_txt)
                year = date.year
                date_txt = None
            else:
                # we are missing the day, so we can not use it as date
                year = pd.to_datetime(date_txt).year
            allowed_outside = False
            urlType, extension = self.clasify_url(url, allow_outside=allowed_outside)
            extType = classify_extension(extension)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published_str=date_txt,
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:

                main_id = self.download_and_upload_file(url, extension, main_metadata, year=str(year))
                if main_id is None:
                    continue
                total_links = []
            elif extType == ExtensionType.WEBPAGE:
                main_id, links_output = self.parse_html(url, str(year), main_metadata)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else:
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)
        
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



        for (date, url) in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id, links_output = self.parse_html(url, str(date.year), main_metadata)
            total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_categories =  [
                {
                    "file_url": url,
                    "category_name": Categories.NEWS_AND_EVENTS.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)

        ### Speeches
        self.get("https://www.rba.gov.au/payments-and-infrastructure/resources/speeches.html")
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'fs')]"
        speeches = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        for speech in speeches:
            # ul/li/a
            links = speech.find_elements(By.XPATH, ".//ul/li/a")
            try:
                url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            except NoSuchElementException:
                # find first html link from links select some representative from the list
                urls = [x.get_attribute("href") for x in links if x.get_attribute("href").endswith(".html")]
                if len(urls) > 0:
                    url = urls[0]
                else:
                    logger.warning(f"No href found for speech: '{speech.get_attribute('textContent')}' for Payments and Infrastructure - Resources - Speeches")
                    continue
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
            
            founded_links = []
            for link in links:
                link_url = link.get_attribute("href")
                founded_links.append((link_url, link.text))
            to_process.append((date, url, founded_links))


        for date, url, temp_links in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id, links_output = self.parse_html(url, str(date.year), main_metadata)
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            for (main_link, main_link_text) in temp_links:
                if url == main_link:
                    continue
                allowed_outside = False
                urlType, extension = self.clasify_url(main_link, allow_outside=allowed_outside)
                extType = classify_extension(extension)
                link_metadata = LinkMetadata(
                    url=main_link,
                    link_name=main_link_text,
                    main_file_id=main_id,
                )
                if extType == ExtensionType.FILE:
                    link_id = self.download_and_upload_file(main_link, extension, link_metadata,  year=str(date.year))
                    if link_id is None:
                        continue
                elif extType == ExtensionType.WEBPAGE:
                    metadata = LinkMetadata(
                        url=main_link,
                        link_name=main_link_text,
                        main_file_id=main_id,
                    )
                    link_id, _ = self.parse_html(main_link, str(date.year), metadata, parse_links=False)
                else:
                    if allowed_outside or urlparse(main_link).netloc == self.bank_config.NETLOC:
                        logger.error(f"Unknown file type: {main_link}", extra={
                            "url": url,
                            "link_url": main_link,
                            "urlType": urlType,
                            "extension_type": extension
                        })
                    continue
                total_links.append({
                    "file_url": url,
                    "link_url": main_link,
                    "link_name": main_link_text,
                    "file_id": link_id,
                })
                    
            # categories
            total_categories = [
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
            ]
            self.add_all_atomic([result], total_categories, total_links)

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
        
        for date_txt, url in to_process:
            logger.info(f"Processing: {url}")
            date = None
            year = None
            date_txt = date_txt.strip()
            # if date has just one "-" it is not a date
            if date_txt.count("-") == 2:
                date = pd.to_datetime(date_txt)
                date_txt = None
                year = str(date.year)
            elif date_txt.count("-") == 1:
                # we are missing the day, so we can not use it as date
                year = str(pd.to_datetime(date_txt).year)
            allowed_outside = False
            urlType, extension = self.clasify_url(url, allow_outside=allowed_outside)
            extType = classify_extension(extension)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published_str=date_txt,
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=year)
                if main_id is None:
                    continue
                total_links = []
            elif extType == ExtensionType.WEBPAGE:
                main_id, links_output = self.parse_html(url, year, main_metadata)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else: 
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)


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
        for date_txt, url in to_process:
            logger.info(f"Processing: {url}")
            date = None
            year = None
            if date_txt is not None:
                date_txt = date_txt.strip()
                # if date has just one "-" it is not a date
                if date_txt[0].isdigit():
                    date = pd.to_datetime(date_txt)
                    date_txt = None
                    year = str(date.year)
                else:
                    year = str(pd.to_datetime(date_txt).year)
            url_parsed = urlparse(url)
            
            allowed_outside = False
            urlType, extension = self.clasify_url(url, allow_outside=allowed_outside)
            extType = classify_extension(extension)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published_str=date_txt,
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=year)
                if main_id is None:
                    continue
                total_links = []
            elif extType == ExtensionType.WEBPAGE:
                main_id, links_output = self.parse_html(url, year, main_metadata)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else:
                if allowed_outside or url_parsed.netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_categories =[
                {
                    "file_url": url,
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)
        # Speeches
        self.get("https://www.rba.gov.au/fin-stability/resources/speeches.html")
        to_process = []
        xpath = "//div[@id='list-speeches']/div[contains(@class, 'py')]"
        speeches = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        for speech in speeches:
            links = speech.find_elements(By.XPATH, ".//ul/li/a")
            try:
                url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
            except NoSuchElementException:
                # find first html link from links
                urls = [x.get_attribute("href") for x in links if x.get_attribute("href").endswith(".html")]
                if len(urls) > 0:
                    url = urls[0]
                else:
                    logger.warning(f"No href found for speech: '{speech.get_attribute('textContent')}' for Payments and Infrastructure - Resources - Speeches")
                    continue
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
            # ul/li/a
            founded_links = []
            for link in links:
                link_url = link.get_attribute("href")
                founded_links.append((link_url, link.text))
            to_process.append((date, url, founded_links))

        for date, url, temp_links in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id, links_output = self.parse_html(url, str(date.year), main_metadata)
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            for (main_link, main_link_text) in temp_links:
                if url == main_link:
                    continue
                allowed_outside = False
                urlType, extension = self.clasify_url(main_link, allow_outside=allowed_outside)
                extType = classify_extension(extension)
                link_metadata = LinkMetadata(
                    url=main_link,
                    link_name=main_link_text,
                    main_file_id=main_id,
                )
                if extType == ExtensionType.FILE:
                    link_id = self.download_and_upload_file(main_link, extension, main_metadata, year=str(date.year))
                    if link_id is None:
                        continue
                elif extType == ExtensionType.WEBPAGE:
                    link_id, _ = self.parse_html(main_link, str(date.year), link_metadata, parse_links=False)
                else:
                    if allowed_outside or urlparse(main_link).netloc == self.bank_config.NETLOC:
                        logger.error(f"Unknown file type: {main_link}", extra={
                            "url": url,
                            "link_url": main_link,
                            "urlType": urlType,
                            "extension_type": extension
                        })
                    continue
                total_links.append({
                    "file_url": url,
                    "link_url": main_link,
                    "link_name": main_link_text,
                    "file_id": link_id,
                })
            # categories
            total_categories = [
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
            ]
            self.add_all_atomic([result], total_categories, total_links)


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
            
            for date, article_url in to_process:
                logger.info(f"Processing: {article_url}")
                allowed_outside = False
                urlType, extension = self.clasify_url(article_url, allow_outside=allowed_outside)
                extType = classify_extension(extension)
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=article_url,
                    date_published=str(date),
                    scraping_time=str(scraping_time),
                )
                if extType == ExtensionType.FILE:
                    main_id = self.download_and_upload_file(article_url, extension, main_metadata, year=str(date.year))
                    if main_id is None:
                        continue
                    total_links = []
                elif extType == ExtensionType.WEBPAGE:
                    main_id, links_output = self.parse_html(article_url, str(date.year), main_metadata)
                    total_links = [
                        {
                            "file_url": article_url,
                            "link_url": link,
                            "link_name": link_text,
                            "file_id": link_id,
                        } for (link, link_text, link_id) in links_output
                    ]
                else:
                    if allowed_outside or urlparse(article_url).netloc == self.bank_config.NETLOC:
                        logger.error(f"Unknown file type: {article_url}", extra={
                            "url": article_url,
                            "urlType": urlType,
                            "extension_type": extension
                        })
                    continue
                result = {
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_url": article_url,
                    "file_id": main_id,
                }
                total_categories = [
                    {
                        "file_url": article_url,
                        "category_name": Categories.NEWS_AND_EVENTS.value
                    }
                ]   
                self.add_all_atomic([result], total_categories, total_links)

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
                # ul/li/a
                links = speech.find_elements(By.XPATH, ".//ul/li/a")
                try:
                    url = speech.find_element(By.XPATH, ".//h3//a").get_attribute("href")
                except NoSuchElementException:
                    # find first html link from links
                    urls = [x.get_attribute("href") for x in links if x.get_attribute("href").endswith(".html")]
                    if len(urls) > 0:
                        url = urls[0]
                    else:     
                        logger.warning(f"No href found for speech: '{speech.get_attribute('textContent')}' for Payments and Infrastructure - Resources - Speeches")
                        continue
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date = pd.to_datetime(speech.find_element(By.XPATH, ".//time").get_attribute("datetime"))
                founded_links = []
                for link in links:
                    link_url = link.get_attribute("href")
                    founded_links.append((link_url, link.text))
                to_process.append((date, url, founded_links))
            
            for date, url, temp_links in to_process:
                logger.info(f"Processing: {url}")
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=url,
                    date_published=str(date),
                    scraping_time=str(scraping_time),
                )
                main_id, links_output = self.parse_html(url, str(date.year), main_metadata)
                result = {
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                for (main_link, main_link_text) in temp_links:
                    if url == main_link:
                        continue
                    allowed_outside = False
                    urlType, extension = self.clasify_url(main_link, allow_outside=allowed_outside)
                    extType = classify_extension(extension)
                    link_metadata = LinkMetadata(
                        url=main_link,
                        link_name=main_link_text,
                        main_file_id=main_id,
                    )
                    if extType == ExtensionType.FILE:
                        link_id = self.download_and_upload_file(main_link, extension, link_metadata, year=str(date.year))
                        if link_id is None:
                            continue
                    elif extType == ExtensionType.WEBPAGE:
                        link_id, _ = self.parse_html(main_link, str(date.year), link_metadata, parse_links=False)
                    else:
                        if allowed_outside or urlparse(main_link).netloc == self.bank_config.NETLOC:
                            logger.error(f"Unknown file type: {main_link}", extra={
                                "url": url,
                                "link_url": main_link,
                                "urlType": urlType,
                                "extension_type": extension
                            })
                        continue
                    total_links.append({
                        "file_url": url,
                        "link_url": main_link,
                        "link_name": main_link_text,
                        "file_id": link_id,
                    })
                # categories
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": Categories.NEWS_AND_EVENTS.value
                    }
                ]
                self.add_all_atomic([result], total_categories, total_links)
            
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

            for (date_txt,url) in to_process:
                logger.info(f"Processing: {url}")
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=url,
                    date_published_str=str(date_txt),
                    scraping_time=str(scraping_time),
                )
                year = str(pd.to_datetime(date_txt).year)
                main_id, links_output = self.parse_html(url, year, main_metadata)
                result = {
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": Categories.MONETARY_POLICY.value
                    },
                    {
                        "file_url": url,
                        "category_name": Categories.RESEARCH_AND_DATA.value
                    }
                ]
                self.add_all_atomic([result], total_categories, total_links)
                


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
                if "Boxes" in date_txt or "Focus Topics" in date_txt:
                    continue
                to_process.append((date_txt, url))

            for (date_txt,url) in to_process:
                logger.info(f"Processing: {url}")
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=url,
                    date_published_str=str(date_txt),
                    scraping_time=str(scraping_time),
                )
                year = str(pd.to_datetime(date_txt).year)
                main_id, links_output = self.parse_html(url, year, main_metadata)
                result = {
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": Categories.MONETARY_POLICY.value
                    },
                    {
                        "file_url": url,
                        "category_name": Categories.RESEARCH_AND_DATA.value
                    }
                ]
                self.add_all_atomic([result], total_categories, total_links)
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
        

            for date, url, categories in to_process:
                logger.info(f"Processing: {url}")
                year = str(date.year)
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=url,
                    date_published=str(date),
                    scraping_time=str(scraping_time),
                )
                main_id, links_output = self.parse_html(url, year, main_metadata)
                result = {
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": category.value
                    } for category in categories
                ]
                self.add_all_atomic([result], total_categories, total_links)
        
        
        # 2010 -> 2014
        for year in range(2010, 2015):
            self.get(main_url.format(year))
            month_xpath = "//div[@id='content']//section//li/a"
            data_list = self.driver_manager.driver.find_elements(By.XPATH, month_xpath)
            to_process = []
            for a_tag in data_list:
                url = a_tag.get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                date_txt = a_tag.text
                to_process.append((date_txt, url))

            for date_txt, url in to_process:
                logger.info(f"Processing: {url}")
                year = str(pd.to_datetime(date_txt).year)
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=url,
                    date_published_str=str(date_txt),
                    scraping_time=str(scraping_time),
                )
                main_id, links_output = self.parse_html(url, year, main_metadata)
                result = {
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                total_categories = [{
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                }]
                self.add_all_atomic([result], total_categories, total_links)
        
            
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


            for date_txt, url in to_process:
                logger.info(f"Processing: {url}")
                year = str(pd.to_datetime(date_txt).year)
                scraping_time = pd.Timestamp.now()
                main_metadata = MainMetadata(
                    url=url,
                    date_published_str=str(date_txt),
                    scraping_time=str(scraping_time),
                )
                main_id, links_output = self.parse_html(url, year, main_metadata)
                result = {
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                total_categories = [{
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                }]
                self.add_all_atomic([result], total_categories, total_links)
        
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

        for date_txt, url, categories in to_process:
            logger.info(f"Processing: {url}")
            year = None
            date = None
            date_txt = date_txt.strip()
            if date_txt[0].isdigit():
                date = pd.to_datetime(date_txt)
                year = str(date.year)
                date_txt = None
            else:
                year = str(pd.to_datetime(date_txt).year)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published_str=str(date_txt),
                scraping_time=str(scraping_time),
            )
            main_id, links_output = self.parse_html(url, year, main_metadata)
            result = {
                "date_published": date,
                "date_published_str": date_txt,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            total_categories = [
                {
                    "file_url": url,
                    "category_name": category.value
                } for category in categories
            ]
            self.add_all_atomic([result], total_categories, total_links)

        
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

                
        for date, url in to_process:
            logger.info(f"Processing: {url}")
            year = str(date.year)
            allowed_outside = False
            urlType, extension = self.clasify_url(url, allow_outside=allowed_outside)
            extType = classify_extension(extension)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            total_links = []
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=year)
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, links_output = self.parse_html(url, year, main_metadata)
                total_links.extend([
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ])
            else:
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            if url in to_process_links:
                for link_url, link_text in to_process_links[url]:
                    urlType, extension = self.clasify_url(link_url, allow_outside=allowed_outside)
                    extType = classify_extension(extension)
                    link_metadata = LinkMetadata(
                        url=link_url,
                        link_name=link_text,
                        main_file_id=main_id,
                    )
                    if extType == ExtensionType.FILE:
                        link_id = self.download_and_upload_file(link_url, extension, link_metadata, year=year)
                        if link_id is None:
                            continue
                    elif extType == ExtensionType.WEBPAGE:
                        link_id, _ = self.parse_html(link_url, year, link_metadata, parse_links=False)
                    else:
                        if allowed_outside or urlparse(link_url).netloc == self.bank_config.NETLOC:
                            logger.error(f"Unknown file type: {link_url}", extra={
                                "url": url,
                                "link_url": link_url,
                                "urlType": urlType,
                                "extension_type": extension
                            })
                        continue
                    total_links.append({
                        "file_url": url,
                        "link_url": link_url,
                        "link_name": link_text,
                        "file_id": link_id,
                    })
            total_categories = [{
                "file_url": url,
                "category_name": Categories.RESEARCH_AND_DATA.value
            }]
            self.add_all_atomic([result], total_categories, total_links)
        



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

        for date, url in to_process:
            logger.info(f"Processing: {url}")
            urlType, extension = self.clasify_url(url)
            extType = classify_extension(extension)
            allowed_outside = False
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=str(date.year))
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, links_output = self.parse_html(url, str(date.year), main_metadata)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else:
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                }
            ]

            self.add_all_atomic([result], total_categories, total_links)


        # Reporting and Planning
        ## RBA Annual Report
        rba_ar_url = "https://www.rba.gov.au/publications/annual-reports/rba/{}/"
        current_year = pd.Timestamp.now().year

        for year in range(1960, current_year + 1):
            date_txt = f"{year}"
            url = rba_ar_url.format(year)
            if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
            self.get(url)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published_str=date_txt,
                scraping_time=str(scraping_time),
            )
            # find a tag with "report" or "Annual Report" text 
            a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//div[@id='content']//a[text()='Report' or text()='report' or contains(text(),'Annual Report')]")
            if len(a_tags) > 0:
                url = a_tags[0].get_attribute("href")
                if url in all_urls:
                    logger.debug(f"Href is already in db: {url}")
                    continue
                logger.info(f"Processing: {url}")
                urlType, extension = self.clasify_url(url)
                extType = classify_extension(extension)
                if extType == ExtensionType.FILE:
                    main_id = self.download_and_upload_file(url, extension, main_metadata, year=str(year))
                    if main_id is None:
                        continue
                elif extType == ExtensionType.WEBPAGE:
                    main_id, links_output = self.parse_html(url, str(year), main_metadata)
                    total_links = [
                        {
                            "file_url": url,
                            "link_url": link,
                            "link_name": link_text,
                            "file_id": link_id,
                        } for (link, link_text, link_id) in links_output
                    ]
                else:
                    if urlparse(url).netloc == self.bank_config.NETLOC:
                        logger.error(f"Unknown file type: {url}", extra={
                            "url": url,
                            "urlType": urlType,
                            "extension_type": extension
                        })
                    continue
                result = {
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                
            else:
                try:
                    ul = self.driver_manager.driver.find_element(By.XPATH, "//ul[@class='list-contents']")
                except NoSuchElementException:
                    logger.warning(f"No data found for year: {year} and url: {rba_ar_url.format(year)}")
                    continue
                logger.info(f"Processing: {url}")
                main_id, links_output = self.parse_html(url, str(year), main_metadata)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                result = {
                    "date_published": None,
                    "date_published_str": date_txt,
                    "scraping_time": scraping_time,
                    "file_url": url,
                    "file_id": main_id,
                }
                allowed_outside = False
                for a_tag in ul.find_elements(By.XPATH, ".//a"):
                    a_url = a_tag.get_attribute("href")
                    a_urlType, a_extension = self.clasify_url(a_url)
                    a_extType = classify_extension(a_extension)
                    link_metadata = LinkMetadata(
                        url=a_url,
                        link_name=a_tag.text,
                        main_file_id=main_id,
                    )
                    if a_extType == ExtensionType.FILE:
                        link_id = self.download_and_upload_file(a_url, a_extension, link_metadata, year=str(year))
                        if link_id is None:
                            continue
                    elif a_extType == ExtensionType.WEBPAGE:
                        link_id, _ = self.parse_html(a_url, str(year), link_metadata, parse_links=False)
                        total_links.append({
                            "file_url": url,
                            "link_url": a_url,
                            "link_name": a_tag.text,
                            "file_id": link_id,
                        })
                    else:
                        if allowed_outside or urlparse(a_url).netloc == self.bank_config.NETLOC:
                            logger.error(f"Unknown file type: {a_url}", extra={
                                "url": url,
                                "link_url": a_url,
                                "urlType": a_urlType,
                                "extension_type": a_extension
                            })
                        continue
            total_categories = [{
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                }]
            self.add_all_atomic([result], total_categories, total_links)



        
        ## PSB Annual Report
        psb_ar_url = "https://www.rba.gov.au/publications/annual-reports/psb/{}/"
        current_year = pd.Timestamp.now().year
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
            if url in all_urls:
                logger.debug(f"Href is already in db: {url}")
                continue
            logger.info(f"Processing: {url}")
            urlType, extension = self.clasify_url(url)
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=url,
                date_published_str=str(year),
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=str(year))
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, links_output = self.parse_html(url, str(year), main_metadata)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else:
                if urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            
            result = {
                "date_published": None,
                "date_published_str": str(year),
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": url,
                    "category_name": Categories.RESEARCH_AND_DATA.value
                },
                {
                    "file_url": url,
                    "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)
            

    




    def parse_html(
            self, url: str,
            year: str,
            metadata: MainMetadata | LinkMetadata,
            parse_links: bool = True):
        self.get(url)
        xpath = "//main[@id='content' or @id='main'] | //div[@id='content' or @id='main']"
        try:
            content = self.driver_manager.driver.find_element(By.XPATH, xpath)
        except NoSuchElementException:
            logger.warning(f"No content found for url: {url}")
            return None
        file_id = self.process_html_page(metadata, year)
        def f_get_links():
            links = []
            for link in content.find_elements(By.XPATH, ".//a"):
                link_text = link.get_attribute("textContent").strip()
                link_url = link.get_attribute("href")
                links.append((link_text, link_url))
            return links
        if parse_links:
            processed_links = self.process_links(file_id, f_get_links, year=year)
        else:
            processed_links = []
        return file_id, processed_links
        
    

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
        for date, href in to_process:
            logger.info(f"Processing: {href}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=href,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id, links_output = self.parse_html(
                href,
                str(date.year),
                main_metadata,
            )
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": href,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": href,
                    "category_name": category.value
                } for category in categories
            ]
            total_links = [
                {
                    "file_url": href,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            self.add_all_atomic([result], total_categories, total_links)
            



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
