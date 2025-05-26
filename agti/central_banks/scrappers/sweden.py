from collections import defaultdict
import logging
import re
from urllib.parse import urlparse
import pandas as pd
import os
import time
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from agti.agti.central_banks.types import ExtensionType, MainMetadata
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, classify_extension
from selenium.common.exceptions import NoSuchElementException

logger = logging.getLogger(__name__)
__all__ = ["SwedenBankScrapper"]

class SwedenBankScrapper(BaseBankScraper):
    IGNORED_PATHS = [

    ]

    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get("https://www.riksbank.se/en-gb/")
        wait = WebDriverWait(self.driver_manager.driver, 10, 0.1)
        wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'js-accept-cookies')]"))
        ).click()
        self.cookies = self.driver_manager.driver.get_cookies()
    
    def process_monetary_policy(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-monetary-policy/"
        logger.info(f"Processing Monetary Policy News")
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.MONETARY_POLICY])

        # Account MP
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/account-of-monetary-policy/"
        logger.info(f"Processing Monetary Policy Account")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.MONETARY_POLICY])

        # Minutes MP
        main_url = "https://www.riksbank.se/en-gb/press-and-published/minutes-of-the-executive-boards-monetary-policy-meetings/"
        logger.info(f"Processing Monetary Policy Minutes")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.MONETARY_POLICY])

        
        # process archive reports
        main_url = "https://archive.riksbank.se/en/Web-archive/Published/Published-from-the-Riksbank/Monetary-policy/Monetary-Policy-Report/index.html@all=1.html"
        logger.info(f"Processing Monetary Policy Reports Archive")
        self.get(main_url)
        trs = self.driver_manager.driver.find_elements(By.XPATH, "//table//tr")[1:]
        to_process = defaultdict(list)
        omit_dates = []
        for tr in trs:
            tds = tr.find_elements(By.XPATH, "./td")
            date = pd.to_datetime(tds[0].text, dayfirst=True)
            a_tag = tds[1].find_element(By.XPATH, ".//a")
            href = a_tag.get_attribute("href")
            href_text = a_tag.text
            if href in all_urls:
                logger.debug(f"Url is already in db: {href}")
                categories = [Categories.MONETARY_POLICY]
                total_missing_cat = [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories if (href, category.value) not in all_categories
                ]
                if len(total_missing_cat) > 0:
                    self.add_to_categories(total_missing_cat)
                omit_dates.append(date)
                continue

            to_process[date].append((href_text, href))

        for date in omit_dates:
            if date in to_process:
                del to_process[date]


        # we need to group by date, same date == same release
        for date, values in to_process.items():
            def my_filter(value):
                return "slides" not in value[0].lower() and value[1].endswith(".pdf")
            main_reports = list(filter(my_filter, values))
            if len(main_reports) == 0:
                main_reports = list(filter(lambda x: x[1].endswith(".pdf"), values))
            main_report = main_reports[0]

            links = list(filter(lambda x: x[1] != main_report[1], values))

            logger.info(f"Processing {main_report[1]}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=main_report[1],
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id = self.download_and_upload_file(main_report[1], "pdf", main_metadata, year=str(date.year))
            if main_id is None:
                continue
            
            result = {
                "file_url": main_report[1],
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": main_report[1],
                    "category_name": Categories.MONETARY_POLICY.value
                }
            ]
            def f_get_links():
                return links
            processed_links = self.process_links(main_id, f_get_links, year=str(date.year))
            total_links = [
                {
                    "file_url": main_report[1],
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in processed_links
            ]
            self.add_all_atomic([result], total_categories, total_links)
            

        
        # process minutes
        main_url = "https://archive.riksbank.se/en/Web-archive/Published/Minutes-of-the-Executive-Boards-monetary-policy-meetings/index.html@all=1.html"
        logger.info(f"Processing Monetary Policy Minutes Archive")
        self.get(main_url)
        trs = self.driver_manager.driver.find_elements(By.XPATH, "//table//tr")[1:]
        to_process = []

        for tr in trs:
            tds = tr.find_elements(By.XPATH, "./td")
            date = pd.to_datetime(tds[0].text, dayfirst=True)
            a_tag = tds[1].find_element(By.XPATH, ".//a")
            href = a_tag.get_attribute("href")
            if href in all_urls:
                logger.debug(f"Url is already in db: {href}")
                categories = [Categories.MONETARY_POLICY]
                total_missing_cat = [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories if (href, category.value) not in all_categories
                ]
                if len(total_missing_cat) > 0:
                    self.add_to_categories(total_missing_cat)
                continue
            to_process.append((date, href))


        for (date, href) in to_process:
            logger.info(f"Processing {href}")
            self.get(href)
            main_div = self.driver_manager.driver.find_element(By.XPATH, "//div[@id='main']")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=href,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id = self.process_html_page(main_metadata, str(date.year))
            def f_get_links():
                links = []
                for link in main_div.find_elements(By.XPATH, ".//a"):
                    link_text = link.get_attribute("textContent").strip()
                    link_url = link.get_attribute("href")
                    if link_url is None:
                        continue
                    parsed_link = urlparse(link_url)
                    if any(ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS):
                        continue
                    links.append((link_text, link_url))
                return links
            processed_links = self.process_links(main_id, f_get_links, year=str(date.year))
            total_links = [
                {
                    "file_url": href,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in processed_links
            ]
            result = {
                "file_url": href,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": href,
                    "category_name": Categories.MONETARY_POLICY.value
                }
            ]
            self.add_all_atomic([result], total_categories, total_links)


    def process_financial_stability(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-financial-stability/"
        logger.info(f"Processing Financial Stability News")
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        
        # financial stability reports
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/financial-stability-report/?year=Show+all"
        logger.info(f"Processing Financial Stability Reports")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        # archive
        main_url = "https://archive.riksbank.se/en/Web-archive/Published/Published-from-the-Riksbank/Financial-stability/Financial-Stability-Report/index.html@all=1.html"
        logger.info(f"Processing Financial Stability Reports Archive")
        self.get(main_url)
        trs = self.driver_manager.driver.find_elements(By.XPATH, "//table//tr")[1:]
        to_process = defaultdict(list)
        omit_dates = []
        for tr in trs:
            tds = tr.find_elements(By.XPATH, "./td")
            date = pd.to_datetime(tds[0].text, dayfirst=True)
            a_tag = tds[1].find_element(By.XPATH, ".//a")
            href = a_tag.get_attribute("href")
            href_text = a_tag.text
            if href in all_urls:
                logger.debug(f"Url is already in db: {href}")
                categories = [Categories.FINANCIAL_STABILITY_AND_REGULATION]
                total_missing_cat = [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories if (href, category.value) not in all_categories
                ]
                if len(total_missing_cat) > 0:
                    self.add_to_categories(total_missing_cat)
                omit_dates.append(date)
                continue

            to_process[date].append((href_text, href))

        for date in omit_dates:
            if date in to_process:
                del to_process[date]
        
        # we need to group by date, same date == same release

        for date, values in to_process.items():
            pdf_only = [x for x in values if x[1].endswith(".pdf")]
            main_filters = [
                    x for x in pdf_only if 
                        (
                            "chart" not in x[0].lower() and
                            "slides" not in x[0].lower() and
                            "report" in x[0].lower()
                        )
                ]
            if len(main_filters)  == 1:
                main_reports = main_filters
            else:
                raise Exception("More than one main report found")
            main_report = main_reports[0]

            links = list(filter(lambda x: x[1] != main_report[1], values))
            logger.info(f"Processing {main_report[1]}")
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=main_report[1],
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            main_id = self.download_and_upload_file(main_report[1], "pdf", main_metadata, year=str(date.year))
            if main_id is None:
                continue
            result = {
                "file_url": main_report[1],
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": main_report[1],
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                }
            ]
            def f_get_links():
                return links
            processed_links = self.process_links(main_id, f_get_links, year=str(date.year))
            total_links = [
                {
                    "file_url": main_report[1],
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in processed_links
            ]
            self.add_all_atomic([result], total_categories, total_links)


    def process_payments_cash(self):
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-payments-and-cash/"
        logger.info(f"Processing Payments and Cash News")
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.NEWS_AND_EVENTS])


    def process_news(self):
        # news about the bank
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-the-riksbank/"
        logger.info(f"Processing Riksbank News")
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.NEWS_AND_EVENTS, Categories.OTHER])
        # News about markets
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-markets/"
        logger.info(f"Processing Markets News")
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.NEWS_AND_EVENTS])

    def prcoess_speeches_presentations(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        main_url= "https://www.riksbank.se/en-gb/press-and-published/speeches-and-presentations/"
        logger.info(f"Processing Speeches and Presentations")
        ul_xpath = "//div[@class='listing-block__body']//ul"
        page = 1
        to_process = []
        while True:
            self.get(main_url + "?&page={}".format(page))
            # Wait up to 60 seconds for the element to be present, checking every 0.1 seconds
            WebDriverWait(self.driver_manager.driver, 60, poll_frequency=0.1).until(
                EC.presence_of_element_located((By.XPATH, ul_xpath))
            )
            ul = self.driver_manager.driver.find_element(By.XPATH, ul_xpath)
            a_tags = ul.find_elements(By.XPATH,"./li/a")
            if len(a_tags) == 0:
                break
            for a_tag in a_tags:
                date_txt = a_tag.find_elements(By.XPATH,"./p/span")[1].text
                date = pd.to_datetime(date_txt, dayfirst=True)
                href = a_tag.get_attribute("href")
                categories = [Categories.NEWS_AND_EVENTS]
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    total_missing_cat = [
                        {
                            "file_url": href,
                            "category_name": category.value,
                        } for category in categories if (href, category.value) not in all_categories
                    ]
                    if len(total_missing_cat) > 0:
                        self.add_to_categories(total_missing_cat)
                    continue
                to_process.append((date, href, categories))
            page += 1
        

        for date, href, categories in to_process:
            logger.info(f"Processing {href}")
            self.get(href)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=href,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            articles = self.driver_manager.driver.find_elements(By.XPATH, "//article")
            if len(articles) > 1:
                raise Exception("More than one article found")
            article = articles[0]
            
            main_id = self.process_html_page(main_metadata, str(date.year))
            if main_id is None:
                continue
            result = {
                "file_url": href,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }

            def f_get_links():
                links = []
                for link in article.find_elements(By.XPATH, ".//a"):
                    link_text = link.get_attribute("textContent").strip()
                    link_url = link.get_attribute("href")
                    if link_url is None:
                        continue
                    parsed_link = urlparse(link_url)
                    if any(ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS):
                        continue
                    links.append((link_text, link_url))
                return links
            processed_links = self.process_links(main_id, f_get_links, year=str(date.year))
            
            total_links = [
                {
                    "file_url": href,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in processed_links
            ]

            total_categories = [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories
            ]
            self.add_all_atomic([result], total_categories, total_links)

    def process_publications(self):
        
        all_urls = self.get_all_db_urls()
        # account-of-monetary-policy process by MP
        
        # annual report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/annual-report/"
        logger.info(f"Processing Annual Reports")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # climate report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/climate-report/"
        logger.info(f"Processing Climate Reports")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Economic Commentaries
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/economic-commentaries/"
        logger.info(f"Processing Economic Commentaries")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Economic Review
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/economic-review/articles-in-the-economic-review/"
        logger.info(f"Processing Economic Review")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # E-krona
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/e-krona-reports/"
        logger.info(f"Processing E-krona Reports")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS])

        # Financial markets survey
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/financial-markets-survey/"
        logger.info(f"Processing Financial Markets Survey")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # financial stability reports done in financial stability
        # Monetary Policy Reports and Updates done in monetary policy

        ##################################
        ### Other former publications

        # EMU-related information
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/emu-related-information/"
        logger.info(f"Processing EMU Information")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Financial Infrastructure Report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/financial-infrastructure-report/?year=Show+all"
        logger.info(f"Processing Financial Infrastructure Reports")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        # archive
        single_url = "https://archive.riksbank.se/Documents/Rapporter/Fin_infra/2016/rap_finansiell_infrastruktur_160426_eng.pdf"
        if single_url not in all_urls:
            scraping_time = pd.Timestamp.now()
            published_date = pd.to_datetime("2016-04-26")
            main_metadata = MainMetadata(
                url=single_url,
                date_published=str(published_date),
                scraping_time=str(scraping_time),
            )
            main_id = self.download_and_upload_file(single_url, "pdf", main_metadata, year="2016")
            if main_id is not None:
                result = [
                    {
                        "file_url":single_url,
                        "date_published": published_date,
                        "scraping_time": scraping_time,
                        "file_id": main_id,
                    }
                ]
                total_categories = [
                    {
                        "file_url": single_url,
                        "category_name": Categories.RESEARCH_AND_DATA.value
                    }
                ]
                self.add_all_atomic(result, total_categories, [])
        
        # Monetary policy in Sweden - publication from 2010
        # skip empty

        # The Riksbank and Financial stability
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/the-riksbank-and-financial-stability/"
        logger.info(f"Processing Riksbank Financial Stability Publications")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Brochures on notes & coins
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/brochures-on-notes--coins/"
        logger.info(f"Processing Notes and Coins Brochures")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS])
        

        # Risk Survey
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/risk-survey/"
        logger.info(f"Processing Risk Survey")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        # add archive as well
        archive_urls = ["https://archive.riksbank.se/Documents/Rapporter/Riskenkat/2016/rap_riskenkat_161116_eng.pdf",
                        "https://archive.riksbank.se/Documents/Rapporter/Riskenkat/2016/rap_riskenkat_160525_uppdaterad2_eng.pdf"]

        for single_url in archive_urls:
            if single_url not in all_urls:
                scraping_time = pd.Timestamp.now()
                published_date = pd.to_datetime("2016-11-16")
                main_metadata = MainMetadata(
                    url=single_url,
                    date_published=str(published_date),
                    scraping_time=str(scraping_time),
                )
                main_id = self.download_and_upload_file(single_url, "pdf", main_metadata, year="2016")
                if main_id is None:
                    continue
                result = {
                        "file_url":single_url,
                        "date_published": published_date,
                        "scraping_time": scraping_time,
                        "file_id": main_id,
                    }
                
                total_categories = [
                    {
                        "file_url": single_url,
                        "category_name": Categories.RESEARCH_AND_DATA.value
                    }
                ]
            self.add_all_atomic([result], total_categories, [])
        

        # Payments Report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/payments-in-sweden/"
        logger.info(f"Processing Payments Reports")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS])
        

        # Riksbank studies
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/riksbank-studies/"
        logger.info(f"Processing Riksbank Studies")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Staff memos
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/staff-memos/"
        logger.info(f"Processing Staff Memos")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        

        # The Riksbank’s Statute Book
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/statute-book/"
        logger.info(f"Processing Statute Book")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.INSTITUTIONAL_AND_GOVERNANCE])

        # The Riksbank's Business Survey
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/the-riksbanks-business-survey/"
        logger.info(f"Processing Business Survey")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA, Categories.INSTITUTIONAL_AND_GOVERNANCE])

        # The Swedish Financial Market
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/the-swedish-financial-market/"
        logger.info(f"Processing Swedish Financial Market")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.INSTITUTIONAL_AND_GOVERNANCE])
        
        

        # Working Paper Series
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/working-paper-series/"
        logger.info(f"Processing Working Papers")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Occasional Paper Series
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/working-paper-series/occasional-paper-series/"
        logger.info(f"Processing Occasional Papers")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        

        # Conferences
        main_url = "https://www.riksbank.se/en-gb/press-and-published/conferences/"
        logger.info(f"Processing Conferences")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.NEWS_AND_EVENTS])

        # The Riksbank's cybersecurity competition
        main_url = "https://www.riksbank.se/en-gb/press-and-published/cybersecurity-competition/"
        logger.info(f"Processing Cybersecurity Competition")
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.NEWS_AND_EVENTS])


    def process_consultation_responses(self):
        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/the-riksbanks-domestic-consultation-responses/"
        logger.info(f"Processing Domestic Consultation Responses")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/the-riksbanks-international-consultation-responses/"
        logger.info(f"Processing International Consultation Responses")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/general-council-consultation-responses/"
        logger.info(f"Processing General Council Consultation Responses")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/other-consultations-responses/"
        logger.info(f"Processing Other Consultation Responses")
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])


    def simple_process(self, main_url, has_categories=False, additional_cat = []):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        ul_xpath = "//div[@class='listing-block__body']//ul"
        page_number = 1
        one_page = False
        to_process = []

        self.get(main_url)
        # find "pagination__link" class
        all_pagitation_links = self.driver_manager.driver.find_elements(By.XPATH, "//a[contains(@class, 'pagination__link')]")
        if len(all_pagitation_links) == 0:
            one_page = True

        while True:
            if one_page and page_number > 1:
                break
            if not one_page:
                page_url = main_url + "?&page={}".format(page_number)
            else:
                page_url = main_url
            self.get(page_url)
            
            # Wait up to 60 seconds for the element to be present, checking every 0.1 seconds
            WebDriverWait(self.driver_manager.driver, 60, poll_frequency=0.1).until(
                EC.presence_of_element_located((By.XPATH, ul_xpath))
            )
            ul = self.driver_manager.driver.find_element(By.XPATH, ul_xpath)
            a_tags = ul.find_elements(By.XPATH,"./li/a")
            if len(a_tags) == 0:
                break
            for a_tag in a_tags:
                
                if has_categories:
                    date_categories_txt = a_tag.find_element(By.XPATH,"./span[@class='date-and-category']").text.split(" ",1)
                    date_txt = date_categories_txt[0]
                    categories = text_to_categories(date_categories_txt[1].split(", "))
                    categories.update(additional_cat)
                else:
                    categories = set(additional_cat) 
                    date_txt = a_tag.find_element(By.XPATH,"./span[@class='label']").text
                
                date = pd.to_datetime(date_txt, dayfirst=True)
                href = a_tag.get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    total_missing_cat = [
                        {
                            "file_url": href,
                            "category_name": category.value,
                        } for category in categories if (href, category.value) not in all_categories
                    ]
                    if len(total_missing_cat) > 0:
                        self.add_to_categories(total_missing_cat)
                    continue
                to_process.append((date, href, categories))
            page_number += 1


        for date, href, categories in to_process:
            logger.info(f"Processing {href}")
            year = str(date.year)
            # check if href is in all_urls
            urlType, extension = self.clasify_url(href)
            allowed_outside = False
            extType = classify_extension(extension)
            processed_links = []
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=href,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(href, extension, main_metadata, year=year)
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                self.get(href)
                pdf_xpath = "//a[contains(text(), 'Download PDF')] | //a[@class='report-page__download']"
                pdf_links = self.driver_manager.driver.find_elements(By.XPATH, pdf_xpath)
                if len(pdf_links) > 0:
                    main_id = self.download_and_upload_file(href, extension, main_metadata, year=year)
                    if main_id is None:
                        continue
                else:
                    # we process the webpage as a file
                    def f_get_links():
                        links = []
                        for link in self.driver_manager.driver.find_elements(By.XPATH, "//article//a | //div[@id='main']//a"):
                            link_text = link.get_attribute("textContent").strip()
                            link_url = link.get_attribute("href")
                            if link_url is None:
                                continue
                            parsed_link = urlparse(link_url)
                            if any(ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS):
                                            continue
                            links.append((link_text, link_url))
                        return links
                    main_id = self.process_html_page(main_metadata, year)
                    processed_links = self.process_links(main_id, f_get_links, year=year)
            else:
                if allowed_outside or urlparse(href).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {href}", extra={
                        "url": href,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue

            result = {
                "file_url": href,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_links = [
                {
                    "file_url": href,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in processed_links
            ]

            total_categories = [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories
                ]
            self.add_all_atomic([result], total_categories, total_links)

            

    def process_all_years(self):
        self.process_monetary_policy()
        self.process_financial_stability()
        self.process_payments_cash()
        self.process_news()
        self.prcoess_speeches_presentations()
        self.process_publications()
        self.process_consultation_responses()



CATEGORY_MAP = {
    'CONSULTATION RESPONSES': Categories.FINANCIAL_STABILITY_AND_REGULATION,
    'MINUTES': Categories.MONETARY_POLICY,
    'ECONOMIC COMMENTARIES': Categories.RESEARCH_AND_DATA,
    'NEWS': Categories.NEWS_AND_EVENTS,
    'STAFF MEMO': Categories.RESEARCH_AND_DATA,
    'RIKSBANK STUDIES': Categories.RESEARCH_AND_DATA,
    'TERMS AND CONDITIONS': Categories.OTHER,
    'SPEECHES': Categories.NEWS_AND_EVENTS,
    'PRESS RELEASE': Categories.NEWS_AND_EVENTS,
    'MINUTES APPENDIX': Categories.MONETARY_POLICY,
    'PRESENTATION': Categories.NEWS_AND_EVENTS,
    'CONFERENCE': Categories.RESEARCH_AND_DATA,
    'RIKSBANKEN PLAY': Categories.NEWS_AND_EVENTS
} 


def text_to_categories(texts):
    return set(CATEGORY_MAP[text] for text in texts)
    