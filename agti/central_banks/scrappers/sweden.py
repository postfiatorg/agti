from collections import defaultdict
import logging
import re
from urllib.parse import urlparse
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf
from selenium.common.exceptions import NoSuchElementException

logger = logging.getLogger(__name__)
__all__ = ["SwedenBankScrapper"]

class SwedenBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "SWE"
    COUNTRY_NAME = "Sweden"



    def process_monetary_policy(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-monetary-policy/"
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.MONETARY_POLICY])

        # Account MP
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/account-of-monetary-policy/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.MONETARY_POLICY])

        # Minutes MP
        main_url = "https://www.riksbank.se/en-gb/press-and-published/minutes-of-the-executive-boards-monetary-policy-meetings/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.MONETARY_POLICY])

        
        # process archive reports
        main_url = "https://archive.riksbank.se/en/Web-archive/Published/Published-from-the-Riksbank/Monetary-policy/Monetary-Policy-Report/index.html@all=1.html"
        self._driver.get(main_url)
        trs = self._driver.find_elements(By.XPATH, "//table//tr")[1:]
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

            to_process[date].append((href, href_text))

        for date in omit_dates:
            if date in to_process:
                del to_process[date]

        result = []
        total_categories = []
        total_links = []
        # we need to group by date, same date == same release
        for date, values in to_process.items():
            def my_filter(value):
                return "slides" not in value[1].lower() and value[0].endswith(".pdf")
            main_reports = list(filter(my_filter, values))
            if len(main_reports) == 0:
                main_reports = list(filter(lambda x: x[0].endswith(".pdf"), values))
            main_report = main_reports[0]

            links = list(filter(lambda x: x[0] != main_report[0], values))
            text = download_and_read_pdf(main_report[0], self.datadump_directory_path)
            logger.info(f"Processing {main_report[0]}")
            result.append({
                "file_url": main_report[0],
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "full_extracted_text": text,
            })
            total_categories.append(
                {
                    "file_url": main_report[0],
                    "category_name": Categories.MONETARY_POLICY.value
                }
            )
            for (link_href, link_tag_text) in links:
                link_text = None
                if link_href.endswith(".pdf"):
                    link_text = download_and_read_pdf(main_report[0], self.datadump_directory_path)
                total_links.append({
                    "file_url": main_report[0],
                    "link_url": link_href,
                    "link_name": link_tag_text,
                    "full_extracted_text": link_text,
                })

        self.add_all_atomic(result, total_categories, total_links)
            

        
        # process minutes
        main_url = "https://archive.riksbank.se/en/Web-archive/Published/Minutes-of-the-Executive-Boards-monetary-policy-meetings/index.html@all=1.html"
        self._driver.get(main_url)
        trs = self._driver.find_elements(By.XPATH, "//table//tr")[1:]
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

        result = []
        total_categories = []
        total_links = []
        for (date, href) in to_process:
            logger.info(f"Processing {href}")
            self._driver.get(href)
            main_div = self._driver.find_element(By.XPATH, "//div[@id='main']")
            text = main_div.text
            links = main_div.find_elements(By.XPATH, "./parent::div//a")
            for link in links:
                link_text = None
                link_href = link.get_attribute("href")
                if link_href.endswith(".pdf"):
                    link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
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
                "full_extracted_text": text,
            })
            total_categories.append(
                {
                    "file_url": href,
                    "category_name": Categories.MONETARY_POLICY.value
                }
            )
        self.add_all_atomic(result, total_categories, total_links)


    def process_financial_stability(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-financial-stability/"
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        
        # financial stability reports
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/financial-stability-report/?year={}"
        current_year = pd.Timestamp.now().year
        ul_xpath = "//div[@class='listing-block__body']//ul"
        to_process = []
        for year in range(2017, current_year+1):
            self._driver.get(main_url.format(year))
            ul = self._driver.find_element(By.XPATH, ul_xpath)
            a_tags = ul.find_elements(By.XPATH,"./li/a")
            for a_tag in a_tags:
                date_txt = a_tag.find_element(By.XPATH,"./span[@class='label']").text
                date = pd.to_datetime(date_txt, dayfirst=True)
                href = a_tag.get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    total_missing_cat = [
                        {
                            "file_url": href,
                            "category_name": category.value,
                        } for category in [Categories.FINANCIAL_STABILITY_AND_REGULATION] if (href, category.value) not in all_categories
                    ]
                    if len(total_missing_cat) > 0:
                        self.add_to_categories(total_missing_cat)
                    continue
                to_process.append((date, href))

        result = []
        total_categories = []
        total_links = []
        for (date, href) in to_process:
            self._driver.get(href)

            articles = self._driver.find_elements(By.XPATH, "//article")
            if len(articles) > 1:
                raise Exception("More than one article found")
            article = articles[0]
            text = article.text

            links = article.find_elements(By.XPATH, ".//a")
            for link in links:
                link_href = link.get_attribute("href")
                link_text = None
                if link_href.endswith(".pdf"):
                    link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
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
                "full_extracted_text": text,
            })
            total_categories.append(
                {
                    "file_url": href,
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                }
            )
        self.add_all_atomic(result, total_categories, total_links)
        

        # archive
        main_url = "https://archive.riksbank.se/en/Web-archive/Published/Published-from-the-Riksbank/Financial-stability/Financial-Stability-Report/index.html@all=1.html"
        self._driver.get(main_url)
        trs = self._driver.find_elements(By.XPATH, "//table//tr")[1:]
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

            to_process[date].append((href, href_text))

        for date in omit_dates:
            if date in to_process:
                del to_process[date]
        
        result = []
        total_categories = []
        total_links = []
        # we need to group by date, same date == same release

        for date, values in to_process.items():
            pdf_only = [x for x in values if x[0].endswith(".pdf")]
            main_filters = [
                    x for x in pdf_only if 
                        (
                            "chart" not in x[1].lower() and 
                            "slides" not in x[1].lower() and
                            "report" in x[1].lower()
                        )
                ]
            if len(main_filters)  == 1:
                main_reports = main_filters
            else:
                raise Exception("More than one main report found")
            main_report = main_reports[0]

            links = list(filter(lambda x: x[0] != main_report[0], values))
            text = download_and_read_pdf(main_report[0], self.datadump_directory_path)
            logger.info(f"Processing {main_report[0]}")
            result.append({
                "file_url": main_report[0],
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "full_extracted_text": text,
            })
            total_categories.append(
                {
                    "file_url": main_report[0],
                    "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value
                }
            )
            for (link_href, link_tag_text) in links:
                link_text = None
                if link_href.endswith(".pdf"):
                    link_text = download_and_read_pdf(main_report[0], self.datadump_directory_path)
                total_links.append({
                    "file_url": main_report[0],
                    "link_url": link_href,
                    "link_name": link_tag_text,
                    "full_extracted_text": link_text,
                })
        self.add_all_atomic(result, total_categories, total_links)


    def process_payments_cash(self):
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-payments-and-cash/"
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.NEWS_AND_EVENTS])


    def process_news(self):
        # news about the bank
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-the-riksbank/"
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.NEWS_AND_EVENTS, Categories.OTHER])
        # News about markets
        main_url = "https://www.riksbank.se/en-gb/press-and-published/notices-and-press-releases/news-about-markets/"
        self.simple_process(main_url, has_categories=True, additional_cat=[Categories.NEWS_AND_EVENTS])

    def prcoess_speeches_presentations(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        main_url= "https://www.riksbank.se/en-gb/press-and-published/speeches-and-presentations/"
        ul_xpath = "//div[@class='listing-block__body']//ul"
        page = 1
        to_process = []
        while True:
            self._driver.get(main_url.format(page))
            ul = self._driver.find_element(By.XPATH, ul_xpath)
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
        
        result = []
        total_categories = []
        total_links = []
        for date, href, categories in to_process:
            logger.info(f"Processing {href}")
            self._driver.get(href)
            articles = self._driver.find_elements(By.XPATH, "//article")
            if len(articles) > 1:
                raise Exception("More than one article found")
            article = articles[0]
            text = article.text

            links = article.find_elements(By.XPATH, ".//a")
            for link in links:
                link_href = link.get_attribute("href")
                link_text = None
                if link_href.endswith(".pdf"):
                    link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
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
                "full_extracted_text": text,
            })
            total_categories.extend(
                [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories
                ]
            )
        self.add_all_atomic(result, total_categories, total_links)

    def process_publications(self):
        
        all_urls = self.get_all_db_urls()
        # account-of-monetary-policy process by MP
        
        # annual report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/annual-report/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # climate report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/climate-report/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Economic Commentaries
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/economic-commentaries/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Economic Review
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/economic-review/articles-in-the-economic-review/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # E-krona
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/e-krona-reports/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS])

        # Financial markets survey
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/financial-markets-survey/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # financial stability reports done in financial stability
        # Monetary Policy Reports and Updates done in monetary policy

        ##################################
        ### Other former publications

        # EMU-related information
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/emu-related-information/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Financial Infrastructure Report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/financial-infrastructure-report/?year=Show+all"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        # archive
        single_url = "https://archive.riksbank.se/Documents/Rapporter/Fin_infra/2016/rap_finansiell_infrastruktur_160426_eng.pdf"
        if single_url not in all_urls:
            text = download_and_read_pdf(single_url, self.datadump_directory_path)
            result = [
                {
                    "file_url":single_url,
                    "date_published": pd.to_datetime("2016-04-26"),
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text,
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
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Brochures on notes & coins
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/brochures-on-notes--coins/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS])
        

        # Risk Survey
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/other-former-publications/risk-survey/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        # add archive as well
        archive_urls = ["https://archive.riksbank.se/Documents/Rapporter/Riskenkat/2016/rap_riskenkat_161116_eng.pdf",
                        "https://archive.riksbank.se/Documents/Rapporter/Riskenkat/2016/rap_riskenkat_160525_uppdaterad2_eng.pdf"]
        result = []
        total_categories = []
        for single_url in archive_urls:
            if single_url not in all_urls:
                text = download_and_read_pdf(single_url, self.datadump_directory_path)
                result.append(
                    {
                        "file_url":single_url,
                        "date_published": pd.to_datetime("2016-11-16"),
                        "scraping_time": pd.Timestamp.now(),
                        "full_extracted_text": text,
                    }
                )
                total_categories.append(
                    {
                        "file_url": single_url,
                        "category_name": Categories.RESEARCH_AND_DATA.value
                    }
                )
        self.add_all_atomic(result, total_categories, [])
        

        # Payments Report
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/payments-in-sweden/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS])
        

        # Riksbank studies
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/riksbank-studies/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Staff memos
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/staff-memos/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        

        # The Riksbank’s Statute Book
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/statute-book/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.INSTITUTIONAL_AND_GOVERNANCE])

        # The Riksbank's Business Survey
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/the-riksbanks-business-survey/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA, Categories.INSTITUTIONAL_AND_GOVERNANCE])

        # The Swedish Financial Market
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/the-swedish-financial-market/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.INSTITUTIONAL_AND_GOVERNANCE])
        
        

        # Working Paper Series
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/working-paper-series/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])

        # Occasional Paper Series
        main_url = "https://www.riksbank.se/en-gb/press-and-published/publications/working-paper-series/occasional-paper-series/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.RESEARCH_AND_DATA])
        

        # Conferences
        main_url = "https://www.riksbank.se/en-gb/press-and-published/conferences/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.NEWS_AND_EVENTS])

        # The Riksbank's cybersecurity competition
        main_url = "https://www.riksbank.se/en-gb/press-and-published/cybersecurity-competition/"
        self.simple_process(main_url , has_categories=False, additional_cat=[Categories.NEWS_AND_EVENTS])


    def process_consultation_responses(self):
        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/the-riksbanks-domestic-consultation-responses/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/the-riksbanks-international-consultation-responses/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/general-council-consultation-responses/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])

        main_url = "https://www.riksbank.se/en-gb/press-and-published/consultations-responses/other-consultations-responses/"
        self.simple_process(main_url, has_categories=False, additional_cat=[Categories.FINANCIAL_STABILITY_AND_REGULATION])


    def simple_process(self, main_url, has_categories=False, additional_cat = []):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]


        ul_xpath = "//div[@class='listing-block__body']//ul"
        page_number = 1
        one_page = False
        to_process = []

        self._driver.get(main_url)
        # find "pagination__link" class
        all_pagitation_links = self._driver.find_elements(By.XPATH, "//a[contains(@class, 'pagination__link')]")
        if len(all_pagitation_links) == 0:
            one_page = True



        while True:
            if one_page and page_number > 1:
                break
            if not one_page:
                page_url = main_url + "?&page={}".format(page_number)
            else:
                page_url = main_url
            self._driver.get(page_url)
            ul = self._driver.find_element(By.XPATH, ul_xpath)
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

        result = []
        total_categories = []
        total_links = []
        for date, href, categories in to_process:
            logger.info(f"Processing {href}")
            href_prased = urlparse(href)
            text = None
            if href_prased.path.endswith(".pdf"):
                text = download_and_read_pdf(href, self.datadump_directory_path)
            elif href_prased.path.endswith(".html") or href_prased.path.endswith(".htm") or href_prased.path.endswith("/"):
                self._driver.get(href)
                articles = self._driver.find_elements(By.XPATH, "//article")
                if len(articles) > 1:
                    raise Exception("More than one article found")
                elif len(articles) == 0:
                    if "archive" in href_prased.path:
                        # find div id="main"
                        main_div = self._driver.find_element(By.XPATH, "//div[@id='main']")
                        text = main_div.text
                        links = main_div.find_elements(By.XPATH, ".//a")
                    else:
                        # try to find text Download PDF
                        # or we could parse the page, the issue is, it dynamically loads the content
                        pdf_xpath = "//a[contains(text(), 'Download PDF')] | //a[@class='report-page__download']"
                        pdf_href = self._driver.find_element(By.XPATH, pdf_xpath).get_attribute("href")
                        text = download_and_read_pdf(pdf_href, self.datadump_directory_path)
                        links = []
                else:
                    main_text = articles[0]
                    text = main_text.text
                    links = main_text.find_elements(By.XPATH, ".//a")
                
                for link in links:
                    link_href = link.get_attribute("href")
                    link_text = None
                    if link_href.endswith(".pdf"):
                        link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
                    total_links.append({
                        "file_url": href,
                        "link_url": link_href,
                        "link_name": link.text,
                        "full_extracted_text": link_text,
                    })
            else:
                logger.info(f"Unknown file type: {href}")
                text = None


            total_categories.extend(
                [
                    {
                        "file_url": href,
                        "category_name": category.value,
                    } for category in categories
                ]
            )
            result.append({
                "file_url": href,
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "full_extracted_text": text,
            })
        self.add_all_atomic(result, total_categories, total_links)
                

            

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
    