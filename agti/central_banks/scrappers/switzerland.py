import re
import pandas as pd
import logging
from selenium.webdriver.common.by import By
import urllib
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf

logger = logging.getLogger(__name__)
__all__ = ["SwitzerlandBankScrapper"]

class SwitzerlandBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "CHE"
    COUNTRY_NAME = "Switzerland"
    NETLOC = "www.snb.ch"


    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get(f"https://{self.NETLOC}")
            self.driver_manager.driver.execute_script("window.localStorage.clear();")
        else:
            self.driver_manager.driver.execute_script("window.localStorage.clear();")
            # refresh page
            self.driver_manager.driver.refresh()
        
        wait = WebDriverWait(self.driver_manager.driver, 10)
        xpath = "//button[@class='a-button a-button--primary a-button--size-4 h-typo-button-small js-m-gdpr-banner__button-all']"
        repeat = 3
        for i in range(repeat):
            try:
                cookie_btn = wait.until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                # click the cookie banner
                cookie_btn.click()
                break
            except Exception as e:
                logger.warning(f"Could not click cookie banner", exc_info=True)
                if i == repeat - 1:
                    raise e
        self.cookies = self.driver_manager.driver.get_cookies()

            
    
    def process_annual_report(self):
        logger.info("Processing Annual Report")
        all_db_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        self.get("https://www.snb.ch/en/news-publications/annual-report-overview")
        # xpath get all as from ul tag with class="sitemap-linklist"
        a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//ul[@class='sitemap-linklist']//a")
        if len(a_tags) == 0:
            raise ValueError("No data found for annual report")
        to_process = []
        for a in a_tags:
            href = a.get_attribute("href")
            if href in all_db_urls:
                logger.debug(f"Href is already in db: {href}")
                total_categories = [{
                    "file_url": href,
                    "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                } if (href, Categories.INSTITUTIONAL_AND_GOVERNANCE.value) not in all_categories else {}]
                if len(total_categories) > 0:
                    self.add_to_categories(total_categories)
                continue
            to_process.append(href)

        result = []
        total_categories = []
        total_links = []
        for href in to_process:
            self.get(href)
            if href == "https://www.snb.ch/en/news-publications/annual-report/annual-report-1996-2017":
                def f_url(page: int) -> str:
                    if page == 1:
                        return "https://www.snb.ch/en/news-publications/annual-report/annual-report-1996-2017"
                    return None
                self.process_teasor_list(f_url)
                
            elif href == "https://www.snb.ch/en/news-publications/annual-report/annual-report-1907-1995":
                a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//ul[@class='link-teaser-list']//a")
                for a in a_tags:
                    href = a.get_attribute("href")
                    if href in all_db_urls:
                        logger.debug(f"Href is already in db: {href}")
                        total_categories = [{
                            "file_url": href,
                            "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                        } if (href, Categories.INSTITUTIONAL_AND_GOVERNANCE.value) not in all_categories else {}]
                        if len(total_categories) > 0:
                            self.add_to_categories(total_categories)
                        continue
                    logger.info(f"Processing: {href}")
                    text = download_and_read_pdf(href,self.datadump_directory_path, self)
                    result.append({
                        "date_published": None,
                        "scraping_time": pd.Timestamp.now(),
                        "file_url": href,
                        "full_extracted_text": text,
                    })
                    total_categories.append({
                        "file_url": href,
                        "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                    })
            else:
                a = self.driver_manager.driver.find_element(By.XPATH, "//a[.//span[contains(text(), 'Complete annual report')]]")
                href2 = a.get_attribute("href")
                if href2 in all_db_urls:
                    logger.info(f"Href is already in db: {href2}")
                    total_categories = [{
                        "file_url": href2,
                        "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                    } if (href2, Categories.INSTITUTIONAL_AND_GOVERNANCE.value) not in all_categories else {}]
                    if len(total_categories) > 0:
                        self.add_to_categories(total_categories)
                    continue
                logger.info(f"Processing: {href2}")
                date, text, links = self.extract_date_text_or_pdf(href2)
                total_categories.append({
                    "file_url": href2,
                    "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                })
                result.append({
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_url": href2,
                    "full_extracted_text": text,
                })
                total_links.extend(links)
        self.add_all_atomic(result, total_categories, total_links)


    def process_monetary_policy_decisions(self):
        logger.info("Processing Monetary Policy Decisions")
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        self.get("https://www.snb.ch/en/the-snb/mandates-goals/monetary-policy/decisions")
        x_path_path = "//div[@class='container']//div[starts-with(@id, 'collapse')]"
        xpath = f"{x_path_path}//a[@class='m-mixed-list-item h-typo-body'] | {x_path_path}//div[@class='m-mixed-list-list__subtitle']"
        tags = self.driver_manager.driver.find_elements(By.XPATH, xpath)
        to_process = []
        links_to_process = {}
        current_url = None
        for tag in tags:
            if tag.tag_name == "div":
                current_url = None
            elif tag.tag_name == "a":
                span = tag.find_element(By.XPATH, ".//span")
                if current_url is None:
                    current_url = tag.get_attribute("href")
                    if current_url in all_urls:
                        logger.debug(f"URL already in db: {current_url}")
                        if (current_url, Categories.MONETARY_POLICY.value) not in all_categories:
                            self.add_to_categories([{
                                "file_url": current_url,
                                "category_name": Categories.MONETARY_POLICY.value,
                            }])
                        continue
                    links_to_process[current_url] = []
                    to_process.append(current_url)
                else:
                    # other are links
                    link_url = tag.get_attribute("href")
                    link_name = span.get_attribute("innerText").strip()
                    links_to_process[current_url].append((link_url, link_name))
            else:
                raise ValueError("Unknown tag")
            
        result = []
        total_categories = []
        total_links = []
        for url in to_process:
            logger.info(f"Processing: {url}")
            date, text, links = self.extract_date_text_or_pdf(url)
            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": text,
            })
            total_links.extend(links)
            for link_url, link_name in links_to_process[url]:
                _, text, _ = self.extract_date_text_or_pdf(link_url)
                total_links.append({
                    "file_url": url,
                    "link_url": link_url,
                    "link_name": link_name,
                    "full_extracted_text": text,
                })
            total_categories.append({
                "file_url": url,
                "category_name": Categories.MONETARY_POLICY.value,
            })
        self.add_all_atomic(result, total_categories, total_links)


    def process_news_on_the_website(self):
        logger.info("Processing News on the website")
        page_url = "https://www.snb.ch/en/news-publications/news~page-1={}~"
        f_page = lambda page: page_url.format(page)
        self.process_teasor_list(f_page, [Categories.NEWS_AND_EVENTS], ".//span[@class='publication-type h-typo-small-bold']")


    def process_press_releases(self):
        logger.info("Processing Press Releases")
        page_url = "https://www.snb.ch/en/news-publications/media-releases~page-4={}~"
        f_page = lambda page: page_url.format(page)
        self.process_teasor_list(f_page, [Categories.NEWS_AND_EVENTS])


    def process_quarterly_bulletin(self):
        logger.info("Processing Quarterly Bulletin")
        main_url = "https://www.snb.ch/en/news-publications/economy/quarterly-bulletin/quarterly-bulletin~page-1={}~"
        f_url = lambda page: main_url.format(page)
        self.process_teasor_list(f_url, [Categories.RESEARCH_AND_DATA])

        logger.info("Processing SNB Quarterly Bulletin Studies")
            # SNB
        def f_url(page: int) -> str:
            if page == 1:
                return f"https://www.snb.ch/en/news-publications/economy/quarterly-bulletin/quarterly-bulletin-studies"
            return None
        self.process_teasor_list(f_url, [Categories.RESEARCH_AND_DATA])

    def process_financial_stability_report(self):
        logger.info("Processing Financial Stability Report")
        main_url =  "https://www.snb.ch/en/news-publications/economy/report-financial-stability~page-1={}~"
        f_url = lambda page: main_url.format(page)
        self.process_teasor_list(f_url, [Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.RESEARCH_AND_DATA])

    def process_studies_papers_notes(self):
        logger.info("Processing Economic Studies")
        def f_url(page):
            if page == 1:
                return f"https://www.snb.ch/en/news-publications/economy/economic-studies"
            return None
        self.process_teasor_list(f_url, [Categories.RESEARCH_AND_DATA])

        logger.info("Processing SNB Working Papers")
        def f_url(page):
            if page == 1:
                return f"https://www.snb.ch/en/news-publications/economy/working-papers"
            return None
        self.process_teasor_list(f_url, [Categories.RESEARCH_AND_DATA])

        logger.info("Processing SNB Economic Notes")
        def f_url(page):
            if page == 1:
                return f"https://www.snb.ch/en/news-publications/economy/economic-notes"
            return None
        self.process_teasor_list(f_url, [Categories.RESEARCH_AND_DATA])

    def process_speeches(self):
        logger.info("Processing Speeches")
        main_url = "https://www.snb.ch/en/news-publications/speeches~page-0={}~"
        f_url = lambda page: main_url.format(page)
        self.process_teasor_list(f_url, [Categories.NEWS_AND_EVENTS])


    def process_business_cycles_signals(self):
        logger.info("Processing Business Cycle Signals")
        main_url = "https://www.snb.ch/en/news-publications/business-cycle-signals~page-2={}~"
        f_url = lambda page: main_url.format(page)
        self.process_teasor_list(f_url, [Categories.RESEARCH_AND_DATA])
        
        

    

    def extract_date_text_or_pdf(self, url: str) -> dict:
        self.get(url)
        try:
            # span with class="h-typo-tiny"
            span_date = self.driver_manager.driver.find_element(By.XPATH, "//span[@class='h-typo-tiny']")
            # December 18, 2024
            date = pd.to_datetime(span_date.text, format="%B %d, %Y")
        except:
            date = None

        download_buttons = self.driver_manager.driver.find_elements(By.XPATH, "//a[span[normalize-space(text())='Download']]")
        if len(download_buttons) > 1:
            raise ValueError("More than one download button found")
        if len(download_buttons) == 1:
            pdf_href = download_buttons[0].get_attribute("href")
            text = download_and_read_pdf(pdf_href,self.datadump_directory_path)
            return date, text, []
        
        # try to find german or french
        download_buttons = self.driver_manager.driver.find_elements(By.XPATH, "//a[span[normalize-space(text())='german' or normalize-space(text())='french']]")
        if len(download_buttons) > 2:
            raise ValueError("More than two download button found for german or french")
        if len(download_buttons) > 0:
            pdf_href = download_buttons[0].get_attribute("href")
            text = download_and_read_pdf(pdf_href,self.datadump_directory_path)
            return date, text, []
        xpath = "//main//article"
        try:
            text = self.driver_manager.driver.find_element(By.XPATH, xpath).text
        except:
            return date, None, []
        # add links
        a_tags = self.driver_manager.driver.find_elements(By.XPATH, f"{xpath}//a")
        links = []
        for a in a_tags:
            link = a.get_attribute("href")
            name = a.text
            extracted_link_text = None
            if link.endswith(".pdf"):
                extracted_link_text = download_and_read_pdf(link,self.datadump_directory_path, self)
            links.append({
                "file_url": url,
                "link_url": link,
                "link_name": name,
                "full_extracted_text": extracted_link_text
            })
        return date, text, links

        
    

    def process_teasor_list(self, func_target_url, categories: list[Categories]  = [], xpath_category: str | None = None):
        found_any = False
        to_process = []
        url_categories = {}
        all_db_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        page = 1
        while True:
            url = func_target_url(page)
            if url is None:
                break
            self.get(url)
            # get "link-teaser-list" ul tag
            a_tags = self.driver_manager.driver.find_elements(By.XPATH, "//ul[contains(@class, 'link-teaser-list') or contains(@class, 'publication-link-list')]//a")
            if len(a_tags) < 3:
                break
            else:
                found_any = True
            for a in a_tags:
                total_categories = categories.copy()
                # get a tag
                href = a.get_attribute("href")
                if xpath_category is not None:
                    category = self.map_category_str(a.find_element(By.XPATH, xpath_category).text.strip())
                    if category is None:
                        continue
                    total_categories.append(category)
                if href in all_db_urls:
                    logger.debug(f"Href is already in db: {href}")
                    # drop remove categories in all_categories
                    total_categories_dict = [{
                            "file_url": href,
                            "category_name": c.value,
                        } for c in total_categories if (href,c.value) not in all_categories]
                    if len(total_categories_dict) > 0:
                        self.add_to_categories(total_categories_dict)
                    continue
                to_process.append(href)
                url_categories[href] = total_categories


            page += 1
        if not found_any:
            raise ValueError("No data found")
        result = []
        total_categories = []
        total_links = []
        for url in to_process:
            logger.info(f"Processing: {url}")
            if url.endswith(".pdf"):
                text = download_and_read_pdf(url,self.datadump_directory_path, self)
                date = None

            else:
                date, text, links = self.extract_date_text_or_pdf(url)
            if text is None:
                continue
            total_links.extend(links)

            result.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": url,
                "full_extracted_text": text,
            })
            total_categories.extend([{
                "file_url": url,
                "category_name": c.value,
            } for c in url_categories[url]])
        self.add_all_atomic(result, total_categories, total_links)

    def map_category_str(self, category: str) -> Categories | None:
        mapping = {
            'Data portal': None,#Categories.RESEARCH_AND_DATA,
            'SNB Economic Notes': Categories.RESEARCH_AND_DATA,
            'Financial markets': Categories.FINANCIAL_STABILITY_AND_REGULATION,
            'Banknotes and coins': Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS,
            'SNB Economic Studies': Categories.RESEARCH_AND_DATA,
            'Financial stability': Categories.FINANCIAL_STABILITY_AND_REGULATION,
            'Shareholders': Categories.INSTITUTIONAL_AND_GOVERNANCE,
            'Web-TV': None, #Categories.NEWS_AND_EVENTS,
            'Statistics': Categories.RESEARCH_AND_DATA,
            'Publication': Categories.RESEARCH_AND_DATA,
            'SNB Working Papers': Categories.RESEARCH_AND_DATA,
            'Press release': Categories.NEWS_AND_EVENTS,
            'Publications': Categories.RESEARCH_AND_DATA,
            'Payment transactions': Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS,
            'Media': Categories.NEWS_AND_EVENTS,
            'Research TV': Categories.RESEARCH_AND_DATA,
            'Speech': Categories.NEWS_AND_EVENTS,
            'Circular letter': Categories.INSTITUTIONAL_AND_GOVERNANCE
        }
        return mapping[category]



    def process_all_years(self):
        # based on https://www.snb.ch/en/news-publications and
        # https://www.snb.ch/en/news-publications/order-publications
        self.process_monetary_policy_decisions()
        self.process_news_on_the_website()
        self.process_press_releases()
        self.process_annual_report()
        self.process_quarterly_bulletin()
        self.process_financial_stability_report()
        self.process_studies_papers_notes()
        self.process_speeches()
        self.process_business_cycles_signals()

    



        # we skip monthly statistical bulletin

        # we skip monthly bulletin of banking statistics

        # we skip Banks in Switzerland reports (discontinued)
    

