import re
from urllib.parse import urlparse
import pandas as pd
import logging
from selenium.webdriver.common.by import By
import selenium
from agti.agti.central_banks.types import ExtensionType, MainMetadata
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, classify_extension

logger = logging.getLogger(__name__)
__all__ = ["SwitzerlandBankScrapper"]

class SwitzerlandBankScrapper(BaseBankScraper):
    IGNORED_PATHS = [
        "/contact",
        "/career",
        "/claims/rss",
        "/rss/claims"
    ]
    DOWNLOAD_A_XPATH = "(//a[.//span[translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='download']]) | (//a[.//span[normalize-space(text())='german' or normalize-space(text())='french']])"
    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get(self.bank_config.URL)
            self.driver_manager.driver.execute_script("window.localStorage.clear();")
        else:
            self.driver_manager.driver.execute_script("window.localStorage.clear();")
            # refresh page
            self.driver_manager.driver.refresh()
        wait = WebDriverWait(self.driver_manager.driver, 10)
        xpath = "//div[@data-g-name='GdprBanner']//button[normalize-space(.)='Accept']"
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
                
        # add own cookie 
        # Name: SNB-Access-Restriction, Value: GMBF, Domain:www.snb.ch, Path: /, Expiration: Session
        self.driver_manager.driver.add_cookie({
            'name': 'SNB-Access-Restriction',
            'value': 'GMBF',
            'domain': 'www.snb.ch',
            'path': '/',
        })
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


        for href in to_process:
            scraping_time = pd.Timestamp.now()
            self.get(href)
            if href == "https://www.snb.ch/en/news-publications/annual-report/annual-report-1996-2017":
                def f_url(page: int) -> str:
                    if page == 1:
                        return "https://www.snb.ch/en/news-publications/annual-report/annual-report-1996-2017"
                    return None
                self.process_teasor_list(f_url)
                continue
                
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
                    main_metadata = MainMetadata(
                        url=href,
                        date_published_str="1995-1997",
                        scraping_time=str(scraping_time),
                    )
                    file_id = self.download_and_upload_file(href, "pdf", main_metadata, year=None)
                    result = {
                        "date_published": None,
                        "date_published_str": "1995-1997",
                        "scraping_time": scraping_time,
                        "file_url": href,
                        "file_id": file_id,
                    }
                    total_categories = [{
                        "file_url": href,
                        "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                    }]
                    self.add_all_atomic([result], total_categories, [])
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
                file_id, date, total_links = self.extract_date_text_or_pdf(href2, scraping_time)
                total_categories = [{
                    "file_url": href2,
                    "category_name": Categories.INSTITUTIONAL_AND_GOVERNANCE.value,
                }]
                result = {
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_url": href2,
                    "file_id": file_id,
                }
                self.add_all_atomic([result], total_categories, total_links)


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
                    links_to_process[current_url] = []
                    if current_url in all_urls:
                        logger.debug(f"URL already in db: {current_url}")
                        if (current_url, Categories.MONETARY_POLICY.value) not in all_categories:
                            self.add_to_categories([{
                                "file_url": current_url,
                                "category_name": Categories.MONETARY_POLICY.value,
                            }])
                        continue
                    to_process.append(current_url)
                else:
                    # other are links
                    link_url = tag.get_attribute("href")
                    link_name = span.get_attribute("innerText").strip()
                    links_to_process[current_url].append((link_name, link_url))
            else:
                raise ValueError("Unknown tag")
            
        for url in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_id, date, total_links = self.extract_date_text_or_pdf(url, scraping_time)
            year = str(date.year) if date is not None else None
            result = {
                "date_published": date,
                "scraping_time": scraping_time,
                "file_url": url,
                "file_id": main_id,
            }
            links_output = self.process_links(
                main_id,
                lambda: links_to_process[url],
                year=str(year),
                download_a_tag_xpath=self.DOWNLOAD_A_XPATH,
            )
            total_links.extend([
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ])
            total_categories = [{
                "file_url": url,
                "category_name": Categories.MONETARY_POLICY.value,
            }]
            self.add_all_atomic([result], total_categories, total_links)


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
        
        

    

    def extract_date_text_or_pdf(self, url: str, scraping_time) -> tuple:
        # main_id, date, links_output
        self.get(url)
        try:
            # span with class="h-typo-tiny"
            span_date = self.driver_manager.driver.find_element(By.XPATH, "//span[@class='h-typo-tiny']")
            # December 18, 2024
            date = pd.to_datetime(span_date.text, format="%B %d, %Y")
        except:
            date = None
        year = str(date.year) if date is not None else None
        main_metadata = MainMetadata(
            url=url,
            date_published=str(date) if date is not None else None,
            scraping_time=str(scraping_time),
        )

        download_buttons = self.driver_manager.driver.find_elements(By.XPATH, "//a[.//span[translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='download']]")
        if len(download_buttons) > 1:
            raise ValueError("More than one download button found")
        if len(download_buttons) == 1:
            pdf_href = download_buttons[0].get_attribute("href")
            file_id = self.download_and_upload_file(pdf_href, "pdf", main_metadata, year=year)
            return file_id, date, []
        
        # try to find german or french
        download_buttons = self.driver_manager.driver.find_elements(By.XPATH, "//a[.//span[normalize-space(text())='german' or normalize-space(text())='french']]")
        if len(download_buttons) > 2:
            raise ValueError("More than two download button found for german or french")
        if len(download_buttons) > 0:
            pdf_href = download_buttons[0].get_attribute("href")
            file_id = self.download_and_upload_file(pdf_href, "pdf", main_metadata, year=year)
            return file_id, date, []
        main_id = self.process_html_page(main_metadata, year)
        def get_links():
            links_data = []
            for temp_link in self.driver_manager.driver.find_elements(By.XPATH, "//main//a"):
                try:
                    link_href = temp_link.get_attribute("href")
                    if link_href is None:
                        continue
                    parsed_link = urlparse(link_href)
                    link_name = temp_link.get_attribute("textContent").strip()
                except selenium.common.exceptions.StaleElementReferenceException:
                    continue
                if any([ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS]):
                    continue
                links_data.append((link_name, link_href))
            return links_data
        
        links_output = self.process_links(
                main_id,
                get_links,
                year=year,
                download_a_tag_xpath=self.DOWNLOAD_A_XPATH,
            )
        total_links = [
                {
                    "file_url": url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
        return main_id, date, total_links

        
    

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
                if href == "https://www.snb.ch/public/en/rss/claims":
                    continue
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
        
        for url in to_process:
            if any([ignored_path in url for ignored_path in self.IGNORED_PATHS]):
                continue
            logger.info(f"Processing: {url}")
            allowed_outside = False
            urlType, extension = self.classify_url(url)
            extType = classify_extension(extension)
            date = None
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=url,
                    scraping_time=str(scraping_time),
                )
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=None)
                if main_id is None:
                    raise ValueError(f"Could not download file: {url}")
            elif extType == ExtensionType.WEBPAGE:
                main_id, date, total_links = self.extract_date_text_or_pdf(url, scraping_time)
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
            total_categories = [{
                "file_url": url,
                "category_name": c.value,
            } for c in url_categories[url]]
            self.add_all_atomic([result], total_categories, total_links)

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
            'Circular letter': Categories.INSTITUTIONAL_AND_GOVERNANCE,
            'Research': Categories.RESEARCH_AND_DATA,
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
    

