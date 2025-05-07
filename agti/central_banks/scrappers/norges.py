import logging
import sys
import time
from urllib.parse import urlparse
import pandas as pd
import time
import requests
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf


logger = logging.getLogger(__name__)

__all__ = ["NorgesBankScrapper"]

class NorgesBankScrapper(BaseBankScraper):
    IGNORED_WHOLE_PATHS = [
        "/en/news-events/news/",
        "/en/news-events/news",
        "/en/news-events/",
        "/en/news-events",
        "/en/news-events/publications/",
        "/en/news-events/publications",
        "/en/news-events/calendar/",
        "/en/news-events/calendar",
        "",
        "/",
        "/en/",
        "/en",
    ]
    def initialize_cookies(self, go_to_url = False):
        current_url = self.driver_manager.driver.current_url
        # go to main page
        parsed_current_url = urlparse(current_url)
        # if we are under /api/NewsList/LoadMoreAndFilter,  go to main page
        if parsed_current_url.path.startswith("/api/NewsList/LoadMoreAndFilter") or go_to_url:
            # go to main page
            self.driver_manager.driver.get(self.bank_config.URL)

        self.driver_manager.driver.execute_script("CookieInformation.submitConsent()")
        time.sleep(0.1)
        self.cookies = self.driver_manager.driver.get_cookies()
        if parsed_current_url.path.startswith("/api/NewsList/LoadMoreAndFilter") and not go_to_url:
            self.driver_manager.driver.get(current_url)


    
    


    def process_all_years(self):


        self.process_meetings()

        # News & Events
        ## News and publications (id:145875)
        news_events_categories = {
            "press releases": [Categories.NEWS_AND_EVENTS],
            "new items": [Categories.NEWS_AND_EVENTS],
            "speeches": [Categories.NEWS_AND_EVENTS],
            "submissions": [Categories.OTHER, Categories.NEWS_AND_EVENTS],
            "balance sheets": [Categories.RESEARCH_AND_DATA, Categories.NEWS_AND_EVENTS],
            "circulars": [Categories.OTHER, Categories.NEWS_AND_EVENTS],
            "articles and opinion pieces": [Categories.OTHER, Categories.NEWS_AND_EVENTS],
            "news items": [Categories.NEWS_AND_EVENTS],
            "articles and opinion pieces": [Categories.OTHER, Categories.NEWS_AND_EVENTS],
        }
        logger.info(f"Processing News")
            
        self.process_id(145875, news_events_categories)
        
        ## Publications (id: 145877)
        ### reports
        publications = {
            "norway's financial system": [
                Categories.FINANCIAL_STABILITY_AND_REGULATION, 
                Categories.RESEARCH_AND_DATA
            ],
            "documentation note": [
                Categories.OTHER
            ],
            "financial infrastructure report": [
                Categories.FINANCIAL_STABILITY_AND_REGULATION, 
                Categories.RESEARCH_AND_DATA
            ],
            "financial stability report": [
                Categories.FINANCIAL_STABILITY_AND_REGULATION, 
                Categories.RESEARCH_AND_DATA
            ],
            "management of foreign exchange reserves": [
                Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, 
                Categories.MONETARY_POLICY
            ],
            "expectations survey": [
                Categories.RESEARCH_AND_DATA
            ],
            "market surveys": [
                Categories.RESEARCH_AND_DATA, 
                Categories.FINANCIAL_STABILITY_AND_REGULATION
            ],
            "norges bank papers": [
                Categories.RESEARCH_AND_DATA
            ],
            "monetary policy report": [
                Categories.MONETARY_POLICY
            ],
            "regional network reports": [
                Categories.RESEARCH_AND_DATA, 
                Categories.INSTITUTIONAL_AND_GOVERNANCE
            ],
            "norges bank’s survey of bank lending": [
                Categories.RESEARCH_AND_DATA
            ],
            "annual report": [
                Categories.INSTITUTIONAL_AND_GOVERNANCE
            ],
            "working papers": [
                Categories.RESEARCH_AND_DATA
            ],
            "staff memo": [
                Categories.INSTITUTIONAL_AND_GOVERNANCE, 
                Categories.RESEARCH_AND_DATA
            ],
            "occasional papers": [
                Categories.RESEARCH_AND_DATA
            ],
            "government debt management memo": [
                Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS, 
                Categories.INSTITUTIONAL_AND_GOVERNANCE
            ],
            "external evaluations": [
                Categories.INSTITUTIONAL_AND_GOVERNANCE
            ],
            "annual reports for retail payment services": [
                Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, 
                Categories.INSTITUTIONAL_AND_GOVERNANCE
            ],
            "economic commentaries": [
                Categories.RESEARCH_AND_DATA
            ],

        }

        logger.info(f"Processing Publications")
        self.process_id(145877, publications)





    def process_meetings(self):
        # Monetary policy meetings (still uses old api), maybe we will need to change this later
        logger.info(f"Processing Monetary policy meetings")
        all_urls = self.get_all_db_urls()
        all_categories = self.get_all_db_categories()

        categories = [
            Categories.MONETARY_POLICY,
        ]

        page = 1
        while True:
            output = []
            page_url = f"https://www.norges-bank.no/api/NewsList/LoadMoreAndFilter?currentPageId=78157&page={page}&clickedCategoryFilter=0&clickedYearFilter=0&language=en"
            self.get(page_url)
            xpath_articles = "//article[@class='article-list__item']"
            articles = self.driver_manager.driver.find_elements(By.XPATH, xpath_articles)
            if len(articles) == 0:
                break
            for article in articles:
                # we can ignore tags
                date_str = article.find_element(By.XPATH, ".//div[@class='meta']")
                date = pd.to_datetime(date_str.text)
                a_tag = article.find_element(By.XPATH, ".//h3/a")
                href = a_tag.get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    # add missing categories
                    total_missing_cat = [
                        {
                            "file_url": href,
                            "category_name": category.value,
                        } for category in categories if (href, category.value) not in all_categories
                    ]
                    if len(total_missing_cat) > 0:
                        self.add_to_categories(total_missing_cat)
                    continue
                output.append(
                    (href, date)
                )

            if page == 1 and len(articles) == 0:
                raise ValueError(f"No articles found for Meetings")
            

            # process each article

            for href, date in output:
                logger.info(f"Processing: {href}")
                self.get(href)
                # open all datails html tag
                self.open_all_details()
                main_id = self.process_html_page(str(date.year))
                if main_id is None:
                    continue
                result = {
                    "file_url": href,
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_id": main_id,
                }
                content = None
                xpaths = [
                    "//div[@class='article publication-start']",
                    "//article[@class='article']",
                ]
                for xpath in xpaths:
                    elements = self.driver_manager.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        content = elements[0]
                        break
                def f_get_links():
                    if content is None:
                        return []
                    links = []
                    for link in content.find_elements(By.XPATH, ".//a"):
                        link_text = link.get_attribute("textContent").strip()
                        link_url = link.get_attribute("href")
                        if link_url is None:
                            continue
                        parsed_link = urlparse(link_url)
                        if any(ignored_path == parsed_link.path for ignored_path in self.IGNORED_WHOLE_PATHS):
                            continue
                        links.append((link_text, link_url))
                    return links
                processed_links = self.process_links(f_get_links, year=str(date.year))
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



            page += 1
            self.random_sleep()


    def open_all_details(self):
        self.driver_manager.driver.execute_script("""
            document.querySelectorAll('details').forEach(d => {
                d.open = true;                  // set the DOM property
                // or: d.setAttribute('open','');  // add the HTML attribute
            });
            """)





    def process_id(self, id: int, categories_mapper: dict[str,Categories], category_filter=0):
        all_urls = self.get_all_db_urls()
        # Process a single ID
        logger.info(f"Processing ID: {id}")
        totalHits = sys.maxsize
        current_hits = 0
        
        while current_hits < totalHits:
            page_url = self.api_url(id, current_hits)
            logger.info(f"Fetching {current_hits}")
            # we use request
            headers = self.get_headers()
            cookies = self.get_cookies_for_request()
            proxies = self.get_proxies()
            for i in range(3):
                try:
                    resp = requests.get(page_url, headers=headers, cookies=cookies, proxies=proxies, allow_redirects=True, timeout=60)
                    resp.raise_for_status()
                except requests.exceptions.HTTPError:
                    logger.exception(f"HTTPError getting filetype for {page_url}", extra={
                        "url": page_url,
                        "headers": headers,
                        "cookies": cookies,
                        "proxies": proxies,
                    })
                    cookies = None
                    logger.info("Trying again with new proxy")
                    if self.driver_manager.proxy_provider is not None:
                        new_proxies = self.driver_manager.proxy_provider.get_proxy()
                        proxies = {
                            "http": new_proxies,
                            "https": new_proxies
                        }
                except Exception as e:
                    logger.exception(f"General getting filetype from {page_url}", extra={
                        "url": page_url,
                        "headers": headers,
                        "cookies": cookies,
                        "proxies": proxies,
                    })
                    break
            if resp.status_code != 200:
                logger.exception(f"Failed to get file type for {page_url}, status code: {resp.status_code}", extra={
                    "url": page_url,
                    "headers": headers,
                    "cookies": cookies,
                    "proxies": proxies,
                })
                return
            fetched_json = resp.json()
            totalHits = fetched_json["totalHits"]

            for hit in fetched_json["hits"]:
                total_url = f"https://{self.bank_config.NETLOC}{hit['url']}"
                if total_url in all_urls:
                    logger.debug(f"Already processed {total_url}")
                    continue
                logger.info(f"Processing: {total_url}")

                date = pd.to_datetime(hit["date"])
                self.get(total_url)
                self.open_all_details()
                main_id = self.process_html_page(str(date.year))
                if main_id is None:
                    continue
                result = {
                    "file_url": total_url,
                    "date_published": date,
                    "scraping_time": pd.Timestamp.now(),
                    "file_id": main_id,
                }
                xpaths = [
                    "//div[@class='article publication-start']",
                    "//article[@class='article']",
                    "//main"
                ]
                content = None
                for xpath in xpaths:
                    elements = self.driver_manager.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        content = elements[0]
                        break
                
                
                def f_get_links():
                    if content is None:
                        return []
                    links = []
                    for link in content.find_elements(By.XPATH, ".//a"):
                        link_text = link.get_attribute("textContent").strip()
                        link_url = link.get_attribute("href")
                        if link_url is None:
                            continue
                        parsed_link = urlparse(link_url)
                        if any(ignored_path == parsed_link.path for ignored_path in self.IGNORED_WHOLE_PATHS):
                            continue
                        links.append((link_text, link_url))
                    return links
                
                processed_links = self.process_links(f_get_links, year=str(date.year))
                total_links = [
                    {
                        "file_url": total_url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in processed_links
                ]
                total_categories = []
                if hit["tag"].lower() in categories_mapper:
                    total_categories = [
                        {
                            "file_url": total_url,
                            "category_name": cat.value,
                        } for cat in categories_mapper[hit["tag"].lower()]
                    ]
                else:
                    logger.warning(f"Unknown category: {hit['tag'].lower()} total_url: {total_url}")
                self.add_all_atomic([result], total_categories, total_links)
            
            # increment the current hits
            current_hits += 10 # the api returns 10 hits at a time
            self.random_sleep()
            


        

    @staticmethod
    def api_url(id: int, skip: int = 0) -> str:
        # API URL for Norges Bank
        return f"https://www.norges-bank.no/api/aktuelt?includeFacets=False&language=en&rootPageId={id}&skip={skip}"
    
    