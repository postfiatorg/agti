
import io
import re
from urllib.parse import urljoin, urlparse
import pandas as pd
import logging
import requests
import re
import selenium
from selenium.webdriver.common.by import By
from agti.agti.central_banks.types import ExtensionType, MainMetadata
from selenium.webdriver.support import expected_conditions as EC
from ..utils import Categories, classify_extension
from ..base_scrapper import BaseBankScraper

logger = logging.getLogger(__name__)

__all__ = ["FEDBankScrapper"]

class FEDBankScrapper(BaseBankScraper):
    """
    We use  "For use at" initial text to detect correct tolerances for pdfplumber.
    Plus, we use it for extracting exact datetime.
    """
    IGNORED_PATHS = [
        "/aboutthefed/contact-us-topics.htm",
        "/faqs.htm",
        "/videos.htm",
        "/careers.htm",
        "/azindex.htm",
        "/sitemap.htm",
        "/calendar.htm",
        "/newsevents.htm",
        "/default.htm",
        "/monetarypolicy.htm",
    ]

    def process_news_events(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        # press releases (from 2006, we avoid archive)
        # fetch https://www.federalreserve.gov/json/ne-press.json
        to_process = []
        ret = requests.get(
            "https://www.federalreserve.gov/json/ne-press.json")
        ret.encoding = "utf-8-sig"
        data = ret.json()
        for d in data:
            if "updateDate" in d:
                update_date = pd.to_datetime(d["updateDate"].split(":")[1])
                if pd.Timestamp.now() - update_date > pd.Timedelta(days=2):
                    logger.warning("Update date is older than 2 days for the press releases data")
                continue

            date = pd.to_datetime(d["d"])
            url = urljoin("https://www.federalreserve.gov", d['l'])
            pt = d["pt"]
            categories = [Categories.NEWS_AND_EVENTS.value]
            if pt in ['Orders on Banking Applications', 'Enforcement Actions', 'Monetary Policy', 'Banking and Consumer Regulatory Policy']:
                categories.append(Categories.FINANCIAL_STABILITY_AND_REGULATION.value)
            if pt == "Monetary Policy":
                categories.append(Categories.MONETARY_POLICY.value)
            if pt == "Other Announcements":
                categories.append(Categories.OTHER.value)
            if url in all_urls:
                logger.debug(f"Url is already in db: {url}")
                total_categories = [
                    {"file_url": url, "category_name": category}
                    for category in categories
                    if (url, category) not in all_categories
                ]
                if len(total_categories) > 0:
                    self.add_to_categories(total_categories)
                continue
            to_process.append((url, date, categories))
        

        for url, date, categories in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_id, total_links = self.read_html(url, str(date.year),scraping_time,date = date)
            result ={
                "file_url": url,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [
                {"file_url": url, "category_name": category}
                for category in categories
            ]
            self.add_all_atomic([result], total_categories, total_links)

        # speeches
        ret = requests.get("https://www.federalreserve.gov/json/ne-speeches.json")
        ret.encoding = "utf-8-sig"
        data = ret.json()
        to_process = []
        for d in data:
            if "updateDate" in d:
                update_date = pd.to_datetime(d["updateDate"].split(":")[1])
                if pd.Timestamp.now() - update_date > pd.Timedelta(days=2):
                    logger.warning("Update date is older than 2 days for the speeches data")
                continue
            date = pd.to_datetime(d["d"])
            url = urljoin("https://www.federalreserve.gov", d['l'])
            categories = [Categories.NEWS_AND_EVENTS.value]
            if url in all_urls:
                logger.debug(f"Url is already in db: {url}")
                total_categories = [
                    {"file_url": url, "category_name": category}
                    for category in categories
                    if (url, category) not in all_categories
                ]
                if len(total_categories) > 0:
                    self.add_to_categories(total_categories)
                continue
            to_process.append((url, date, categories))
        

        for url, date, categories in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_id, total_links = self.read_html(url, str(date.year), scraping_time, date=date)
            result = {
                "file_url": url,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories.extend([
                {"file_url": url, "category_name": category}
                for category in categories
            ])
            self.add_all_atomic([result], total_categories, total_links)

        # Testimony of Federal Reserve Officials
        ret = requests.get("https://www.federalreserve.gov/json/ne-testimony.json")
        ret.encoding = "utf-8-sig"
        data = ret.json()
        to_process = []
        for d in data:
            if "updateDate" in d:
                update_date = pd.to_datetime(d["updateDate"].split(":")[1])
                if pd.Timestamp.now() - update_date > pd.Timedelta(days=2):
                    logger.warning("Update date is older than 2 days for the testimony data")
                continue
            date = pd.to_datetime(d["d"])
            url = urljoin("https://www.federalreserve.gov", d['l'])
            categories = [Categories.NEWS_AND_EVENTS.value]
            if url in all_urls:
                logger.debug(f"Url is already in db: {url}")
                total_categories = [
                    {"file_url": url, "category_name": category}
                    for category in categories
                    if (url, category) not in all_categories
                ]
                if len(total_categories) > 0:
                    self.add_to_categories(total_categories)
                continue
            to_process.append((url, date, categories))
        for url, date, categories in to_process:
            logger.info(f"Processing: {url}")
            scraping_time = pd.Timestamp.now()
            main_id, total_links = self.read_html(url, str(date.year),scraping_time, date=date)
            result = {
                "file_url": url,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories.extend([
                {"file_url": url, "category_name": category}
                for category in categories
            ])
            self.add_all_atomic([result], total_categories, total_links)


        # conferences
        # we skip them for now

    def process_FOMC(self, do_history=True):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        
        ret = requests.get("https://www.federalreserve.gov/monetarypolicy/materials/assets/final-recent.json")
        ret.encoding = "utf-8-sig"
        data = ret.json()['mtgitems']
        if do_history:
            ret = requests.get("https://www.federalreserve.gov/monetarypolicy/materials/assets/final-hist.json")
            ret.encoding = "utf-8-sig"
            data.extend(ret.json()['mtgitems'])
        
        to_process = []
        links_to_process = {}
        for d in data:
            date = pd.to_datetime(d["d"])
            url = None
            if "files" in d:
                files_output = {}
                files = d["files"]
                for file in files:
                    if file.get("name") == "HTML":
                        files_output["html"] = file["url"]
                    elif file.get("name") == "PDF":
                        files_output["pdf"] = file["url"]
                if len(files_output) == 2:
                    url = files_output["html"]
                    if url in links_to_process:
                        links_to_process[url].update([files_output["pdf"]])
                    else:
                        links_to_process[url] = set([files_output["pdf"]])
                elif "html" in files_output:
                    url = files_output["html"]
                elif "pdf" in files_output:
                    url = files_output["pdf"]

            elif "url" in d:
                url = d["url"]
            if url is None:
                continue
            if url.startswith("http") and  not "www.federalreserve.gov" in url:
                continue
            total_url = urljoin("https://www.federalreserve.gov", url)
            if total_url in all_urls:
                logger.debug(f"Url is already in db: {total_url}")
                total_categories = [
                    {"file_url": total_url, "category_name": Categories.MONETARY_POLICY.value}
                ]
                if (total_url, Categories.MONETARY_POLICY.value) not in all_categories:
                    self.add_to_categories(total_categories)
                continue
            to_process.append((url, date))
        

        for url, date in to_process:
            # if url does not start with https://www.federalreserve.gov, we add it
            total_url = urljoin("https://www.federalreserve.gov", url)
            logger.info(f"Processing: {total_url}")
            urlType, extension = self.classify_url(total_url)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=total_url,
                    scraping_time=str(scraping_time),
                    date_published=str(date),
                )
                main_id = self.download_and_upload_file(total_url, extension, main_metadata, year=str(date.year))
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, total_links = self.read_html(total_url, str(date.year),scraping_time, date=date)
            else:
                if allowed_outside or urlparse(total_url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {total_url}", extra={
                        "url": total_url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "file_url": total_url,
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_id": main_id,
            }
            total_categories = [{"file_url": total_url, "category_name": Categories.MONETARY_POLICY.value}]
            self.add_all_atomic([result], total_categories, total_links)

        # memos
        ret = requests.get("https://www.federalreserve.gov/monetarypolicy/materials/assets/final-memos.json")
        ret.encoding = "utf-8-sig"
        data = ret.json()['mtgitems']
        to_process = []
        links_to_process = {}
        for d in data:
            date = pd.to_datetime(d["d"])
            if "url" in d:
                url = d["url"]
            elif "dbl" in d:
                dbl = d["dbl"]
                for temp in dbl:
                    if "rk" in temp:
                        if url in links_to_process:
                            links_to_process[url].append((temp["name"], temp["url"]))
                        else:
                            links_to_process[url] = [(temp["name"], temp["url"])]
                    else:
                        url = temp["url"]
            if "pkg" in d:
                pkg = d["pkg"]
                for temp in pkg:
                    if "files" in temp:
                        for file in temp["files"]:
                            if url in links_to_process:
                                links_to_process[url].append((file["name"], file["url"]))
                            else:
                                links_to_process[url] = [(file["name"], file["url"])]
                                
                    else:
                        if url in links_to_process:
                            links_to_process[url].append((temp["name"], temp["url"]))
                        else:
                            links_to_process[url] = [(temp["name"], temp["url"])]
            total_url = urljoin("https://www.federalreserve.gov", url)
            if total_url in all_urls:
                logger.debug(f"Url is already in db: {total_url}")
                total_categories = [
                    {"file_url": total_url, "category_name": Categories.MONETARY_POLICY.value}
                ]
                if (total_url, Categories.MONETARY_POLICY.value) not in all_categories:
                    self.add_to_categories(total_categories)
                continue
            to_process.append((url, date))
        
        for url, date in to_process:
            # if url does not start with https://www.federalreserve.gov, we add it
            total_url = urljoin("https://www.federalreserve.gov", url)
            logger.info(f"Processing: {total_url}")

            urlType, extension = self.classify_url(total_url)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=total_url,
                    scraping_time=str(scraping_time),
                    date_published=str(date),
                )
                main_id = self.download_and_upload_file(total_url, extension, main_metadata, year=str(date.year))
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, total_links = self.read_html(total_url, str(date.year))
            else:
                if allowed_outside or urlparse(total_url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {total_url}", extra={
                        "url": total_url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "file_url": total_url,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [{"file_url": total_url, "category_name": Categories.MONETARY_POLICY.value}]
            self.add_all_atomic([result], total_categories, total_links)



        

        

            


    def process_monetary_policy(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        # a tag regex
        A_REGEX = r'<a\s+href="([^"]+)">([^<]+)</a>'


        self.get("https://www.federalreserve.gov/monetarypolicy/publications/mpr_default.htm")
        to_process = []
        links_to_process = {}
        # select div by id lazyload-container
        div = self.driver_manager.driver.find_element(
            By.XPATH, "//div[@id='article']/div/div[@class='row']/div")
        # iterate over all divs inside dl
        elements = list(div.find_elements(By.XPATH, "./*"))
        h4s = elements[::3]
        ps = elements[1::3]
        # hrs = elements[2::3]  # we can ignore these
        for h4, p in zip(h4s, ps):
            # get year
            year = int(h4.text.strip())
            # get inner html of p
            html_p = p.get_attribute("innerHTML")
            for line in html_p.split("<br>"):
                month_word = line.split(':')[0].strip()
                a_tags = dict([(name, href) for (href, name) in re.findall(A_REGEX, line)])
                date_txt = f"{month_word} {year}"
                url = a_tags.get("HTML",a_tags.get("Testimony"))
                total_url = urljoin("https://www.federalreserve.gov", url)
                if url is None:
                    continue
                if total_url in all_urls:
                    logger.debug(f"Url is already in db: {total_url}")
                    total_categories = [
                        {"file_url": total_url, "category_name": Categories.MONETARY_POLICY.value}
                    ]
                    if (total_url, Categories.MONETARY_POLICY.value) not in all_categories:
                        self.add_to_categories(total_categories)
                    continue
                to_process.append((total_url, date_txt))

                links_to_process[total_url] = []
                for a_name, a_link in a_tags.items():
                    if a_link == url:
                        continue
                    a_link = urljoin("https://www.federalreserve.gov", a_link)
                    links_to_process[total_url].append((a_name, a_link))
        
        for url, date_txt in to_process:
            logger.info(f"Processing: {url}")
            year = date_txt.split(" ")[-1]
            urlType, extension = self.classify_url(url)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=url,
                    scraping_time=str(scraping_time),
                    date_published_str=date_txt,
                )
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=year)
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, total_links = self.read_html(url, year, scraping_time,date_txt=date_txt)
            else:
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "file_url": url,
                "date_published_str": date_txt,
                "date_published": None,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [{"file_url": url, "category_name": Categories.MONETARY_POLICY.value}]
            self.add_all_atomic([result], total_categories, total_links)


        ## Beige Book
        current_year = pd.Timestamp.now().year
        urls = {
            year: f"https://www.federalreserve.gov/monetarypolicy/beigebook{year}.htm" for year in range(1996, current_year)
        }
        urls[current_year] = "https://www.federalreserve.gov/monetarypolicy/publications/beige-book-default.htm"
        
        to_process = []
        links_to_process = {}
        for year, main_url in urls.items():
            self.get(main_url)
            table = self.driver_manager.driver.find_element(By.XPATH, "//table/tbody")
            for tr in table.find_elements(By.XPATH, ".//tr"):
                tds = tr.find_elements(By.XPATH, ".//td")
                if len(tds) == 1:
                    a_tags = dict([(tag.text, tag.get_attribute("href")) for tag in tds[0].find_elements(By.XPATH, ".//a")])
                    if len(a_tags) == 0:
                        continue
                    date_txt  = tds[0].text.split(":")[0].strip()
                elif len(tds) == 2:
                    date_txt = tds[0].text
                    a_tags = dict([(tag.text, tag.get_attribute("href")) for tag in tds[1].find_elements(By.XPATH, ".//a")])
                    if len(a_tags) == 0:
                        continue
                else:
                    raise ValueError(f"Incorrect number of tds in tr in beige book year {year}, len {len(tds)}")
                url = a_tags.get("HTML", a_tags[list(a_tags.keys())[0]])
                date = pd.to_datetime(f"{date_txt} {year}")
                if url in all_urls:
                    logger.debug(f"Url is already in db: {url}")
                    total_categories = [
                        {"file_url": url, "category_name": Categories.MONETARY_POLICY.value}
                    ]
                    if (url, Categories.MONETARY_POLICY.value) not in all_categories:
                        self.add_to_categories(total_categories)
                    continue
                to_process.append((url, date))
                links_to_process[url] = []
                for a_name, a_link in a_tags.items():
                    if a_link == url:
                        continue
                    links_to_process[url].append((a_name, a_link))


        for url, date in to_process:
            logger.info(f"Processing: {url}")
            urlType, extension = self.classify_url(url)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=url,
                    scraping_time=str(scraping_time),
                    date_published=str(date),
                )
                main_id = self.download_and_upload_file(url, extension, main_metadata,  year=str(date.year))
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, total_links = self.read_html(url, str(date.year), scraping_time, date=date)
            else:
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "file_url": url,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [{"file_url": url, "category_name": Categories.MONETARY_POLICY.value}]
            self.add_all_atomic([result], total_categories, total_links)


        # Federal Reserve Balance Sheet Developments
        self.get("https://www.federalreserve.gov/monetarypolicy/publications/balance-sheet-developments-report.htm")
        to_process = []
        links_to_process = {}
        table = self.driver_manager.driver.find_element(By.XPATH, "//table/tbody")
        for tr in table.find_elements(By.XPATH, ".//tr"):
            tds = tr.find_elements(By.XPATH, ".//td")
            date_txt = tds[0].text
            a_tags = dict([(tag.text, tag.get_attribute("href")) for tag in tds[1].find_elements(By.XPATH, ".//a")])
            if len(a_tags) == 0:
                continue
            url = a_tags.get("HTML", a_tags[list(a_tags.keys())[0]])
            if url in all_urls:
                logger.debug(f"Url is already in db: {url}")
                total_categories = [
                    {"file_url": url, "category_name": Categories.MONETARY_POLICY.value}
                ]
                if (url, Categories.MONETARY_POLICY.value) not in all_categories:
                    self.add_to_categories(total_categories)
                continue
            to_process.append((url, date_txt))
            links_to_process[url] = []
            for a_name, a_link in a_tags.items():
                if a_link == url:
                    continue
                a_link = urljoin("https://www.federalreserve.gov", a_link)
                links_to_process[url].append((a_name, a_link))


        for url, date_txt in to_process:
            year = date_txt.split(" ")[1]
            urlType, extension = self.classify_url(url)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=url,
                    scraping_time=str(scraping_time),
                    date_published_str=date_txt,
                )
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=year)
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, total_links = self.read_html(url, year, scraping_time, date_txt=date_txt)
            else:
                if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {url}", extra={
                        "url": url,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            result = {
                "file_url": url,
                "date_published": date,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [{"file_url": url, "category_name": Categories.MONETARY_POLICY.value}]
            self.add_all_atomic([result], total_categories, total_links)


    def process_supervision_and_regulation(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        self.get("https://www.federalreserve.gov/publications/supervision-and-regulation-report.htm")
        xpath = "//div[@id='article']/div/*"
        elements = self.driver_manager.driver.find_elements(By.XPATH, xpath)[2:]
        # assert that the first is h4 tag
        assert elements[0].tag_name == "h4"
        year = int(elements[0].text)
        to_process = []
        links_to_process = {}
        for element in elements:
            if element.tag_name == "h4":
                year = int(element.text)
                continue
            if element.tag_name == "p":
                month = element.text.split(":")[0].strip()
                lines = element.text.split('\n')
                count_hlines = lines[1].count('|')
                links_count = count_hlines + 1
                name1 = lines[1].split(':')[0].strip()
                a_tags = [(a.text,a.get_attribute("href")) for a in element.find_elements(By.XPATH, ".//a")]
                parsed_links = {
                    name1: dict(a_tags[:links_count]),
                }
                if len(lines) > 2:
                    name2 = lines[2].split(':')[0].strip()
                    parsed_links[name2] = dict(a_tags[links_count:])
                url = parsed_links.get("Testimony", parsed_links.get("Report"))["HTML"]
                if url in all_urls:
                    logger.debug(f"Url is already in db: {url}")
                    total_categories = [
                        {"file_url": url, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}
                    ]
                    if (url, Categories.FINANCIAL_STABILITY_AND_REGULATION.value) not in all_categories:
                        self.add_to_categories(total_categories)
                    continue
                date_txt = f"{month} {year}"
                to_process.append((url, date_txt))
                links_to_process[url] = []
                for cat, d in parsed_links.items():
                    links_to_process[url].extend(
                        [
                            (f"{cat}_{name}", link)
                            for name, link in d.items() if link != url
                        ]
                    )
        

        for url, date_txt in to_process:
            logger.info(f"Processing: {url}")
            year = date_txt.split(" ")[1]
            scraping_time = pd.Timestamp.now()
            main_id, _ = self.read_html(url, year, scraping_time, date_txt=date_txt, parse_links=False)
            total_links = []
            if url in links_to_process:
                processed_links = self.process_links(main_id, lambda: links_to_process[url], year=year)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in processed_links
                ]
            total_categories = [{"file_url": url, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}]
            result = {
                "file_url": url,
                "date_published_str": date_txt,
                "date_published": None,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            self.add_all_atomic([result], total_categories, total_links)


    def process_financial_stability(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        self.get("https://www.federalreserve.gov/publications/financial-stability-report.htm")
        xpath = "//div[@id='article']/div/*"
        elements = self.driver_manager.driver.find_elements(By.XPATH, xpath)[3:]
        # assert that the first is h4 tag
        assert elements[0].tag_name == "h4"
        year = int(elements[0].text)
        to_process = []
        links_to_process = {}
        for element in elements:
            if element.tag_name == "h4":
                year = int(element.text)
                continue
            if element.tag_name == "p":
                month = element.text.split(":")[0].strip()
                a_tags = dict([(a.text,a.get_attribute("href")) for a in element.find_elements(By.XPATH, ".//a")])
                url = a_tags["HTML"]
                if url in all_urls:
                    logger.debug(f"Url is already in db: {url}")
                    total_categories = [
                        {"file_url": url, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}
                    ]
                    if (url, Categories.FINANCIAL_STABILITY_AND_REGULATION.value) not in all_categories:
                        self.add_to_categories(total_categories)
                    continue
                date_txt = f"{month} {year}"
                to_process.append((url, date_txt))
                links_to_process[url] = [
                    (a_name, a_link)
                    for a_name, a_link in a_tags.items() if a_link != url
                ]
        
        result = []
        total_categories = []
        total_links = []
        for url, date_txt in to_process:
            logger.info(f"Processing: {url}")
            year = date_txt.split(" ")[1]
            scraping_time = pd.Timestamp.now()
            main_id, _ = self.read_html(url, year, scraping_time, date_txt=date_txt, parse_links=False)
            total_links = []
            if url in links_to_process:
                processed_links = self.process_links(main_id, lambda: links_to_process[url], year=year)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in processed_links
                ]
            total_categories = [{"file_url": url, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}]
            result = {
                "file_url": url,
                "date_published_str": date_txt,
                "date_published": None,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            self.add_all_atomic([result], total_categories, total_links)


    def process_payments_system(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        # Federal Reserve Payments Study (FRPS)
        self.get("https://www.federalreserve.gov/paymentsystems/frps_previous.htm")
        xpath = "//div[@id='article']/*"
        elements = self.driver_manager.driver.find_elements(By.XPATH, xpath)[2:]


        to_process = []
        links_to_process = {}
        date_txt = None
        for element in elements:
            if element.tag_name == "p" and "Released" in element.text:
                date_txt = element.text.split("Released ")[1]
                continue
            elif element.tag_name == "ul":
                a_tags = sorted([ (a.text, a.get_attribute("href")) for a in element.find_elements(By.XPATH, ".//a")], key=lambda x: x[0])
                url = None
                for (name, href) in a_tags:
                    if name == "HTML":
                        url = href
                        break
                    if name == "PDF":
                        url = href
                if url is None:
                    raise ValueError("No HTML link found")
                if url in all_urls:
                    logger.debug(f"Url is already in db: {url}")
                    total_categories = [
                        {"file_url": url, "category_name": Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value}
                    ]
                    if (url, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value) not in all_categories:
                        self.add_to_categories(total_categories)
                    continue

                to_process.append((url, date_txt))
                links_to_process[url] = [
                    (name, href)
                    for name, href in a_tags if href != url
                ]


        for url, date_txt in to_process:
            year = date_txt.split(" ")[1]
            urlType, extension = self.classify_url(url)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=url,
                    scraping_time=str(scraping_time),
                    date_published_str=date_txt,
                )
                main_id = self.download_and_upload_file(url, extension, main_metadata, year=year)
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, _ = self.read_html(url, year, scraping_time, date_txt=date_txt, parse_links=False)
                processed_links = self.process_links(main_id, lambda: links_to_process[url], year=year)
                total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in processed_links
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
                "file_url": url,
                "date_published": None,
                "date_published_str": date_txt,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [{"file_url": url, "category_name": Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value}]
            self.add_all_atomic([result], total_categories, total_links)


    def process_economic_reserach(self):
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]

        # Finance and Economics Discussion Series (FEDS)
        # https://www.federalreserve.gov/econres/feds/index.htm
        main_url = "https://www.federalreserve.gov/econres/feds/{}.htm"
        current_year = pd.Timestamp.now().year
        xpath = "//div[@id='article']/div"
        cat = [
            Categories.RESEARCH_AND_DATA.value
        ]
        for year in range(1996, current_year + 1):
            to_process = []
            self.get(main_url.format(year))
            papers = self.driver_manager.driver.find_elements(By.XPATH, xpath)[1:]
            for paper in papers:
                tag_times = paper.find_elements(By.XPATH, ".//time")
                if len(tag_times) == 0:
                    continue
                date_txt = tag_times[0].get_attribute("datetime")
                href = paper.find_element(By.XPATH, ".//h5/a").get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    total_categories = [
                        {
                            "file_url": href,
                            "category_name": category
                        } for category in cat if (href, category) not in all_categories
                    ]
                    if len(total_categories) > 0:
                        self.add_to_categories(total_categories)
                    continue
                to_process.append((href, date_txt))

            for url, date_txt in to_process:
                year = date_txt.split(" ")[1]
                logger.info(f"Processing: {url}")
                scraping_time = pd.Timestamp.now()
                main_id, total_links = self.read_html(url, year,scraping_time, date_txt=date_txt)
                
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": category
                    } for category in cat
                ]
                
                result = {
                    "file_url": url,
                    "date_published_str": date_txt,
                    "date_published": None,
                    "scraping_time": scraping_time,
                    "file_id": main_id,
                }
                self.add_all_atomic([result], total_categories, total_links)


        # FEDS Notes
        # https://www.federalreserve.gov/econres/notes/feds-notes/default.htm
        main_url = "https://www.federalreserve.gov/econres/notes/feds-notes/{}-index.htm"
        current_year = pd.Timestamp.now().year
        xpath = "//div[@id='article']/div"
        cat = [
            Categories.RESEARCH_AND_DATA.value
        ]
        for year in range(2013, current_year + 1):
            to_process = []
            self.get(main_url.format(year))
            papers = self.driver_manager.driver.find_elements(By.XPATH, xpath)[1:]
            for paper in papers:
                tag_times = paper.find_elements(By.XPATH, ".//time")
                if len(tag_times) == 0:
                    continue
                date = tag_times[0].get_attribute("datetime")
                if not date:
                    # read the inside text
                    date = tag_times[0].text.strip()
                date = pd.to_datetime(date)
                href = paper.find_element(By.XPATH, ".//h5/a").get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    total_categories = [
                        {
                            "file_url": href,
                            "category_name": category
                        } for category in cat if (href, category) not in all_categories
                    ]
                    if len(total_categories) > 0:
                        self.add_to_categories(total_categories)
                    continue
                to_process.append((href, date, year))

            for url, date, year in to_process:
                logger.info(f"Processing: {url}")
                scraping_time = pd.Timestamp.now()
                main_id, total_links = self.read_html(url, str(year), scraping_time, date=date)
                
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": category
                    } for category in cat
                ]
                result = {
                    "file_url": url,
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_id": main_id,
                }
                self.add_all_atomic([result], total_categories, total_links)

        
        #International Finance Discussion Papers (IFDP)
        # https://www.federalreserve.gov/econres/ifdp/index.htm
        main_url = "https://www.federalreserve.gov/econres/ifdp/{}.htm"
        current_year = pd.Timestamp.now().year
        xpath = "//div[@id='article']/div"
        cat = [
            Categories.RESEARCH_AND_DATA.value
        ]
        for year in range(1971, current_year + 1):
            to_process = []
            self.get(main_url.format(year))
            papers = self.driver_manager.driver.find_elements(By.XPATH, xpath)[1:]
            for paper in papers:
                tag_times = paper.find_elements(By.XPATH, ".//time")
                if len(tag_times) == 0:
                    continue
                date = pd.to_datetime(tag_times[0].get_attribute("datetime"))
                href = paper.find_element(By.XPATH, ".//h5/a").get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    total_categories = [
                        {
                            "file_url": href,
                            "category_name": category
                        } for category in cat if (href, category) not in all_categories
                    ]
                    if len(total_categories) > 0:
                        self.add_to_categories(total_categories)
                    continue
                to_process.append((href, date))

            for url, date in to_process:
                year = str(date.year)
                logger.info(f"Processing: {url}")
                scraping_time = pd.Timestamp.now()
                main_id, total_links = self.read_html(url, year, scraping_time, date=date)
                
                total_categories = [
                    {
                        "file_url": url,
                        "category_name": category
                    } for category in cat
                ]
                result = {
                    "file_url": url,
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_id": main_id,
                }
                self.add_all_atomic([result], total_categories, total_links)




    def process_consumers_and_communities(self):
        # Consumer Affairs Letters
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        
        main_url = "https://www.federalreserve.gov/supervisionreg/caletters/{}.htm"
        current_year = pd.Timestamp.now().year
        xpath = "//div[@id='article']/div[@class='row']"
        for year in range(1997, current_year + 1):
            to_process = []
            self.get(main_url.format(year))
            cas = self.driver_manager.driver.find_elements(By.XPATH, xpath)
            for ca in cas[1:]:
                try:
                    a_tag = ca.find_element(By.XPATH, ".//a")
                except selenium.common.exceptions.NoSuchElementException:
                    logger.warning(f"This ca has no link for {year}")
                    continue

                href = a_tag.get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Url is already in db: {href}")
                    if (href, Categories.FINANCIAL_STABILITY_AND_REGULATION.value) not in all_categories:
                        self.add_to_categories([
                        {"file_url": href, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}
                    ])
                    continue
                to_process.append(href)
            for href in to_process:
                logger.info(f"Processing: {href}")
                # get date
                self.get(href)
                # try p with class "date"
                
                if len(p_dates := self.driver_manager.driver.find_elements(By.XPATH, "//p[@class='date']")) == 1:
                    date_txt = p_dates[0].text
                elif len(div_dates := self.driver_manager.driver.find_elements(By.XPATH, "//div[@class='date_text']")) == 1:
                    date_txt = div_dates[0].text
                elif '--' in self.driver_manager.driver.title:
                    date_txt = self.driver_manager.driver.title.split('--')[1].strip()
                elif len(p_center := self.driver_manager.driver.find_elements(By.XPATH, "//p[@align='center']")) == 1:
                    date_txt = p_center[0].text
                elif len(div_col := self.driver_manager.driver.find_elements(By.XPATH, "//div[@id='article']/div/div[@class='col-xs-12 col-sm-4']/strong")) >= 2:
                    date_txt = div_col[1].text
                else:
                    raise ValueError("No date found")
                if '\n' in date_txt:
                    date_txt = date_txt.split('\n')[0]
                date = pd.to_datetime(date_txt)
                scraping_time = pd.Timestamp.now()
                main_id, total_links = self.read_html(href, str(year), scraping_time, date=date, load_page=False)
                total_categories = [{"file_url": href, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}]
                result = {
                    "file_url": href,
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_id": main_id,
                }
                self.add_all_atomic([result], total_categories, total_links)
        
        # Enforcement Actions & Legal Developments
        # download csv to pandas Dataframe from "https://www.federalreserve.gov/supervisionreg/files/enforcementactions.csv"
        csv_url = "https://www.federalreserve.gov/supervisionreg/files/enforcementactions.csv"
        ret = requests.get(csv_url)
        if ret.status_code != 200:
            raise ValueError(f"Error downloading csv from {csv_url}")
        df = pd.read_csv(io.StringIO(ret.text))
        df["Effective Date"] = pd.to_datetime(df["Effective Date"])
        # drop NaN urls
        df = df.dropna(subset=["URL"])
        result = []
        total_links = []
        total_categories = []
        for idx, row in df.iterrows():
            href = urljoin("https://www.federalreserve.gov", row["URL"])
            if href in all_urls:
                logger.debug(f"Url is already in db: {href}")
                if (href, Categories.FINANCIAL_STABILITY_AND_REGULATION.value) not in all_categories:
                    self.add_to_categories([
                    {"file_url": href, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}
                ])
                continue
            logger.info(f"Processing: {href}")
            urlType, extension = self.classify_url(href)
            allowed_outside = False
            extType = classify_extension(extension)
            total_links = []
            scraping_time = pd.Timestamp.now()
            year = str(row["Effective Date"].year)
            if extType == ExtensionType.FILE:
                main_metadata = MainMetadata(
                    url=href,
                    scraping_time=str(scraping_time),
                    date_published=str(row["Effective Date"]),
                )
                main_id = self.download_and_upload_file(href, extension, main_metadata, year=year)
                if main_id is None:
                    continue
            elif extType == ExtensionType.WEBPAGE:
                main_id, total_links = self.read_html(href, year, scraping_time, date=row["Effective Date"])
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
                "date_published": row["Effective Date"],
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [{"file_url": href, "category_name": Categories.FINANCIAL_STABILITY_AND_REGULATION.value}]
            self.add_all_atomic([result], total_categories, total_links)

        
    def read_html(self, url: str, year, scraping_time, date = None, date_txt = None, load_page=True, parse_links=True):
        if load_page:
            self.get(url)
        
        elements = self.driver_manager.driver.find_elements(By.XPATH, "//*[@id='content']")
        if len(elements) == 0:
            # old page try main='content'
            elements = self.driver_manager.driver.find_elements(By.XPATH, "//body")
        if len(elements) == 0:
            raise ValueError(f"No content found in HTML file, {url}")
        element = elements[0]
        main_metadata = MainMetadata(
            url=url,
            scraping_time=str(scraping_time),
            date_published=str(date) if date is not None else None,
            date_published_str=date_txt if date_txt is not None else None,
        )
        main_id = self.process_html_page(main_metadata, year)
        if not parse_links:
            return main_id, []
        # find all links and download them
        def f_get_links():
            links = []
            for link in element.find_elements(By.XPATH, ".//a"):
                link_text = link.get_attribute("textContent").strip()
                link_url = link.get_attribute("href")
                if link_url is None:
                    continue
                parsed_link = urlparse(link_url)
                if any(ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS):
                                continue
                links.append((link_text, link_url))
            return links
        processed_links = self.process_links(main_id, f_get_links, year=year)
        total_links = [
                    {
                        "file_url": url,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in processed_links
                ]
        return main_id,  total_links
    
    def process_all_years(self):
        self.process_FOMC()
        self.process_news_events()
        self.process_monetary_policy()
        self.process_supervision_and_regulation()
        self.process_financial_stability()
        self.process_payments_system()
        self.process_economic_reserach()
        self.process_consumers_and_communities()
