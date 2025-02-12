import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import download_and_read_pdf


logger = logging.getLogger(__name__)

__all__ = ["EnglandBankScrapper"]

class EnglandBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "ENG"
    COUNTRY_NAME = "England"

    def init_filter(self):
        self._driver.get(self.get_base_url())
        wait = WebDriverWait(self._driver, 10)
        # wait for cookie banner to appear and find it by class "cookie__button btn btn-default btn-neutral" using xpath
        xpath = "//button[@class='cookie__button btn btn-default btn-neutral']"

        success = False
        repeat = 3
        for i in range(repeat):
            try:
                cookie_btn = wait.until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                # click the cookie banner
                cookie_btn.click()
                success = True
                break
            except Exception as e:
                if repeat != (repeat - 1):
                    logger.warning(f"Could not click cookie banner repeating", exc_info=True)
                else:
                    logger.exception(f"Could not click cookie banner", exc_info=True)
                    
        if not success:
            raise Exception("Can not click cookie banner")

        wait.until(EC.visibility_of_element_located((By.ID, "SearchResults")))

        def iterate_over_labels(filter_labels, filters_list, check_staleness):
            for label in filter_labels:
                name = label.text.strip()
                if name in filters_list:
                    #self._driver.execute_script("arguments[0].scrollIntoView(false);", label)
                    label = wait.until(EC.element_to_be_clickable(label))
                    label.click()
                    if check_staleness:
                        wait.until(EC.staleness_of(label))
                    filters_list.remove(name)
                    return True
            return False

        type_filters_to_check = set(["Research blog", "Publication", "Speech"])
        while len(type_filters_to_check) > 0:
            # search div with class "sidebar-filters type-filters" using xpath
            filter_div = self._driver.find_element(By.XPATH, "//div[@class='sidebar-filters type-filters']")

            # find all labels elements in the filter div
            filter_labels = filter_div.find_elements(By.TAG_NAME, "label")
            if not iterate_over_labels(filter_labels, type_filters_to_check,False):
                raise Exception(f"Filter {type_filters_to_check} not checked")

        wait.until(EC.visibility_of_all_elements_located((By.ID, "SearchResults")))

        taxonomy_filters_to_check = set(["Monetary Policy Committee (MPC)", "Monetary policy"])
        xpath = "//div[@class='sidebar-filters taxonomy-filters']"
        # search div with class "sidebar-filters taxonomy-filters" using xpath
        while len(taxonomy_filters_to_check) > 0:
            
            wait.until(EC.visibility_of_all_elements_located((By.XPATH, xpath)))
            filter_div = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
            
        
            filter_labels = filter_div.find_elements(By.TAG_NAME, "label")
            if not iterate_over_labels(filter_labels, taxonomy_filters_to_check,True):
                raise Exception(f"Taxonomy {taxonomy_filters_to_check} not checked")
            
        # wait for the page to load
        wait.until(EC.visibility_of_all_elements_located((By.ID, "SearchResults")))
    

    def find_text_and_pdfs(self, tag, href):
        pdf_links = []
        all_text = ""
        self._driver.get(href)
        if '//' not in tag:
            #type_tag = tag
            taxonomy_tag = None
        else:
            _, taxonomy_tag = tag.split(" // ")

            
        if taxonomy_tag == "Inflation Report (IR)" or taxonomy_tag == "Centre for Central Banking Studies (CCBS)":
            # find a tag with class="btn btn-pubs btn-has-img btn-lg link-image" using xpath
            xpath = "//a[@class='btn btn-pubs btn-has-img btn-lg link-image']"
            try:
                a = self._driver.find_element(By.XPATH, xpath)
                pdf_links.append(a.get_attribute("href"))
                return all_text, pdf_links
            except:
                # special cases
                if href == "https://www.bankofengland.co.uk/inflation-report/2017/november-2017-visual-summary":
                    pdf_links = ["https://www.bankofengland.co.uk/-/media/boe/files/inflation-report/2017/nov.pdf?la=en&hash=950B4B1481D081CA035FC076CF9FFFFB08F658A6"]
                    return all_text, pdf_links
                logger.warning(f"Can not find pdf link: {tag} {href}")
                return None
                    
        try:
            # get div by class="published-date"
            date_div = self._driver.find_element(By.CLASS_NAME, "published-date")
            # get parent
            p_section = date_div.find_element(By.XPATH, "./parent::div[@class='col9' or @class='col12']/parent::div[@class='container']/parent::section")
            all_text = p_section.text
            pdf_links = [a.get_attribute("href") for a in p_section.find_elements(By.XPATH, ".//a[contains(@href, '.pdf')]")]
            # drop duplicates
            pdf_links = list(set(pdf_links))
            if len(all_text) >= 300 or len(pdf_links) != 0:
                return all_text, pdf_links
            

            all_text = ""
            try:
                container = self._driver.find_element(By.XPATH, "//div[@class='container container-has-navigation']/div[@class='container-publication']")
                all_text = container.text
                if len(all_text) < 300:
                    logger.warning(f"Text is too short: {tag} {href}")
                    return None
                return all_text, pdf_links
                
            except:
                # we dafault to pdf links
                # find all a tags with class="btn btn-pubs btn-has-img btn-lg link-image" or "btn btn-pubs btn-has-img btn-lg"
                xpath = "//a[@class='btn btn-pubs btn-has-img btn-lg link-image' or @class='btn btn-pubs btn-has-img btn-lg']"
                
                a_tags = self._driver.find_elements(By.XPATH, xpath)
                for a_tag in a_tags:
                    time.sleep(0.01)
                    pdf_links.append(a_tag.get_attribute("href"))
                if len(a_tags) == 0:
                    logger.warning(f"Missing content and publish date and pdf files: {tag} {href}")
                    return None
                return all_text, pdf_links
        except:
            pass
        logger.warning(f"Can not find content: {tag} {href}")
        return None


    def process_all_years(self):
        self.init_filter()
        year = pd.Timestamp.now().year

        to_process = []
        while year >= 1998:
            # get id = SearchResults div
            search_results = self._driver.find_element(By.ID, "SearchResults")
            # find all elements with class="col3"
            elements = search_results.find_elements(By.XPATH, ".//div[@class='col3']")
            for element in elements:
                a = element.find_element(By.TAG_NAME, "a")
                href = a.get_attribute("href")

                # tag is under a in class="release-tag" div 
                tag = a.find_element(By.CLASS_NAME, "release-tag-wrap").text

                # get date using time tag with datetime attribute
                time_tag = element.find_element(By.TAG_NAME, "time")
                date = pd.to_datetime(time_tag.get_attribute("datetime"))
                to_process.append((tag, href, date))
                year = min(year, date.year)
            
            self.go_to_next_page()



        all_urls = self.get_all_db_urls()
        output = []
        for tag, href, date in to_process:
            if href in all_urls:
                logger.info(f"Href is already in db: {href}")
                continue
            logger.info(f"Processing: {href}")
            if (data := self.find_text_and_pdfs(tag, href)) is not None:
                text, pdf_links = data
                total_text = text
                # NOTE: handle mutiple data in different tables
                for pdf_link in pdf_links:
                    pdf_text = download_and_read_pdf(pdf_link, self.datadump_directory_path)
                    total_text += "\n######## PDF FILE START ########\n" + pdf_text + "\n######## PDF FILE END ########\n"

            output.append({
                "date_published": date,
                "scraping_time": pd.Timestamp.now(),
                "file_url": href,
                "full_extracted_text": total_text,
            })

        self.add_to_db(output)

                
                
            



    def get_current_page(self):
        wait = WebDriverWait(self._driver, 10)
        # find list-pagination__link list-pagination__link--page list-pagination__link--is-current
        xpath = "//a[@class='list-pagination__link list-pagination__link--page list-pagination__link--is-current']"
        current_page = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
        # get data-page-link attribute
        return int(current_page.get_attribute("data-page-link"))
    

    def go_to_next_page(self):
        wait = WebDriverWait(self._driver, 10)
        current_page = self.get_current_page()
        # find a href with data-page-link attribute = current_page + 1
        xpath = f"//a[@data-page-link='{current_page + 1}']"
        next_page = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        next_page.click()
        # wait for finish loading class list-pagination ul
        wait.until(EC.visibility_of_all_elements_located((By.ID, "SearchResults")))



    def get_base_url(self) -> str:
        return "https://www.bankofengland.co.uk/news"