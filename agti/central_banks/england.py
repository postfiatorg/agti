
import os
import re
import socket
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import urllib
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pdfplumber
from sqlalchemy import text



class EnglandBankScrapper:
    """
    Issues
    - speech as pdf with graphs only (https://www.bankofengland.co.uk/speech/2024/november/swati-dhingra-panellist-at-third-boe-watchers-conference-inflation-dynamics)

    
    """
    COUNTRY_CODE_ALPHA_3 = "ENG"
    COUNTRY_NAME = "England"

    def __init__(self, pw_map, user_name, table_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()
        self.table_name = table_name

        self._driver = self._setup_driver()

    def ip_hostname(self):
        hostname = socket.gethostname()
        IPAddr = socket.gethostbyname(hostname)
        return IPAddr, hostname


    def _setup_driver(self):
        driver = webdriver.Firefox()
        return driver
    
    def download_and_read_pdf(self, url: str) -> str:
        filename = os.path.basename(url)

        urllib.request.urlretrieve(url, self.datadump_directory_path / filename)

        with pdfplumber.open(self.datadump_directory_path / filename) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text()

        os.remove(self.datadump_directory_path / filename)

        return text
    
    def get_all_dates(self):
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(user_name=self.user_name)
        query = text("""
SELECT date_created 
FROM central_banks 
WHERE country_code_alpha_3 = :country_code_alpha_3
""")
        params = {
            "country_code_alpha_3": EnglandBankScrapper.COUNTRY_CODE_ALPHA_3
        }
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]
    

    def __del__(self):
        self._driver.close()

    
    def init_filter(self):
        self._driver.get(self.get_base_url())
        wait = WebDriverWait(self._driver, 10)
        # wait for cookie banner to appear and find it by class "cookie__button btn btn-default btn-neutral" using xpath
        xpath = "//button[@class='cookie__button btn btn-default btn-neutral']"
        cookie_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        # click the cookie banner
        cookie_btn.click()

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



        taxonomy_filters_to_check = set(["Monetary Policy Committee (MPC)", "Monetary policy"])
        xpath = "//div[@class='sidebar-filters taxonomy-filters']"
        # search div with class "sidebar-filters taxonomy-filters" using xpath
        while len(taxonomy_filters_to_check) > 0:
            
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
                print("Cant not find pdf link: ", tag, href)
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
                    print("No text find on container-publication")
                    return None
                return all_text, pdf_links
                
            except:
                # we dafault to pdf links
                # find all a tags with class="btn btn-pubs btn-has-img btn-lg link-image" or "btn btn-pubs btn-has-img btn-lg"
                xpath = "//a[@class='btn btn-pubs btn-has-img btn-lg link-image' or @class='btn btn-pubs btn-has-img btn-lg']"
                
                a_tags = self._driver.find_elements(By.XPATH, xpath)
                for a_tag in a_tags:
                    pdf_links.append(a_tag.get_attribute("href"))
                if len(a_tags) == 0:
                    print("Missing content and publish date and pdf files: ", tag, href)
                    return None
                return all_text, pdf_links
        except:
            pass
        print("Not working: ", tag, href)
        return None


    def process_all_years(self):
        """
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

        
        import pickle
        with open("to_process.pickle", "wb") as f:
            pickle.dump(to_process, f)
        """
        import pickle
        with open("to_process.pickle", "rb") as f:
            to_process = pickle.load(f)

        # This is how we scrape the data
        # 1. use published date and get text + pdf files
        # if len(text) < 300 and len(pdf_links) == 0:
        # 2. we use "container container-has-navigation" and get text 
        # 3. 
        
        for tag, href, date in to_process:
            self.find_text_and_pdfs(tag, href)
            



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
    
    


    def pageBottom(self):
        bottom=False
        a=0
        while not bottom:
            new_height = self._driver.execute_script("return document.body.scrollHeight")
            self._driver.execute_script(f"window.scrollTo(0, {a});")
            if a > new_height:
                bottom=True
            time.sleep(0.001)
            a+=5



    def get_base_url(self) -> str:
        return "https://www.bankofengland.co.uk/news"