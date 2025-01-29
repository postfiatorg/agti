
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
    COUNTRY_CODE_ALPHA_3 = "ENG"
    COUNTRY_NAME = "England"

    def __init__(self, pw_map, user_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()

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
                raise Exception(f"Taxonomy {type_filters_to_check} not checked")
            

        
            

    

    def process_research_blog(self, href):
        print("Processing research blog")


    def process_speeches(self, href):
        print("Processing speeches")


    # publication
    def process_working_papers(self, href):
        print("Processing working paper")

    # publication
    def process_monetery_policy_reports(self, href):
        print("Processing monetary policy report")

    # publication
    def process_monetary_policy_committee(self, href):
        print("Processing monetary policy committee")

    def process_all_years(self):
        self.init_filter()
        #self.pageBottom()
        print(self.get_current_page())
        self.go_to_next_page()
        print(self.get_current_page())
        self.go_to_next_page()
        print(self.get_current_page())
        # get id = SearchResults div
        search_results = self._driver.find_element(By.ID, "SearchResults")
        # find all elements with class="col3"
        elements = search_results.find_elements(By.XPATH, ".//div[@class='col3']")
        for element in elements:
            # get a href
            a = element.find_element(By.TAG_NAME, "a")
            href = a.get_attribute("href")

            # get date using time tag with datetime attribute
            time_tag = element.find_element(By.TAG_NAME, "time")
            date = time_tag.get_attribute("datetime")
            print(date, href)



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
        wait.until(EC.staleness_of(self._driver.find_element(By.XPATH, xpath)))
    
    


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