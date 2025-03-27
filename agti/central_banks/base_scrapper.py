import socket
import logging
import time
import random
from urllib.parse import urlparse
import pandas as pd
from sqlalchemy import text
from selenium.webdriver.support import expected_conditions as EC
from agti.utilities.settings import CredentialManager
from agti.utilities.db_manager import DBConnectionManager


__all__ = ["BaseBankScraper"]

logger = logging.getLogger(__name__)

class BaseBankScraper:
    """Base class for bank scrapers with common functionality."""


    registry = {}

    COUNTRY_CODE_ALPHA_3 = None  # Set in child classes
    COUNTRY_NAME = None  # Set in child classes

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Automatically register the subclass
        BaseBankScraper.registry[cls.__name__] = cls

    def __init__(self,driver_manager, pw_map, user_name, table_name, min_sleep, max_sleep):
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self.pw_map = pw_map
        self.user_name = user_name
        self.table_name = table_name
        self.driver_manager = driver_manager
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()
        self._header_to_use = None

        self._cookies = None

    def get(self, url):
        # random sleep time to mimic human behavior
        time.sleep(random.uniform(self.min_sleep, self.max_sleep))
        self.driver_manager.driver.get(url)

    def initialize_cookies(self):
        raise NotImplementedError

    def get_headers(self):
        return self.driver_manager.headers
    
    def get_cookies(self):
        if self._cookies is None:
            raise ValueError("Cookies not initialized.")
        return self._cookies
    
    def get_driver(self):
        return self.driver_manager.driver
        

    def ip_hostname(self):
        """Retrieve machine IP and hostname."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname
    
    def get_category_table_name(self):
        return f"{self.table_name}_categories"
    
    def get_links_table_name(self):
        return f"{self.table_name}_links"

    def get_all_db_urls(self):
        """Retrieve all URLs already stored in the database."""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        query = text(f"SELECT file_url FROM {self.table_name} WHERE country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.COUNTRY_CODE_ALPHA_3}
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            output =  [row[0] for row in rs.fetchall()]
        return output
        
    def get_all_db_categories(self):
        """Retrieve all categories already stored in the database."""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        # query select all from categories table, which joined with self.table_name over file_url has country_code_alpha_3
        table_name = self.get_category_table_name()
        query = text(f"SELECT {table_name}.file_url, {table_name}.category_name FROM {table_name} " +  \
                    f"INNER JOIN {self.table_name} ON {table_name}.file_url = {self.table_name}.file_url " + \
                    f"WHERE {self.table_name}.country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.COUNTRY_CODE_ALPHA_3}
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            output =  [row for row in rs.fetchall()]
        return output
    
    def get_all_db_links(self):
        """Retrieve all links already stored in the database."""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        links_table_name = self.get_links_table_name()
        query = text(f"SELECT {links_table_name}.file_url, {links_table_name}.link_url FROM {links_table_name} " +  \
            f"INNER JOIN {self.table_name} ON {links_table_name}.file_url = {self.table_name}.file_url " + \
            f"WHERE {self.table_name}.country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.COUNTRY_CODE_ALPHA_3}
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            output =  [row for row in rs.fetchall()]
        return output
    

    def add_to_db(self, data, dbconnx=None):
        """Store scraped data into the database."""
        df = pd.DataFrame(data)
        if df.empty:
            logger.info("No new data found.")
            return
        
        ipaddr, hostname = self.ip_hostname()
        df["country_name"] = self.COUNTRY_NAME
        df["country_code_alpha_3"] = self.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        # check for uniques of URLS of file_url
        duplicated = df["file_url"].duplicated()
        # log each duplicated file_url with date published
        for _, row in df[duplicated].iterrows():
            url = row["file_url"]
            date_published = row["date_published"]
            count_duplicates = df[df["file_url"] == url].shape[0]
            logger.debug(f"Duplicate file_url found: {url} with {count_duplicates} entries",
                           extra={
                                "date_published": date_published,
                                "url": url,
                                "count_duplicates": count_duplicates
                           })
        # drop duplicates on file_url
        df = df.drop_duplicates(subset=["file_url"])

        # verify if all file_url are not already in the database
        db_urls = self.get_all_db_urls()
        # all should be new otherwise raise warning for each not new url
        for _, row in df.iterrows():
            url = row["file_url"]
            date_published = row["date_published"]
            if url in db_urls:
                logger.debug(f"URL already in database: {url}", extra={
                    "date_published": date_published,
                    "url": url})

        # drop all urls in database on file_url
        df = df[~df["file_url"].isin(db_urls)] 
        logger.info(f"Adding {df.shape[0]} new entries to the database.")
        if dbconnx is None:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)

    def add_to_categories(self, data, dbconnx=None):
        """Store scraped data into categories table."""
        df = pd.DataFrame(data)
        if df.empty:
            logger.info("No new data found for categories.")
            return
        
        # duplicated file_url and category_name
        duplicated = df[["file_url", "category_name"]].duplicated()
        # log each duplicated file_url with date published
        for _, row in df[duplicated].iterrows():
            url = row["file_url"]
            category_name = row["category_name"]
            logger.debug(f"Duplicate file_url and category_name found: {url} - {category_name}",
                           extra={
                                "url": url,
                                "category_name": category_name
                           })
        # drop duplicates on file_url and category_name
        df = df.drop_duplicates(subset=["file_url", "category_name"])

        # verify unique over whole table
        db_categories = self.get_all_db_categories()
        db_categories = [(url, category_name) for url, category_name in db_categories]
        # all should be new otherwise raise warning for each not new url
        for _, row in df.iterrows():
            url = row["file_url"]
            category_name = row["category_name"]
            if (url, category_name) in db_categories:
                logger.debug(f"URL and category_name already in database: {url} - {category_name}", extra={
                    "url": url,
                    "category_name": category_name})
        
        # drop all urls in database on file_url
        df = df[~df[["file_url", "category_name"]].apply(tuple,axis=1).isin(db_categories)]


        logger.info(f"Adding {df.shape[0]} new entries to the categories table.")
        if dbconnx is None:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        table_name = self.get_category_table_name()
        df.to_sql(table_name, con=dbconnx, if_exists="append", index=False)


    def add_to_links(self, data, dbconnx=None):
        """Store scraped data into links table."""
        df = pd.DataFrame(data)
        if df.empty:
            logger.info("No new data found for links.")
            return
        
        # we require UNIQUE (file_url, link_url) pairs only from the df
        # duplicated file_url and link_url
        duplicated = df.duplicated(subset=["file_url", "link_url"])
        # log each duplicated file_url with date published
        for _, row in df[duplicated].iterrows():
            url = row["file_url"]
            link_url = row["link_url"]
            logger.debug(f"Duplicate file_url and link_url found: {url} - {link_url}",
                           extra={
                                "url": url,
                                "link_url": link_url
                           })
        # drop duplicates on file_url and link_url
        df = df.drop_duplicates(subset=["file_url", "link_url"])

        # verify unique over whole table
        db_links = self.get_all_db_links()
        db_links = [(url, link_url) for url, link_url in db_links]
        # all should be new otherwise raise warning for each not new url
        for _, row in df.iterrows():
            url = row["file_url"]
            link_url = row["link_url"]
            if (url, link_url) in db_links:
                logger.debug(f"URL and link_url already in database: {url} - {link_url}", extra={
                    "url": url,
                    "link_url": link_url})
            
        # drop all urls in database on file_url
        df = df[~df[["file_url", "link_url"]].apply(tuple,axis=1).isin(db_links)]

        logger.info(f"Adding {df.shape[0]} new entries to the links table.")
        if dbconnx is None:
            dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        table_name = self.get_links_table_name()
        df.to_sql(table_name, con=dbconnx, if_exists="append", index=False)



    def add_all_atomic(self, data, tags, links):
        """Store scraped data into the database."""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        with dbconnx.begin() as connection:
            self.add_to_db(data,dbconnx=connection)
            self.add_to_categories(tags,dbconnx=connection)
            self.add_to_links(links,dbconnx=connection)
        