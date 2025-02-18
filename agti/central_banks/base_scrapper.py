import socket
import logging
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

    def __init__(self,driver, pw_map, user_name, table_name):
        self.pw_map = pw_map
        self.user_name = user_name
        self.table_name = table_name
        self._driver = driver
        self.db_connection_manager = DBConnectionManager(pw_map=self.pw_map)
        self.credential_manager = CredentialManager()
        self.datadump_directory_path = self.credential_manager.get_datadump_directory_path()

    def ip_hostname(self):
        """Retrieve machine IP and hostname."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname
    
    def get_category_table_name(self):
        return f"{self.table_name}_categories"

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

    def add_to_db(self, data):
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
            logger.warning(f"Duplicate file_url found: {url} with {count_duplicates} entries",
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
                logger.warning(f"URL already in database: {url}", extra={
                    "date_published": date_published,
                    "url": url})

        # drop all urls in database on file_url
        df = df[~df["file_url"].isin(db_urls)] 
        logger.info(f"Adding {df.shape[0]} new entries to the database.")
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)

    def add_to_categories(self, data):
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
            logger.warning(f"Duplicate file_url and category_name found: {url} - {category_name}",
                           extra={
                                "url": url,
                                "category_name": category_name
                           })
        # drop duplicates on file_url and category_name
        df = df.drop_duplicates(subset=["file_url", "category_name"])

        # verify unique over whole table
        db_categories = self.get_all_db_categories()
        # all should be new otherwise raise warning for each not new url
        for _, row in df.iterrows():
            url = row["file_url"]
            category_name = row["category_name"]
            if (url, category_name) in db_categories:
                logger.warning(f"URL and category_name already in database: {url} - {category_name}", extra={
                    "url": url,
                    "category_name": category_name})
        
        # drop all urls in database on file_url
        df = df[~df[["file_url", "category_name"]].isin(db_categories).all(axis=1)]


        logger.info(f"Adding {df.shape[0]} new entries to the categories table.")
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        table_name = self.get_category_table_name()
        df.to_sql(table_name, con=dbconnx, if_exists="append", index=False)