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

    def get_all_db_urls(self):
        """Retrieve all URLs already stored in the database."""
        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        query = text(f"SELECT file_url FROM {self.table_name} WHERE country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.COUNTRY_CODE_ALPHA_3}
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            return [row[0] for row in rs.fetchall()]

    def add_to_db(self, data):
        """Store scraped data into the database."""
        df = pd.DataFrame(data)
        if df.empty:
            print("No new data found.")
            return
        
        ipaddr, hostname = self.ip_hostname()
        df["country_name"] = self.COUNTRY_NAME
        df["country_code_alpha_3"] = self.COUNTRY_CODE_ALPHA_3
        df["scraping_machine"] = hostname
        df["scraping_ip"] = ipaddr

        dbconnx = self.db_connection_manager.spawn_sqlalchemy_db_connection_for_user(self.user_name)
        df.to_sql(self.table_name, con=dbconnx, if_exists="append", index=False)