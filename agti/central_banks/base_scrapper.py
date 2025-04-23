import os
from pathlib import Path
import socket
import logging
import time
import random
import boto3
from urllib.parse import urlparse
import uuid
import pandas as pd
import requests
from sqlalchemy import text
from selenium.webdriver.support import expected_conditions as EC
from agti.agti.central_banks.utils import get_status
from agti.utilities.settings import CredentialManager
from agti.utilities.db_manager import DBConnectionManager
from selenium.common.exceptions import NoSuchElementException
from agti.agti.central_banks.types import SCRAPERCONFIG, SQLDBCONFIG, BotoS3Config, CountryCB, SupportedScrapers


__all__ = ["create_bank_scraper"]

logger = logging.getLogger(__name__)

def create_bank_scraper(
        bank_enum: SupportedScrapers, 
        driver_manager, 
        sql_config, 
        scraper_config,
        boto3_config
    ):
    """
    Factory function to create a scraper instance based on SupportedScrapers enum
    
    Args:
        bank_enum (SupportedScrapers): The bank to create a scraper for
        driver_manager: Selenium driver manager instance
        sql_config: SQL configuration instance
        scraper_config: Scraper configuration instance
        boto3_config: Boto3 configuration instance
        
    Returns:
        BaseBankScraper: An instance of the appropriate bank scraper
    """
    from agti.agti.central_banks.scrappers.australia import AustraliaBankScrapper
    from agti.agti.central_banks.scrappers.canada import CanadaBankScrapper
    from agti.agti.central_banks.scrappers.ecb import ECBBankScrapper
    from agti.agti.central_banks.scrappers.england import EnglandBankScrapper
    from agti.agti.central_banks.scrappers.fed import FEDBankScrapper
    from agti.agti.central_banks.scrappers.japan import JapanBankScrapper
    from agti.agti.central_banks.scrappers.norges import NorgesBankScrapper
    from agti.agti.central_banks.scrappers.sweden import SwedenBankScrapper
    from agti.agti.central_banks.scrappers.switzerland import SwitzerlandBankScrapper
    
    scraper_map = {
        SupportedScrapers.AUSTRALIA: AustraliaBankScrapper,
        SupportedScrapers.CANADA: CanadaBankScrapper,
        SupportedScrapers.EUROPE: ECBBankScrapper,
        SupportedScrapers.ENGLAND: EnglandBankScrapper,
        SupportedScrapers.USA: FEDBankScrapper,
        SupportedScrapers.JAPAN: JapanBankScrapper,
        SupportedScrapers.NORGES: NorgesBankScrapper,
        SupportedScrapers.SWEDEN: SwedenBankScrapper,
        SupportedScrapers.SWITZERLAND: SwitzerlandBankScrapper
    }
    
    if bank_enum not in scraper_map:
        raise ValueError(f"Unsupported bank enum: {bank_enum}")
    
    return scraper_map[bank_enum](
        bank_config=bank_enum.value,
        driver_manager=driver_manager,
        sql_config=sql_config,
        scraper_config=scraper_config,
        boto3_config=boto3_config,
    )

class BaseBankScraper:
    """Base class for bank scrapers with common functionality."""

    def __init__(
            self,
            bank_config: CountryCB ,
            driver_manager,
            sql_config: SQLDBCONFIG,
            scraper_config: SCRAPERCONFIG,
            boto3_config: BotoS3Config,
            ):
        self.scraper_config = scraper_config
        self.session_counter = 0

        self.sql_config = sql_config
        self.driver_manager = driver_manager
        self.datadump_directory_path = CredentialManager().get_datadump_directory_path()
        
        # Store the bank configuration from SupportedScrapers enum
        self.bank_config = bank_config

        self.bucket = self._initialize_bucket(boto3_config)
        if self.bucket is None:
            raise ValueError(f"Failed to create or access S3 bucket: {boto3_config.BUCKET_NAME}")

        self.cookies = None
        self.initialize_cookies(go_to_url=True)


    @staticmethod
    def _initialize_bucket(boto3_config: BotoS3Config):
        """
        Initialize the S3 bucket for the bank.
        """
        # Create the S3 bucket if it doesn't exist
        client = boto3.client(
            "s3",
            aws_access_key_id=boto3_config.ACCESS_KEY,
            aws_secret_access_key=boto3_config.SECRET_KEY,
            region_name=boto3_config.REGION_NAME,
            endpoint_url=boto3_config.ENDPOINT_URL,
        )
        location = {'LocationConstraint': boto3_config.REGION_NAME}
        # check if bucket exists
        try:
            bucket = client.create_bucket(Bucket=boto3_config.BUCKET_NAME, CreateBucketConfiguration=location)
        except Exception as e:
            logger.exception(f"Error creating bucket: {e}")
            return None
        return bucket
        


    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get(self.bank_config.URL)
        self.cookies = self.driver_manager.driver.get_cookies()


    def random_sleep(self):
        """Sleep for a random time between min and max sleep time."""
        time.sleep(random.uniform(self.scraper_config.SLEEP_MIN, self.scraper_config.SLEEP_MAX))

    def get(self, url):
        parsed_url = urlparse(url)
        # random sleep time to mimic human behavior
        self.random_sleep()
        # we assume that get is only called on htm or htm pages
        # 2 requrements to refresh session, count > interval + netloc and url.netloc == NETLOC and it is not 
        if self.session_counter > self.scraper_config.SESSION_REFRESH_INTERVAL and parsed_url.netloc == self.bank_config.NETLOC:
            self.driver_manager.driver.delete_all_cookies()
            self.cookies = None
            self.driver_manager.reset_session()
            self.session_counter = 0
            new_headers = self.driver_manager.headers
            logger.debug("Refreshing headers", extra={"new_headers": new_headers})
        success = False
        for i in range(4):
            if i > 0:
                logger.warning(f"Retrying {i} for url: {url}")
            if i == 2:
                # we try to refresh the session if we fail
                logger.debug(f"Refreshing session {i} for url failed 2 times already: {url}")
                self.driver_manager.driver.delete_all_cookies()
                self.cookies = None
                self.driver_manager.reset_session()
                self.session_counter = 0
                new_headers = self.driver_manager.headers
                logger.debug("Refreshing headers", extra={"new_headers": new_headers})
            self.driver_manager.driver.get(url)
            logs = self.driver_manager.driver.get_log("performance")
            response = get_status(logs, url)
            if response == 200:
                success = True
                break
            elif response == 404:
                logger.warning(f"Page not found: {url}, response code: {response}")
                success = True
                break
            else:
                logger.warning(f"Failed to load page: {url}, response code: {response}")
                # we have to wait for a while
                self.random_sleep()
        if not success:
            logger.error(f"Failed to load page: {url}")
            logger.debug(f"Headers: {self.driver_manager.headers}")
            logger.debug(f"Cookies: {self.cookies}")
            logger.debug(f"Proxy: {self.driver_manager.driver.proxy}")
            return False
        if self.cookies is None and parsed_url.netloc == self.bank_config.NETLOC:
            try:
                self.initialize_cookies()
                logger.debug("Cookies initialized", extra={"cookies": self.cookies})
            except Exception as e:
                logger.exception(f"No cookies not found for url: {url}, ERROR: {e}", extra={"url": url})
        else:
            # we only increment if cookies are set
            self.session_counter += 1
        return True

    def get_headers(self):
        return self.driver_manager.headers
    
    def get_cookies_for_request(self):
        # edit cookies to be used in requests
        cookies = self.cookies
        if cookies is None:
            return None
        cookies = {cookie["name"]: cookie["value"] for cookie in cookies}
        return cookies
    
    def get_proxies(self):
        return self.driver_manager.driver.proxy
    
    def get_driver(self):
        return self.driver_manager.driver
        

    def ip_hostname(self):
        """Retrieve machine IP and hostname."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname
    
    def get_category_table_name(self):
        return f"{self.sql_config.TABLE_NAME}_categories"
    
    def get_links_table_name(self):
        return f"{self.sql_config.TABLE_NAME}_links"

    def get_all_db_urls(self):
        """Retrieve all URLs already stored in the database."""
        dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
        query = text(f"SELECT file_url FROM {self.sql_config.TABLE_NAME} WHERE country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.bank_config.COUNTRY_CODE_ALPHA_3}
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            output =  [row[0] for row in rs.fetchall()]
        return output
        
    def get_all_db_categories(self):
        """Retrieve all categories already stored in the database."""
        dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
        # query select all from categories table, which joined with self.sql_config.TABLE_NAME over file_url has country_code_alpha_3
        table_name = self.get_category_table_name()
        query = text(f"SELECT {table_name}.file_url, {table_name}.category_name FROM {table_name} " +  \
                    f"INNER JOIN {self.sql_config.TABLE_NAME} ON {table_name}.file_url = {self.sql_config.TABLE_NAME}.file_url " + \
                    f"WHERE {self.sql_config.TABLE_NAME}.country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.bank_config.COUNTRY_CODE_ALPHA_3}
        with dbconnx.connect() as con:
            rs = con.execute(query, params)
            output =  [row for row in rs.fetchall()]
        return output
    
    def get_all_db_links(self):
        """Retrieve all links already stored in the database."""
        dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
        links_table_name = self.get_links_table_name()
        query = text(f"SELECT {links_table_name}.file_url, {links_table_name}.link_url FROM {links_table_name} " +  \
            f"INNER JOIN {self.sql_config.TABLE_NAME} ON {links_table_name}.file_url = {self.sql_config.TABLE_NAME}.file_url " + \
            f"WHERE {self.sql_config.TABLE_NAME}.country_code_alpha_3 = :country_code_alpha_3")
        params = {"country_code_alpha_3": self.bank_config.COUNTRY_CODE_ALPHA_3}
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
        df["country_name"] = self.bank_config.COUNTRY_NAME
        df["country_code_alpha_3"] = self.bank_config.COUNTRY_CODE_ALPHA_3
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
            dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
        df.to_sql(self.sql_config.TABLE_NAME, con=dbconnx, if_exists="append", index=False)

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
            dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
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
            dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
        table_name = self.get_links_table_name()
        df.to_sql(table_name, con=dbconnx, if_exists="append", index=False)



    def add_all_atomic(self, data, tags, links):
        """Store scraped data into the database."""
        dbconnx = self.sql_config.CONNECTION_MANAGER.spawn_sqlalchemy_db_connection_for_user(self.sql_config.USER_NAME)
        with dbconnx.begin() as connection:
            self.add_to_db(data,dbconnx=connection)
            self.add_to_categories(tags,dbconnx=connection)
            self.add_to_links(links,dbconnx=connection)



    def download_pdf(self, url):
        # NOTE: This is a temporary fix to disable PDF processing for quick local testing
        if os.getenv("DISABLE_PDF_PARSING", "false").lower() == "true":
            time.sleep(0.1) # Simulate processing
            return "Processing pdf disabled"
        uuid_str = str(uuid.uuid4())
        filename = f"{uuid_str}.pdf"
        filepath = Path(os.path.join(self.datadump_directory_path, filename))

        headers = self.get_headers()
        cookies = self.get_cookies_for_request()
        proxies = self.get_proxies()
        for i in range(3):
            try:
                with requests.get(url, headers=headers, cookies=cookies, proxies=proxies, stream=True, timeout=100) as r:
                    r.raise_for_status()
                    try:
                        with open(filepath, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    except Exception as stream_error:
                        raise stream_error
            except requests.exceptions.HTTPError:
                logger.exception("Error downloading and reading PDF", extra={
                    "url": url,
                    "filepath": filepath,
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
                logger.exception("Error downloading and reading PDF", extra={
                    "url": url,
                    "filepath": filepath,
                    "headers": headers,
                    "cookies": cookies,
                    "proxies": proxies,
                })
                break

        if os.path.exists(filepath):
            return filepath
        return None



    def upload_pdf_to_s3(self, filepath: Path, year: int = None, remove_file: bool = True):
        """
        Upload a PDF file to S3 bucket.
        
        Args:
            filepath (Path): The path of the PDF file to upload.
            year (int): The year for the S3 path.
            remove_file (bool): Whether to remove the local file after upload.
            
        Returns:
            bool: True if the file was uploaded successfully, False otherwise.
        1. Upload the file to S3 bucket.
        path: /country_code_alpha_3/{year}/
        """
        filename = filepath.name
        # Upload to S3
        if year is None:
            year = "unknown"
        self.bucket.upload_file(filepath, f"{self.bank_config.COUNTRY_CODE_ALPHA_3}/{year}/{filename}")
        # Remove local file if specified
        if remove_file:
            os.remove(filepath)
        logger.info(f"Uploaded {filename} to S3 bucket {self.bucket.name} at path {self.bank_config.COUNTRY_CODE_ALPHA_3}/{year}/")
        return True
        