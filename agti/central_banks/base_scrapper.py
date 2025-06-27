import base64
import mimetypes
import os
from pathlib import Path
import socket
import logging
import time
import random
import boto3
from urllib.parse import quote, urlparse
import hashlib
import pandas as pd
import requests
from sqlalchemy import text
from selenium.webdriver.support import expected_conditions as EC
import urllib3
from agti.agti.central_banks.utils import classify_extension, get_hash_for_url, get_status
from agti.agti.central_banks.common import clean_text
from agti.utilities.settings import CredentialManager
from botocore.exceptions import ClientError
from agti.agti.central_banks.types import DYNAMIC_PAGE_EXTENSIONS, SCRAPERCONFIG, SQLDBCONFIG, STATIC_PAGE_EXTENSIONS, BotoS3Config, CountryCB, ExtensionType, LinkMetadata, MainMetadata, SupportedScrapers, URLType
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

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
    from agti.agti.central_banks.scrapers.australia import AustraliaBankScrapper
    from agti.agti.central_banks.scrapers.canada import CanadaBankScrapper
    from agti.agti.central_banks.scrapers.ecb import ECBBankScrapper
    from agti.agti.central_banks.scrapers.england import EnglandBankScraper
    from agti.agti.central_banks.scrapers.fed import FEDBankScrapper
    from agti.agti.central_banks.scrapers.japan import JapanBankScrapper
    from agti.agti.central_banks.scrapers.norges import NorgesBankScrapper
    from agti.agti.central_banks.scrapers.sweden import SwedenBankScrapper
    from agti.agti.central_banks.scrapers.switzerland import SwitzerlandBankScrapper
    
    scraper_map = {
        SupportedScrapers.AUSTRALIA: AustraliaBankScrapper,
        SupportedScrapers.CANADA: CanadaBankScrapper,
        SupportedScrapers.EUROPE: ECBBankScrapper,
        SupportedScrapers.ENGLAND: EnglandBankScraper,
        SupportedScrapers.USA: FEDBankScrapper,
        SupportedScrapers.JAPAN: JapanBankScrapper,
        SupportedScrapers.NORGE: NorgesBankScrapper,
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
            bucket_dict = client.create_bucket(Bucket=boto3_config.BUCKET_NAME, CreateBucketConfiguration=location)
        except client.exceptions.BucketAlreadyOwnedByYou:
            pass
        except Exception as e:
            logger.exception(f"Error creating bucket: {e}")
            return None
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=boto3_config.ACCESS_KEY,
            aws_secret_access_key=boto3_config.SECRET_KEY,
            region_name=boto3_config.REGION_NAME,
            endpoint_url=boto3_config.ENDPOINT_URL,
        )
        bucket = s3.Bucket(boto3_config.BUCKET_NAME)
        return bucket
        


    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get(self.bank_config.URL)
        self.cookies = self.driver_manager.driver.get_cookies()


    def random_sleep(self):
        """Sleep for a random time between min and max sleep time."""
        time.sleep(random.uniform(self.scraper_config.SLEEP_MIN, self.scraper_config.SLEEP_MAX))

    def refresh_session(self):
        """Refresh the session by deleting all cookies and resetting the driver."""
        self.driver_manager.driver.delete_all_cookies()
        self.cookies = None
        self.driver_manager.reset_session()
        self.session_counter = 0
        new_headers = self.driver_manager.headers
        logger.debug("Refreshing headers", extra={"new_headers": new_headers})

    def get(self, url, raise_exception=True):
        refreshed_headers_before_first_get = False
        parsed_url = urlparse(url)
        # random sleep time to mimic human behavior
        self.random_sleep()
        # we assume that get is only called on htm or htm pages
        # 2 requrements to refresh session, count > interval + netloc and url.netloc == NETLOC and it is not 
        if self.session_counter > self.scraper_config.SESSION_REFRESH_INTERVAL and parsed_url.netloc == self.bank_config.NETLOC:
            self.refresh_session()
            refreshed_headers_before_first_get = True
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
            try:
                self.driver_manager.driver.get(url)
                logs = self.driver_manager.driver.get_log("performance")
                response = get_status(logs, url)
            except (urllib3.exceptions.ReadTimeoutError, TimeoutError) as e:
                logger.exception(f"TimeoutError for url: {url}, ERROR: {e}", extra={"url": url})
                # we have to wait for a while
                self.random_sleep()
                continue
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
        if not success and not refreshed_headers_before_first_get:
            logger.error(f"Failed to load page: {url}")
            logger.debug(f"Headers: {self.driver_manager.headers}")
            logger.debug(f"Cookies: {self.cookies}")
            logger.debug(f"Proxy: {self.driver_manager.driver.proxy}")
            if raise_exception:
                raise Exception(f"Failed to load page: {url}")
            return False
        elif refreshed_headers_before_first_get and not success:
            # we shall go to main url refresh there session
            logger.debug(f"Failed to load page: {url} after refreshing headers before first get")
            # go to main page
            self.driver_manager.driver.get(self.bank_config.URL)
            self.refresh_session()
            self.initialize_cookies()
            # we try again
            return self.get(url, raise_exception=raise_exception)
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

        # apply clean_text to link_name
        df["link_name"] = df["link_name"].apply(clean_text)

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



    def download_file(self, url, extension):
        # NOTE: This is a temporary fix to disable PDF processing for quick local testing
        id_str = get_hash_for_url(url)
        filename = f"{id_str}.{extension}"
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
            except (requests.exceptions.HTTPError, urllib3.exceptions.ReadTimeoutError):
                logger.exception(f"Error downloading and reading extension {extension} for {url}", extra={
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
                self.random_sleep()
            except Exception as e:
                logger.exception(f"Error downloading and reading extension {extension} for {url}", extra={
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
    
    def save_page_as_pdf(self):
        """
        Save the current page as a PDF file.
        We use "printToPDF" to save the page as a PDF.
        
        Args:
            url (str): The URL of the page to save.
            
        Returns:
            str: The path of the saved PDF file.
        """
        # Generate a unique filename for the PDF
        url = self.driver_manager.driver.current_url
        id_str = get_hash_for_url(url)
        filename = f"{id_str}.pdf"
        filepath = Path(os.path.join(self.datadump_directory_path, filename))

            # 4. Define print options
        print_options = {
            "printBackground": True,       # include background colors/images
            "preferCSSPageSize": True,     # use CSS @page size if defined
            "marginTop": 0.4,              # margins in inches
            "marginBottom": 0.4,
            "marginLeft": 0.4,
            "marginRight": 0.4,
            # "scale": 1.0,                # adjust scale if needed
            # experimental flags—for outline or tagging:
            # "generateTaggedPDF": False,
            # "generateDocumentOutline": False
        }
        result = self.driver_manager.driver.execute_cdp_cmd("Page.printToPDF", print_options)
        pdf_data = base64.b64decode(result["data"]) # Decode the base64 data

        # Save the PDF data to a file
        with open(filepath, "wb") as f:
            f.write(pdf_data)
        logger.info(f"Saved page as PDF: {filepath}")
        return filepath

    def download_and_upload_file(self, url, extension, metadata: MainMetadata | LinkMetadata, year=None):
        """
        Download a file from the given URL and upload it to S3.
        
        Args:
            url (str): The URL of the file to download.
            extension (str): The file extension.
            metadata (MainMetadata | LinkMetadata): Metadata to include in the S3 object.
            year (str): The year for the S3 path.

        Returns:
            bool: True if the file was downloaded and uploaded successfully, False otherwise.
        """
        filepath = self.download_file(url, extension)
        if filepath is not None:
            done = self.upload_file_to_s3(filepath, metadata, year=year)
            if done:
                return filepath.stem
            else:
                logger.error(f"Failed to upload file to S3: {filepath}", extra={
                    "url": url,
                    "extension_type": extension,
                    "year": year,
                })
                return None
        logger.error(f"Failed to download file: {url}", extra={
            "url": url,
            "extension_type": extension,
            "year": year,
        })
        return None
        



    def upload_file_to_s3(
            self,
            filepath: Path,
            metadata: MainMetadata | LinkMetadata,
            year: str = None,
            remove_file: bool = True
        ):
        """
        Upload a file to S3 bucket.
        
        Args:
            filepath (Path): The path of the file to upload.
            metadata (MainMetadata | LinkMetadata): Metadata to include in the S3 object.
            year (str): The year for the S3 path.
            remove_file (bool): Whether to remove the local file after upload.
            
        Returns:
            bool: True if the file was uploaded successfully, False otherwise.
        1. Upload the file to S3 bucket.
        path: /country_code_alpha_3/{year}/
        """
        # create key
        filename = filepath.name
        if year is None:
            year = "unknown"
        key = f"{self.bank_config.COUNTRY_CODE_ALPHA_3}/{year}/{filename}"

        # check if file exists in S3
        try:
            self.bucket.Object(key).load()
            logger.info(f"File already exists in S3: {key}")
            if remove_file:
                os.remove(filepath)
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code != "404":
                logger.error(f"Unable to check existence of {key}: {e}")
                # we upload the file anyway
        
        # guess content type of filepath
        ctype = mimetypes.guess_type(filepath)[0]
        # Upload the file to S3
        extra_args = {
            "Metadata": metadata.to_dict(),
        }
        if ctype is not None:
            extra_args["ContentType"] = ctype
        self.bucket.upload_file(
            filepath,
            key,
            ExtraArgs=extra_args,
        )
        # Remove local file if specified
        if remove_file:
            os.remove(filepath)
        
        logger.info(f"Uploaded {filename} to S3 bucket {self.bucket.name} at path {self.bank_config.COUNTRY_CODE_ALPHA_3}/{year}/")
        return True
    

    def process_html_page(self, metadata: MainMetadata | LinkMetadata, year):
        filepath = self.save_page_as_pdf()
        if filepath is not None:
            if self.upload_file_to_s3(filepath, metadata, year=year):
                return filepath.stem
            logger.error(f"Failed to upload file to S3: {filepath}", extra={
                "year": year,
                "filepath": filepath,
                "url": self.driver_manager.driver.current_url
            })
            return None
        logger.error(f"Failed to save page as PDF: {filepath}", extra={
            "year": year,
            "filepath": filepath,
            "url": self.driver_manager.driver.current_url
        })
        return None
        
    def get_file_type_request(self, url):
        """
        Get the file type based on the URL extension.
        """
        headers = self.get_headers()
        cookies = self.get_cookies_for_request()
        proxies = self.get_proxies()
        for i in range(3):
            try:
                resp = requests.head(url, headers=headers, cookies=cookies, proxies=proxies, allow_redirects=True, timeout=60)
                # Sometime servers returns 500, even tough the headers are present
                # we shall try to use get instead of head
                if resp.status_code == 500:
                    logger.warning(f"500 status code for {url}, trying GET request", extra={
                        "url": url,
                        "headers": headers,
                        "cookies": cookies,
                        "proxies": proxies,
                    })
                    resp = requests.get(url, headers=headers, cookies=cookies, proxies=proxies, allow_redirects=True, timeout=60) 
                resp.raise_for_status()
                if resp.ok:
                    break
            except requests.exceptions.HTTPError:
                logger.exception(f"HTTPError getting filetype for {url}", extra={
                    "url": url,
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
                logger.exception(f"General getting filetype from {url}", extra={
                    "url": url,
                    "headers": headers,
                    "cookies": cookies,
                    "proxies": proxies,
                })
                break
        # end of loop
        if resp.status_code != 200:
            logger.exception(f"Failed to get file type for {url}, status code: {resp.status_code}", extra={
                "url": url,
                "headers": headers,
                "cookies": cookies,
                "proxies": proxies,
            })
            return None
        ctype = resp.headers.get("Content-Type", "").split(";", 1)[0].lower()
        # if ctype has text/html we return page, otherwise we return file
        ctype_extension = mimetypes.guess_extension(ctype)
        if ctype_extension is None:
            logger.warning(f"Unknown content type: {ctype} for url: {url}", extra={
                "url": url,
                "headers": headers,
                "cookies": cookies,
                "proxies": proxies,
            })
            return None
        return ctype_extension.lstrip(".").lower()
    

    def classify_url(self, link, allow_outside=False):
        parsed_link = urlparse(link)
        ext = os.path.splitext(parsed_link.path)[1].lstrip(".").lower()
        output = [None, None]
        # NOTE uerltype is never None
        if parsed_link.netloc == self.bank_config.NETLOC:
            output[0] = URLType.INTERNAL
        else:
            output[0] = URLType.EXTERNAL
        if len(ext) > 0:
            output[1] = ext
        elif allow_outside or output[0] == URLType.INTERNAL:
            output[1] = self.get_file_type_request(link)
        #else:
        #    output[1] = None
        return output
        


    def process_links(self, main_file_id, f_get_links, year = None, allow_outside=False, download_a_tag_xpath=None):
        """
        Args:
            f_get_links (function): Function to get links from the page
        1. get all links
        2. filter links
        2a. link with extesion to file type (like pdf...) will be downloaded and uploaded to s3
        2b. link point to website within the same domain will be transferred to pdf and uploaded to s3
        2c. link point to website outside the domain will be ignored
        """
        # get all links from the main page
        all_links = [
            (link_text, link) for link_text, link in f_get_links() if link is not None and link_text != ""
        ]
        
        result = []
        processed_paths = [urlparse(self.driver_manager.driver.current_url).path]
        for link_text, link in all_links:
            if link.startswith("tel:") or link.startswith("mailto:") or link.startswith("javascript:"):
                continue
            if link.endswith("#"):
                logger.debug(f"Link ends with #: {link}", extra={
                    "link": link,
                    "link_text": link_text,
                    "current_url": self.driver_manager.driver.current_url
                })
                continue
            # ignore links, which have fragment ot the same page
            link_parsed = urlparse(link)
            if link_parsed.path == "" or link_parsed.path == "/":
                continue
            current_url_parsed = urlparse(self.driver_manager.driver.current_url)
            if link_parsed.fragment != "":
                if link_parsed.netloc == self.bank_config.NETLOC and \
                    any([link_parsed.path == p for p in processed_paths]):
                    logger.debug(f"Link has fragment to the same page: {link}", extra={
                        "link": link,
                        "link_text": link_text,
                        "current_url": self.driver_manager.driver.current_url
                    })
                    continue
            processed_paths.append(link_parsed.path)
            urlType, extension = self.classify_url(link, allow_outside=allow_outside)
            if extension is None:
                if (urlType == URLType.EXTERNAL and allow_outside) or urlType == URLType.INTERNAL:
                    logger.error(f"Unknown file type for {link}", extra={
                        "link": link,
                        "link_text": link_text,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            filepath = None
            if classify_extension(extension) == ExtensionType.FILE:
                # download file and upload to s3
                filepath = self.download_file(link, extension)
                if filepath is None:
                    logger.error(f"Failed to download file: {link}", extra={
                        "link": link,
                        "link_text": link_text,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                    continue
            elif urlType == URLType.INTERNAL:
                # we now it is webpage and we process internal links only
                success = self.get(link, raise_exception=False)
                if not success:
                    logger.error(f"Failed to retrieve internal link: {link}", extra={
                        "link": link,
                        "link_text": link_text
                    })
                    continue
                # save it as pdf
                if download_a_tag_xpath is not None:
                    try:
                        download_button = self.driver_manager.driver.find_element(By.XPATH, download_a_tag_xpath)
                        new_link = download_button.get_attribute("href")
                        new_link_text = download_button.text
                        # we need to classify the link again
                        _, download_extension = self.classify_url(new_link, allow_outside=allow_outside)
                        assert classify_extension(download_extension) == ExtensionType.FILE
                        # we replace the link and link_text with the new ones
                        extension = download_extension
                        link = new_link
                        link_text = new_link_text
                        filepath = self.download_file(
                            link,
                            extension
                        )
                    except NoSuchElementException:
                        pass
                    except AssertionError:
                        logger.error(f"Download new_link {new_link} has unsupported extension: {download_extension}", extra={
                            "new_link": new_link,
                            "new_link_text": new_link_text,
                            "old_link": link,
                            "old_link_text": link_text,
                            "old_urlType": urlType,
                            "extension_type": download_extension
                        })
                    finally:
                        if filepath is None:
                            # we save the page as pdf
                            filepath = self.save_page_as_pdf()
                else:
                    filepath = self.save_page_as_pdf()
            else:
                # we ignore external links
                pass
            metadata = LinkMetadata(
                link_name=link_text,
                main_file_id=main_file_id,
                url=link,
            )
            if filepath is not None:
                if self.upload_file_to_s3(filepath, metadata, year=year):
                    result.append((link, link_text, filepath.stem))
                else:
                    logger.error(f"Failed to upload file to S3: {filepath}", extra={
                        "link": link,
                        "link_text": link_text,
                        "urlType": urlType,
                        "extension_type": extension
                    })
        return result
                
            
            



        
        