import os
import logging
from pathlib import Path
from urllib.parse import urlparse
import requests
import hashlib
import time
import enum
import re
import json

from agti.agti.central_banks.types import DYNAMIC_PAGE_EXTENSIONS, STATIC_PAGE_EXTENSIONS, ExtensionType

class Categories(enum.Enum):
    INSTITUTIONAL_AND_GOVERNANCE = "Institutional & Governance"
    MONETARY_POLICY = "Monetary Policy"
    FINANCIAL_STABILITY_AND_REGULATION = "Financial Stability & Regulation"
    RESEARCH_AND_DATA = "Research & Data"
    MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS = "Market Operations & Payment Systems"
    CURRENCY_AND_FINANCIAL_INSTRUMENTS = "Currency & Financial Instruments"
    NEWS_AND_EVENTS = "News & events"
    OTHER = "Other"



logger = logging.getLogger(__name__)


def get_hash_for_url(url):
    """
    Get the hash for a given URL.
    """
    return hashlib.sha1(url.encode()).hexdigest()

def download_and_read_pdf(url, save_dir, base_scraper, evaluate_tolerances=None):
    raise NotImplementedError("This function is not supported in this version.")

def classify_extension(ext):
    """
    Classify the file extension into static or dynamic.
    """
    if ext is None or len(ext) == 0:
        return None
    if ext in STATIC_PAGE_EXTENSIONS:
        return ExtensionType.WEBPAGE
    elif ext in DYNAMIC_PAGE_EXTENSIONS:
        return ExtensionType.WEBPAGE
    else:
        return ExtensionType.FILE
    
def get_cookies_headers(driver):
    # Get cookies from browser & unpack into a dictionary.
    #    
    cookies = {cookie["name"]: cookie["value"] for cookie in driver.get_cookies_for_request()}
    # Use a synchronous request to retrieve response headers.
    #
    script = """
    var xhr = new XMLHttpRequest();
    xhr.open('GET', window.location.href, false);
    xhr.send(null);
    return xhr.getAllResponseHeaders();
    """
    headers = driver.execute_script(script)
    
    # Unpack headers into dictionary.
    #
    headers = headers.splitlines()
    headers = dict([re.split(": +", header, maxsplit=1) for header in headers])
    return cookies, headers

def pageBottom(driver):
    bottom=False
    a=0
    while not bottom:
        new_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script(f"window.scrollTo(0, {a});")
        if a > new_height:
            bottom=True
        time.sleep(0.001)
        a+=5

def recusive_lower_keys(d):
    if isinstance(d, dict):
        return {k.lower(): recusive_lower_keys(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [recusive_lower_keys(i) for i in d]
    else:
        return d

def get_status(logs, target_url):
    parsed_target_url = urlparse(target_url)
    # get clean url without fragment
    without_fragment_target_url = parsed_target_url._replace(fragment="").geturl()
    possible_urls = [
        target_url,
        without_fragment_target_url,
        without_fragment_target_url + "?",
        parsed_target_url._replace(scheme="https").geturl(),
        parsed_target_url._replace(scheme="http").geturl(),
        parsed_target_url._replace(path=parsed_target_url.path + "/").geturl(),
        parsed_target_url._replace(path=parsed_target_url.path.rstrip("/")).geturl(),
    ]
    for log in logs:
        if log["message"]:
            d = json.loads(log["message"])
            d = recusive_lower_keys(d)
            try:
                content_type = (
                    "text/html"
                    in d["message"]["params"]["response"]["headers"]["content-type"]
                )
                response_received = d["message"]["method"] == "Network.responseReceived"
                if content_type and response_received and any([
                    d["message"]["params"]["response"]["url"] == url
                    for url in possible_urls
                ]):
                    return d["message"]["params"]["response"]["status"]
            except KeyError:
                pass