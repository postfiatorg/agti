import os
import logging
from pathlib import Path
from urllib.parse import urlparse
import requests
import hashlib
import time
import enum
import json
import re

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
    target_url_req_id = -1
    for log in logs:
        msg = json.loads(log["message"])["message"]
        method = msg.get("method")
        params = msg.get("params", {})
        if method == "Network.requestWillBeSent" and params.get("request", {}).get("url") in possible_urls:
            target_url_req_id = params.get("requestId")
        elif method == "Network.responseReceived":
            req_id = params.get("requestId")
            resp = params.get("response", {})
            if req_id == target_url_req_id:
                status = resp.get("status")
                if status:
                    return status
    return None