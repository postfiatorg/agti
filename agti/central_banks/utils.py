import os
import logging
import requests
import pdfplumber
import time
import enum
import re
import json

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
logging.getLogger("pdfminer.cmapdb").setLevel(logging.ERROR)

def download_and_read_pdf(url, save_dir, headers=None, cookies=None, proxies=None, evaluate_tolerances=None):
    """Download and extract text from a PDF file."""

    # NOTE: This is a temporary fix to disable PDF processing for quick local testing
    if os.getenv("DISABLE_PDF_PARSING", "false").lower() == "true":
        time.sleep(0.1) # Simulate processing
        return "Processing pdf disabled"


    filename = os.path.basename(url)
    filepath = os.path.join(save_dir, filename)
    
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.6834.110 Safari/537.36",
        "Accept": "application/pdf",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "TE": "Trailers",
        "Cache-Control": "max-age=0",
        "Pragma": "no-cache",
        "DNT": "1",  # Do Not Track
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-GPC": "1"  # Global Privacy Control
    }
    """
    
    text = None
    
    try:
        with requests.get(url, headers=headers, cookies=cookies, proxies=proxies, stream=True, timeout=100) as r:
            r.raise_for_status()
            try:
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            except Exception as stream_error:
                raise stream_error
        extract_kwargs = {}
        if evaluate_tolerances:
            x_tol, y_tol = evaluate_tolerances(filepath)
            extract_kwargs["x_tolerance"] = x_tol
            extract_kwargs["y_tolerance"] = y_tol

        with pdfplumber.open(filepath) as pdf:
            text = ""
            for page in pdf.pages:
                # pass x_tol and y_tol to extract_text method if they are not None
                text += page.extract_text(**extract_kwargs).replace('\x00','')


    except Exception as e:
        logger.exception("Error downloading and reading PDF", extra={"url": url, "filepath": filepath, "tolerances": evaluate_tolerances})
        return None
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)
    return text
    
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

def get_status(logs):
    for log in logs:
        if log["message"]:
            d = json.loads(log["message"])
            try:
                content_type = (
                    "text/html"
                    in d["message"]["params"]["response"]["headers"]["content-type"]
                )
                response_received = d["message"]["method"] == "Network.responseReceived"
                if content_type and response_received:
                    return d["message"]["params"]["response"]["status"]
            except:
                pass