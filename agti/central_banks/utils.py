import os
import logging
import requests
import pdfplumber
import time
import enum

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

def download_and_read_pdf(url, save_dir, evaluate_tolerances=None):
    """Download and extract text from a PDF file."""
    filename = os.path.basename(url)
    filepath = os.path.join(save_dir, filename)
    
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
    
    text = None
    
    try:
        with requests.get(url, headers=headers, stream=True, timeout=10) as r:
            r.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
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
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)
    return text
    


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