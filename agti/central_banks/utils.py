import os
import requests
import pdfplumber
import time

def download_and_read_pdf(url, save_dir, evaluate_tolerances=None):
    """Download and extract text from a PDF file."""
    filename = os.path.basename(url)
    filepath = os.path.join(save_dir, filename)
    
    headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0"
        }
    
    text = None
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(response.content)

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
        print(f"Error processing PDF {url}: {e}")
    
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