import requests
import time
# this was removed ... dont know why it needed to be 
# "Accept-Encoding": "gzip, deflate",
# Host www.sec.gov

import requests
import time

class SECRequestUtility:
    def __init__(self, pw_map, max_retries=3):
        self.pw_map = pw_map
        self.max_retries = max_retries
        
    def compliant_request(self, url):
        """Complies with SEC requirements for pulling down data"""
        headers = {
            "User-Agent": self.pw_map['sec_request_name']
        }
        
        for attempt in range(self.max_retries):
            try:
                print(f"Requesting {url}")
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raises an HTTPError for bad responses
                time.sleep(0.3)  # Ensures max 10 requests per second
                return response
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == self.max_retries - 1:
                    print(f"Max retries reached. Failed to request {url}")
                    return None
                time.sleep(1)  # Wait for 1 second before retrying