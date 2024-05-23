import requests
import time
class SECRequestUtility:
    def __init__(self,pw_map):
        self.pw_map = pw_map
        
    def compliant_request(self, url):
        """ this complies with SEC requirements for pulling down data""" 
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        req_output = requests.get(url, headers=headers)
        time.sleep(.18)
        if req_output.status_code != 200:
            print(f'blew up requesting {url}')
        return req_output