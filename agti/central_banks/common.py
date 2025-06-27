import re
import os
import random
import time
import ua_generator
from seleniumwire import webdriver
from dataclasses import dataclass
from selenium.webdriver.chrome.options import Options

@dataclass
class BrightDataProxy:
    proxy_host: str
    proxy_port: int
    proxy_username: str
    proxy_password: str
    countries: list[str]
     
    def get_proxy(self, session=None):
        zone = self.proxy_username
        if session:
            zone = f"{zone}-session-{session}"
        if len(self.countries) > 0:
            # choose random country from the list
            country = random.choice(self.countries)
            zone = f"{zone}-country-{country}"
        proxy_string = f"http://{zone}:{self.proxy_password}@{self.proxy_host}:{self.proxy_port}"
        return proxy_string
    
    @staticmethod
    def random_session_string():
        return os.urandom(8).hex()

class DriverManager:
    def __init__(self, run_headless, proxy_provider=None):
        self.proxy_provider = proxy_provider
        self.run_headless = run_headless
        self.generator = None
        self.headers = None
        self.driver = None

    def new_headers(self):
        if self.generator is None:
            raise Exception("User agent generator is not initialized")
        self.generator.headers.reset()
        self.generator.headers.accept_ch('Sec-CH-UA-Platform-Version, Sec-CH-UA-Full-Version-List')
        self.headers = self.generator.headers.get()
        self.headers["Accept-Language"] = "en-US,en;q=0.9"
        self.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        self.headers["Accept-Encoding"] = "gzip, deflate, br, zstd"

    def new_proxy(self):
        if self.proxy_provider:
            session = self.proxy_provider.random_session_string()
            proxy_str = self.proxy_provider.get_proxy(session=session)
            self.driver.proxy = {
                "http": proxy_str,
                "https": proxy_str,
            }


    def reset_session(self):
        self.new_headers()
        self.new_proxy()

    def __enter__(self):
        seleniumwire_options = None
        if self.proxy_provider:
            session = self.proxy_provider.random_session_string()
            seleniumwire_options = {
                'proxy': {
                    'http': self.proxy_provider.get_proxy(session=session),
                    'https': self.proxy_provider.get_proxy(session=session),
                }
            }
        options = Options()
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        if self.run_headless:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-extensions")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--ignore-certificate-errors")
            options.add_argument("--enable-javascript")
            #options.add_argument(f"user-agent={self.user_agent}")
        self.generator = ua_generator.generate(browser='chrome')
        self.driver = webdriver.Chrome(options=options, seleniumwire_options=seleniumwire_options)
        
        self.new_headers()
        self.driver.request_interceptor = self.intercept_headers


        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()
        self.headers = None
        self.generator = None
        self.driver = None
        time.sleep(0.1)

    def intercept_headers(self,request):
        for my_header,header_value in self.headers.items():
            del request.headers[my_header]
            request.headers[my_header] = header_value
        



def clean_text(value: bytes | str) -> str:
    """
    If `value` is bytes, decode to str.
    Then replace tabs (\t) and backspaces (\b) with a space,
    and collapse any run of whitespace into a single space.
    """
    # 1. Convert bytes → str
    if isinstance(value, bytes):
        text = value.decode('utf-8', errors='ignore')
    else:
        text = value

    # 2. Replace literal tab and backspace characters with a space
    #    (the pattern [\t\b] matches \t or \b)
    text = re.sub(r'[\t\b]', ' ', text)

    # 3. Collapse any sequence of whitespace (spaces, newlines, etc.) to a single space
    text = ' '.join(text.split())

    return text