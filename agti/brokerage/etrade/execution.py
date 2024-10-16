# Standard Library Imports
import datetime
import itertools
import string
import time
from concurrent.futures import ThreadPoolExecutor
from agti.utilities.db_manager import DBConnectionManager
from basic_utilities import regression as reg
# Third-Party Imports
import numpy as np
import pandas as pd
import requests
import selenium
from rauth import OAuth1Service
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from agti.data.tiingo.equities import TiingoDataTool
from agti.utilities.google_sheet_manager import GoogleSheetManager
#from basic_utilities.global_initializor import *
from basic_utilities.regression import *
from selenium.webdriver.firefox.options import Options as FirefoxOptions

class ETradeUXDriver:
    def __init__(self,pw_map,driver_type):
        self.pw_map= pw_map
        self.hard_to_borrow_arr=[]
        
                #options = Options()
        #options = Options()
        #options.add_argument("-private")
        #firefox_profile = webdriver.FirefoxProfile()
        #firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
        

        # Initialize Chrome WebDriver
        if driver_type=='chrome':
            chrome_options = webdriver.ChromeOptions()

        # Enable Incognito mode
            chrome_options.add_argument("--incognito")
            driver = webdriver.Chrome(options=chrome_options)
        
        if driver_type=='firefox':
            firefox_options = FirefoxOptions()
            firefox_options.add_argument("-private")
            driver = webdriver.Firefox(options=firefox_options)

        if driver_type=='edge':
            edge_options = Options()
            edge_options.add_argument("-inprivate")  # For InPrivate mode

            # Initialize Edge service
            #service = Service(executable_path=driver_path)

            # Initialize Edge WebDriver with options and service
            driver = webdriver.Edge(options=edge_options)#, service=service)

        #firefox_options = webdriver.FirefoxOptions()
       # firefox_options.add_argument("--private")
        #webdriver.Firefox(executable_path='C:\\Users\\goodalexander\\Downloads\\geckodriver-v0.33.0-win64\\geckodriver.exe',
        #                                        firefox_profile=firefox_profile,firefox_options=firefox_options)
        self.firefox_driver = driver
        #self.firefox_driver.get('https://us.etrade.com/etx/pxy/login')
        
        print('LOG IN TO ETRADE')
        #username_field = self.firefox_driver.find_element_by_id("USER")
       
        #password_field = self.firefox_driver.find_element_by_id("password")
        
        #checkbox = self.firefox_driver.find_element_by_id("useSecurityCode")
        #self.firefox_driver.execute_script("arguments[0].click();", checkbox)
        input('Press any key once you are logged in')
        
        print('VALIDATING SESSION ID FOR API CALLS')
        self.consumer_key = self.pw_map['etrade_prod']
        self.consumer_secret = self.pw_map['etrade_secret']
        self.base_url = 'https://api.etrade.com'
        etrade = OAuth1Service(
            name="etrade",
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            request_token_url="https://api.etrade.com/oauth/request_token",
            access_token_url="https://api.etrade.com/oauth/access_token",
            authorize_url="https://us.etrade.com/e/t/etws/authorize?key={}&token={}",
            base_url="https://api.etrade.com")


        menu_items = {"1": "Sandbox Consumer Key",
                      "2": "Live Consumer Key",
                      "3": "Exit"}
        base_url = 'https://api.etrade.com'

        # Step 1: Get OAuth 1 request token and secret
        request_token, request_token_secret = etrade.get_request_token(
            params={"oauth_callback": "oob", "format": "json"})
        authorize_url = etrade.authorize_url.format(etrade.consumer_key, request_token)

        
        self.firefox_driver.get(authorize_url)
        # DEPRECATED 
        #accept_button = self.firefox_driver.find_element_by_xpath('//input[@name="submit" and @value="Accept"]')
        try:
            accept_button = self.firefox_driver.find_element(By.XPATH, '//input[@name="submit" and @value="Accept"]')

            accept_button.click()
        except:
            pass
        input('press once you have accepted the terms and conditions')
        input_element = self.firefox_driver.find_element(By.XPATH, '//input[@type="text"]')

        # Get the value of the input element
        verification_code = input_element.get_attribute('value')


        # Step 2: Go through the authentication flow. Login to E*TRADE.
        # After you login, the page will provide a verification code to enter.
        authorize_url = etrade.authorize_url.format(etrade.consumer_key, request_token)
        #webbrowser.open(authorize_url)

        # Step 3: Exchange the authorized request token for an authenticated OAuth 1 session
        self.session = etrade.get_auth_session(request_token,
                                      request_token_secret,
                                      params={"oauth_verifier": verification_code})
        def given_session_generate_account_map(session):
            url = self.base_url + "/v1/accounts/list.json"
            xacct_text=session.get(url, header_auth=True)
            all_account_list = xacct_text.json()
            account= all_account_list['AccountListResponse']['Accounts']['Account'][0]
            return account

        self.account_map = given_session_generate_account_map(session=self.session)
        print('finished auth login')
        self.firefox_driver.get('https://trading.etrade.com/etx/trdclt?ploc=2017-launchpad-OH20')
        
        try:
            try:
                WebDriverWait(self.firefox_driver, 10).until(
                    EC.invisibility_of_element_located((By.CSS_SELECTOR, '.Mask---root---UndVC'))
                )
                trading_tab_element = WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-id="ViewPort_Navigation_TradingTab"]'))
                )
                self.firefox_driver.execute_script("arguments[0].scrollIntoView();", trading_tab_element)
                trading_tab_element.click()
                print('In the Trading Tab')
            except TimeoutException:
                print("Trading tab did not become visible within 10 seconds")
                pass  # Execution will continue from here
            except ElementClickInterceptedException:
                try:
                    self.firefox_driver.execute_script("arguments[0].click();", trading_tab_element)
                    print('Used JS to click the trading tab')
                except:
                    print("JavaScript click also failed.")
                    pass  # Execution will continue from here
        except:
            pass

    def navigate_to_ticker_page__legacy(self, ticker_to_work='ORCL'):
        max_retries = 1
        current_try = 0
        if ticker_to_work != 'NAN':

            try:
                # Wait for search bar to be present and then interact
                search_bar = WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[data-id="ViewPort_Navigation_symbolSearchInput"]'))
                )
                search_bar.clear()
                time.sleep(.1)
                if search_bar.get_attribute("value") == "":
                    search_bar.send_keys(ticker_to_work)
                if search_bar.get_attribute("value") != "":
                    search_bar.clear()
                    time.sleep(.15)
                    if search_bar.get_attribute("value") == "":
                        search_bar.send_keys(ticker_to_work)

                # Wait for dropdown to appear and be populated
                WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li.SymbolSearch---menuItem---xwN3j'))
                )

                # Deprecated
                # dropdown_menu = self.firefox_driver.find_element_by_css_selector('ul.SymbolSearch---menu---98GcZ')
                dropdown_menu = self.firefox_driver.find_element(By.CSS_SELECTOR, 'ul.SymbolSearch---menu---98GcZ')

                # Loop through dropdown items
                for item in dropdown_menu.find_elements(By.CSS_SELECTOR, 'li.SymbolSearch---menuItem---xwN3j'):
                    symbol = item.find_element(By.CSS_SELECTOR, 'span.SymbolSearch---symbol---BQiqg').text
                    if symbol == ticker_to_work:
                        # Use ActionChains for better reliability
                        actions = ActionChains(self.firefox_driver)
                        actions.move_to_element(item).click().perform()
                        return  # Successfully clicked; exit loop
                current_try += 1  # Increment retry count if not successful

            except TimeoutException:
                print(f"Timed out waiting for elements while navigating to {ticker_to_work}. Retry {current_try + 1}/{max_retries}")
                current_try += 1
            except NoSuchElementException:
                print(f"Element not found while navigating to {ticker_to_work}. Retry {current_try + 1}/{max_retries}")
                current_try += 1

    def navigate_to_ticker_page(self, ticker_to_work='ORCL'):
        max_retries = 3
        current_try = 0

        while current_try < max_retries:
            try:
                # Wait for search bar to be present and then interact
                search_bar = WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[data-id="ViewPort_Navigation_symbolSearchInput"]'))
                )
                search_bar.clear()
                time.sleep(0.5)
                search_bar.send_keys(ticker_to_work)
                time.sleep(1)  # Wait for dropdown to populate

                # Wait for dropdown to appear and be populated
                WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li.SymbolSearch---menuItem---xwN3j'))
                )

                dropdown_menu = self.firefox_driver.find_element(By.CSS_SELECTOR, 'ul.SymbolSearch---menu---98GcZ')

                # Loop through dropdown items
                for item in dropdown_menu.find_elements(By.CSS_SELECTOR, 'li.SymbolSearch---menuItem---xwN3j'):
                    symbol = item.find_element(By.CSS_SELECTOR, 'span.SymbolSearch---symbol---BQiqg').text
                    if symbol == ticker_to_work:
                        # Use ActionChains for better reliability
                        actions = ActionChains(self.firefox_driver)
                        actions.move_to_element(item).click().perform()
                        return  # Successfully clicked; exit method

                current_try += 1  # Increment retry count if not successful

            except TimeoutException:
                print(f"Timed out waiting for elements. Retry {current_try + 1}/{max_retries}")
                current_try += 1
            except NoSuchElementException:
                print(f"Element not found. Retry {current_try + 1}/{max_retries}")
                current_try += 1

        print(f"Failed to navigate to {ticker_to_work} after {max_retries} attempts")

    def pop_up_sell_dialogue(self, ticker_to_verify):
        try:
            # Wait until the page is fully loaded
            WebDriverWait(self.firefox_driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'body'))
            )

            psource = self.firefox_driver.page_source

            if ticker_to_verify in psource:
                # Explicitly wait for the sell button to appear
                bid_button = WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-id="QuoteBar_bidAskBtn_SELL"]'))
                )
                bid_button.click()
            else:
                print('Ticker not found in page source.')

        except Exception as e:
            print(f"An error occurred: {e}")

    def pop_up_buy_dialogue(self, ticker_to_verify):
        try:
            # Wait until the page is fully loaded
            WebDriverWait(self.firefox_driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'body'))
            )

            psource = self.firefox_driver.page_source

            if ticker_to_verify in psource:
                # Explicitly wait for the buy button to appear
                ask_button = WebDriverWait(self.firefox_driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-id="QuoteBar_bidAskBtn_BUY"]'))
                )
                ask_button.click()
            else:
                print('Ticker not found in page source.')

        except Exception as e:
            print(f"An error occurred: {e}")



    def specify_share_number_for_bid_or_ask(self,share_number):
    # Locate the input field for Quantity
       # quantity_input =self.firefox_driver.find_element(By.CSS_SELECTOR, 
       #                                                  'input[data-id="OrderTicket_Leg_qtyInput"]')

        # Focus the input field
        #quantity_input.click()

        wait = WebDriverWait(self.firefox_driver, 10)  # Wait up to 10 seconds
        quantity_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[data-id="OrderTicket_Leg_qtyInput"]')))
        quantity_input.click()

        # Get the existing value's length
        existing_value_length = len(quantity_input.get_attribute("value"))

        # Simulate pressing backspace for each existing character
        for _ in range(existing_value_length):
            quantity_input.send_keys(Keys.BACK_SPACE)

        # Input new value, for example, 50
        quantity_input.send_keys(f"{share_number}")

    def only_proceed_if_order_box_contains_ticker(self,ticker_to_work = 'AAPL',timeout = 30):
        try:
            # Wait until the 'value' attribute of the input box becomes 'UAL'
            element = WebDriverWait(self.firefox_driver, timeout).until(
                EC.text_to_be_present_in_element_value(
                    (By.CSS_SELECTOR, "input[data-id='OrderTicketQuote_symbolSearchInput']"), ticker_to_work)
            )
            print(f"The Order Ticket box now contains: {ticker_to_work}")
        except TimeoutException:
            print("Timed out waiting for the Order Ticket box to contain: UAL")


    def get_live_price_from_bid_page(self):
        try:
            # Wait until the span is fully loaded
            bid_span = WebDriverWait(self.firefox_driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                                                'button[data-id="QuoteBar_bidAskBtn_SELL"] span.Button---text---veQ0m.BidAskButton---text---nymYZ'))
            )
            return bid_span.text  # Assuming you want to return the text content within the span
        except Exception as e:
            print(f"An error occurred: {e}")
            return None  #

    def open_duration_dropdown_button(self):
        ''' 
        duration_dropdown = self.firefox_driver.find_element(By.CSS_SELECTOR, 
                                                             'div.Duration---dropdown---uU0yc button.Button---root---R8Eaq')

        # Click to open the dropdown
        duration_dropdown.click()
        ''' 
        try:
            # Explicitly wait for the duration dropdown button to appear
            duration_dropdown = WebDriverWait(self.firefox_driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                                                'div.Duration---dropdown---uU0yc button.Button---root---R8Eaq'))
            )

            # Click to open the dropdown
            duration_dropdown.click()

        except Exception as e:
            print(f"An error occurred: {e}")


    def select_CLO_from_dropdown_menu(self):
        menu_item = WebDriverWait(self.firefox_driver, 5).until(
            EC.visibility_of_element_located((By.XPATH, "//span[text()='CLO - On the Close']"))
        )
        menu_item.click()


    def replace_limit_price_with_new_price(self,limit_price_to_send):
        limit_price_input = self.firefox_driver.find_element(By.NAME, 'limitPrice')

        # Simulate pressing backspace 5 times to remove existing value "66.66"
        for _ in range(10):
            limit_price_input.send_keys(Keys.BACK_SPACE)

        # Send the new value
        limit_price_input.send_keys(f"{limit_price_to_send}")

    def preview_and_submit_order__legacy(self):
        wait = WebDriverWait(self.firefox_driver, 10)  # wait for up to 10 seconds
        preview_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-id="OrderTicketFooter_previewButton"]')))
        preview_button.click()
        psource = self.firefox_driver.page_source


        if (('cannot be processed through this platform' in psource) == False) | (('cannot be sold short as its current price' in psource) == False) | (('Hard to Borrow' in psource) == False):
            print('Ticker has an issue')

        if (('cannot be processed through this platform' in psource) == False) & (('cannot be sold short as its current price' in psource) == False) & (('Hard to Borrow' in psource) == False):
            #send_button = self.firefox_driver.find_element(By.XPATH, '//button/span[text()="Send"]')
            #self.firefox_driver.execute_script("arguments[0].click();", send_button)
            #print('executed order')
            wait = WebDriverWait(self.firefox_driver, 10)
            send_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[span[text()='Send']]")))
            send_button.click()
            #close_button = self.firefox_driver.find_element_by_css_selector('button[data-id="OrderTicketHeader_closeButton"]')
            #close_button.click()


    def preview_and_submit_order(self):
        max_retries = 3  # Number of retries
        retries = 0

        while retries < max_retries:
            try:
                # Wait and click the Preview button
                wait = WebDriverWait(self.firefox_driver, 10)
                preview_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 
                                                                        'button[data-id="OrderTicketFooter_previewButton"]')))
                preview_button.click()

                # Check page source for specific text
                psource = self.firefox_driver.page_source
                if ('cannot' not in psource) & ('not allowed' not in psource):
                    send_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[span[text()='Send']]")))
                    send_button.click()
                    return  # Successfully clicked, exit loop
                else:
                    print('Not sending due to some condition.')
                    return  # Exit loop, no retries needed
            except TimeoutException:
                print('Retrying...')
                retries += 1  # Increase retry count

        print('Failed to execute action after maximum retries.')



    def preview_and_submit_order__aggressive(self):
        wait = WebDriverWait(self.firefox_driver, 10)  # wait for up to 10 seconds
        preview_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-id="OrderTicketFooter_previewButton"]')))
        preview_button.click()
        wait = WebDriverWait(self.firefox_driver, 10)
        send_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[span[text()='Send']]")))
        send_button.click()

    def wait_until_order_is_sent(self):
        try:
            element = WebDriverWait(self.firefox_driver, 10).until(
                EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".OrderConfirmDetails---successResult---L8oT"), "Order Sent")
            )
            print("Element found.")
        except TimeoutException:
            print("Element not found within time limit.")

    def id_generator(self):
        chars=[i for i in string.ascii_uppercase]
        a=np.random.choice(chars)
        b=np.random.choice(chars)
        c=np.random.choice(chars)
        output=a+b+c
        return output

    def check_if_short_is_hard_to_borrow(self):
        htb = False
        try:
            element = self.firefox_driver.find_element(By.XPATH, "//p[contains(text(), 'Hard to Borrow')]")
            htb= False
            if element:
                print("The ticker is Hard to Borrow.")
                htb=True
            else:
                print("The ticker is not Hard to Borrow.")
        except:
            pass
        return htb
    
    def place_limit_on_close_short_order(self, ticker_to_work = 'IGSB',share_count = 20,discount_to_work = .04):
        #ticker_to_work=ticker_to_work.replace('-','.')
        self.navigate_to_ticker_page(ticker_to_work)
        self.pop_up_sell_dialogue(ticker_to_verify=ticker_to_work)
        # self.only_proceed_if_order_box_contains_ticker(ticker_to_work=ticker_to_work,
        #                                               timeout = 30)
        self.specify_share_number_for_bid_or_ask(share_count)
        live_price = self.get_live_price_from_bid_page()
        price_value = float(live_price)

        limit_price_to_work = (1-discount_to_work)*price_value
        self.open_duration_dropdown_button()
        self.select_CLO_from_dropdown_menu()
        limit_price_to_send = round(limit_price_to_work,2)

        #limit_price_field = self.firefox_driver.find_element(By.NAME, "limitPrice")

        # Clear the existing value
        #limit_price_field.clear()

        # Input the new value
        self.replace_limit_price_with_new_price(limit_price_to_send=limit_price_to_send)
        self.preview_and_submit_order__aggressive()
        psource = self.firefox_driver.page_source
        if (('cannot be processed through this platform' in psource) == True) | (('cannot be sold short as its current price' in psource) == True) | (('Hard to Borrow' in psource) == True):
            self.hard_to_borrow_arr.append(ticker_to_work)

        if (('cannot be processed through this platform' in psource) == False) & (('cannot be sold short as its current price' in psource) == False) & (('Hard to Borrow' in psource) == False):
            self.wait_until_order_is_sent()
        

    def open_price_type_dropdown(self):
        price_type_dropdown = self.firefox_driver.find_element(By.CSS_SELECTOR, '[data-id="OrderTicketSettings_priceTypeDropdown"] button')
        price_type_dropdown.click()
    def select_market_order_type_from_price_dropdown(self):
        market_option = WebDriverWait(self.firefox_driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//a[@title="Market"]')))
        market_option.click()

    def place_market_on_close_buy_order(self,ticker_to_work='TLT',share_count=5):
        # ticker_to_work=ticker_to_work.replace('-','.')
        self.navigate_to_ticker_page(ticker_to_work)
        self.pop_up_buy_dialogue(ticker_to_verify=ticker_to_work)
        self.only_proceed_if_order_box_contains_ticker(ticker_to_work=ticker_to_work,
                                                       timeout = 30)
        self.specify_share_number_for_bid_or_ask(share_count)
        self.open_price_type_dropdown()
        self.select_market_order_type_from_price_dropdown()
        # Locate and click on the "Limit" option
        self.open_duration_dropdown_button()
        self.select_CLO_from_dropdown_menu()
        self.preview_and_submit_order__aggressive()
        psource = self.firefox_driver.page_source
        if (('cannot be processed through this platform' in psource) == False) & (('cannot be sold short as its current price' in psource) == False) & (('Hard to Borrow' in psource) == False):

            self.wait_until_order_is_sent()



    def place_limit_on_close_buy_order(self, ticker_to_work='LILM',share_count=1,discount_to_work=.04):
        # ticker_to_work=ticker_to_work.replace('-','.')
        self.navigate_to_ticker_page(ticker_to_work)

        
        self.pop_up_buy_dialogue(ticker_to_verify=ticker_to_work)
        
        self.only_proceed_if_order_box_contains_ticker(ticker_to_work=ticker_to_work,
                                                       timeout = 30)
        self.specify_share_number_for_bid_or_ask(share_count)

        #
        live_price = self.get_live_price_from_bid_page()
        try:
            price_value = float(live_price)

            limit_price_to_work = (1+discount_to_work)*price_value
            self.open_duration_dropdown_button()
            self.select_CLO_from_dropdown_menu()
            limit_price_to_send = round(limit_price_to_work,2)

            #limit_price_field = self.firefox_driver.find_element(By.NAME, "limitPrice")

            # Clear the existing value
            #limit_price_field.clear()
    

            # Input the new value
            self.replace_limit_price_with_new_price(limit_price_to_send=limit_price_to_send)
            self.preview_and_submit_order__aggressive()
            psource = self.firefox_driver.page_source

            if (('cannot be processed through this platform' in psource) == True) | (('cannot be sold short as its current price' in psource) == True) | (('Hard to Borrow' in psource) == True):
                    self.hard_to_borrow_arr.append(ticker_to_work)
            if (('cannot be processed through this platform' in psource) == False) & (('cannot be sold short as its current price' in psource) == False) & (('Hard to Borrow' in psource) == False):
                self.wait_until_order_is_sent()
        except:
            print('no valid price')
            pass



class EtradeTool:
    def __init__(self, pw_map,driver_type):
        self.pw_map = pw_map
        self.consumer_key = self.pw_map['etrade_prod']
        self.consumer_secret = self.pw_map['etrade_secret']
        self.base_url = 'https://api.etrade.com'
        self.google_sheet_manager = GoogleSheetManager(prod_trading=True)
        self.etrade_ux_driver = ETradeUXDriver(pw_map=self.pw_map, driver_type=driver_type)
        self.tiingo_data_tool= TiingoDataTool(pw_map=self.pw_map)
        def oauth():
            """Allows user authorization for the sample application with OAuth 1"""
            etrade = OAuth1Service(
                name="etrade",
                consumer_key=self.consumer_key,
                consumer_secret=self.consumer_secret,
                request_token_url="https://api.etrade.com/oauth/request_token",
                access_token_url="https://api.etrade.com/oauth/access_token",
                authorize_url="https://us.etrade.com/e/t/etws/authorize?key={}&token={}",
                base_url="https://api.etrade.com")

            menu_items = {"1": "Sandbox Consumer Key",
                          "2": "Live Consumer Key",
                          "3": "Exit"}
            base_url = 'https://api.etrade.com'

            # Step 1: Get OAuth 1 request token and secret
            request_token, request_token_secret = etrade.get_request_token(
                params={"oauth_callback": "oob", "format": "json"})

            # Step 2: Go through the authentication flow. Login to E*TRADE.
            # After you login, the page will provide a verification code to enter.
            authorize_url = etrade.authorize_url.format(etrade.consumer_key, request_token)
            #webbrowser.open(authorize_url)
            yc = input("Any Key to Move on")
            print('Alex')
            print(authorize_url)
            print('Alex')
            yc = input("Any Key to Move on Again")
            print(authorize_url)

            text_code = input("Please accept agreement and enter verification code from browser: ")

            # Step 3: Exchange the authorized request token for an authenticated OAuth 1 session
            session = etrade.get_auth_session(request_token,
                                          request_token_secret,
                                          params={"oauth_verifier": text_code})
            return session

        self.session = self.etrade_ux_driver.session
        
        
        def given_session_generate_account_map(session):
            url = self.base_url + "/v1/accounts/list.json"
            xacct_text=session.get(url, header_auth=True)
            all_account_list = xacct_text.json()
            account= all_account_list['AccountListResponse']['Accounts']['Account'][0]
            return account

        self.account_map = given_session_generate_account_map(session=self.session)
        
    def id_generator(self):
        chars=[i for i in string.ascii_uppercase]
        a=np.random.choice(chars)
        b=np.random.choice(chars)
        c=np.random.choice(chars)
        output=a+b+c
        return output
        
    def place_market_on_open_order(self, symbol='SPY',order_action='BUY', quantity= 1,parent_strategy='flow'):
        
        ''' 
        example 
        symbol='UGLD', order_action='BUY', quantity= 1,parent_strategy='flow'
        '''
        session=self.session
        consumer_key=self.consumer_key
        consumer_secret=self.consumer_secret
        dir_conv={'BUY':'b',
                'SELL':'s',
                'BUY_TO_COVER':'bc',
                'SELL_SHORT':'ss'}

        time_of_trade='o'
        date_string = datetime.datetime.now().strftime('%m%d%y')
        ''' options for time of trade c: Close, o: Open, i: intraday'''
        short_dir = dir_conv[order_action]
        headers = {"Content-Type": "application/xml", "consumerKey": consumer_key}
        rand_id=self.id_generator()
        client_order_id = f'{rand_id}{parent_strategy}{symbol}{date_string}{short_dir}{time_of_trade}'
        date_string = datetime.datetime.now().strftime('%m%d%y')
        url = self.base_url + "/v1/accounts/" + self.account_map["accountIdKey"] + "/orders/preview.json"
        consumer_key=consumer_key

        price_type = 'MARKET'
        order_term = 'GOOD_FOR_DAY'

        # Add payload for POST Request
        payload = f"""<PreviewOrderRequest>
                    <orderType>EQ</orderType>
                    <clientOrderId>{client_order_id}</clientOrderId>
                    <Order>
                        <allOrNone>false</allOrNone>
                        <priceType>{price_type}</priceType>
                        <orderTerm>{order_term}</orderTerm>
                        <marketSession>REGULAR</marketSession>
                        <Instrument>
                            <Product>
                                <securityType>EQ</securityType>
                                <symbol>{symbol}</symbol>
                            </Product>
                            <orderAction>{order_action}</orderAction>
                            <quantityType>QUANTITY</quantityType>
                            <quantity>{quantity}</quantity>
                        </Instrument>
                    </Order>
                </PreviewOrderRequest>"""
        response = session.post(url, header_auth=True, headers=headers, data=payload)
        preview_json = response.json()
        print(preview_json)
        preview_id = preview_json['PreviewOrderResponse']['PreviewIds'][0]['previewId']
        order_payload =f''' <PlaceOrderRequest>
        <orderType>EQ</orderType>
        <clientOrderId>{client_order_id}</clientOrderId>
        <PreviewIds>
            <previewId>{preview_id}</previewId>
        </PreviewIds>
        <Order>
            <allOrNone>false</allOrNone>
            <priceType>{price_type}</priceType>
            <orderTerm>{order_term}</orderTerm>
            <marketSession>REGULAR</marketSession>
            <stopPrice />
            <Instrument>
                <Product>
                    <securityType>EQ</securityType>
                    <symbol>{symbol}</symbol>
                </Product>
                <orderAction>{order_action}</orderAction>
                <quantityType>QUANTITY</quantityType>
                <quantity>{quantity}</quantity>
            </Instrument>
        </Order>
        </PlaceOrderRequest>'''
        accountIdKey= self.account_map["accountIdKey"]
        order_url = f'https://api.etrade.com/v1/accounts/{accountIdKey}/orders/place'
        response = session.post(order_url, header_auth=True, headers=headers, data=order_payload)
        print(response.text)
        print('placed MARKET '+symbol+" "+order_action+' '+str(quantity))
        
    def place_market_on_close_order(self, symbol='SPY',order_action='BUY', quantity= 1,parent_strategy='flow'):

        ''' 
        example 
        symbol='UGLD', order_action='BUY', quantity= 1,parent_strategy='flow'
        '''
        session=self.session
        consumer_key=self.consumer_key
        consumer_secret=self.consumer_secret
        dir_conv={'BUY':'b',
                'SELL':'s',
                'BUY_TO_COVER':'bc',
                'SELL_SHORT':'ss'}

        time_of_trade='o'
        date_string = datetime.datetime.now().strftime('%m%d%y')
        ''' options for time of trade c: Close, o: Open, i: intraday'''
        short_dir = dir_conv[order_action]
        headers = {"Content-Type": "application/xml", "consumerKey": consumer_key}
        rand_id=self.id_generator()
        client_order_id = f'{rand_id}{parent_strategy}{symbol}{date_string}{short_dir}{time_of_trade}'
        date_string = datetime.datetime.now().strftime('%m%d%y')
        url = self.base_url + "/v1/accounts/" + self.account_map["accountIdKey"] + "/orders/preview.json"
        consumer_key=consumer_key

        price_type = 'MARKET_ON_CLOSE'
        order_term = 'GOOD_FOR_DAY'

        # Add payload for POST Request
        payload = f"""<PreviewOrderRequest>
                    <orderType>EQ</orderType>
                    <clientOrderId>{client_order_id}</clientOrderId>
                    <Order>
                        <allOrNone>false</allOrNone>
                        <priceType>{price_type}</priceType>
                        <orderTerm>{order_term}</orderTerm>
                        <marketSession>REGULAR</marketSession>
                        <Instrument>
                            <Product>
                                <securityType>EQ</securityType>
                                <symbol>{symbol}</symbol>
                            </Product>
                            <orderAction>{order_action}</orderAction>
                            <quantityType>QUANTITY</quantityType>
                            <quantity>{quantity}</quantity>
                        </Instrument>
                    </Order>
                </PreviewOrderRequest>"""
        response = session.post(url, header_auth=True, headers=headers, data=payload)
        preview_json = response.json()
        preview_id = preview_json['PreviewOrderResponse']['PreviewIds'][0]['previewId']
        order_payload =f''' <PlaceOrderRequest>
        <orderType>EQ</orderType>
        <clientOrderId>{client_order_id}</clientOrderId>
        <PreviewIds>
            <previewId>{preview_id}</previewId>
        </PreviewIds>
        <Order>
            <allOrNone>false</allOrNone>
            <priceType>{price_type}</priceType>
            <orderTerm>{order_term}</orderTerm>
            <marketSession>REGULAR</marketSession>
            <stopPrice />
            <Instrument>
                <Product>
                    <securityType>EQ</securityType>
                    <symbol>{symbol}</symbol>
                </Product>
                <orderAction>{order_action}</orderAction>
                <quantityType>QUANTITY</quantityType>
                <quantity>{quantity}</quantity>
            </Instrument>
        </Order>
        </PlaceOrderRequest>'''
        accountIdKey= self.account_map["accountIdKey"]
        order_url = f'https://api.etrade.com/v1/accounts/{accountIdKey}/orders/place'
        response = session.post(order_url, header_auth=True, headers=headers, data=order_payload)
        print(response.text)
        print('placed MOC '+symbol+" "+order_action+' '+str(quantity))

    def output_rejected_order_df(self):
        account_map = self.account_map
        session = self.session
        consumer_key = self.consumer_key
        order_arr = []
        headers = {"consumerkey": consumer_key}
        params_rejected = {"status": "REJECTED", 'count': 100}

        base_url = 'https://api.etrade.com'
        orders_json = base_url + "/v1/accounts/" + account_map['accountIdKey'] + "/orders.json"
        response_rejected = session.get(orders_json, header_auth=True, params=params_rejected, headers=headers)
        all_orders = response_rejected.json()
        order_arr.append(all_orders)

        iterx = 0
        while iterx < 15:
            next_string = ''
            next_token = ''
            try:
                next_string = all_orders['OrdersResponse']['next']
                next_token = next_string.split('marker=')[1].split('&')[0]
            except:
                pass
            if next_token != '':
                params_rejected = {"status": "REJECTED", 'count': 100, 'marker': next_token}
                response_rejected = session.get(orders_json, header_auth=True, params=params_rejected, headers=headers)
                all_orders = response_rejected.json()
                order_arr.append(all_orders)
            iterx += 1

        full_order_arr = []
        for orders_to_work in order_arr:
            single_order_to_work = orders_to_work['OrdersResponse']['Order']
            temp_order_df = pd.DataFrame(single_order_to_work).copy()
            for xfield in ['placedTime', 'orderValue', 'status', 'orderTerm', 'priceType', 
                        'limitPrice', 'stopPrice', 'marketSession', 'allOrNone', 'netPrice', 
                        'netBid', 'netAsk', 'gcd', 'ratio', 'Instrument']:
                temp_order_df[xfield] = temp_order_df['OrderDetail'].apply(lambda x: x[0][xfield])
            
            instrument_fields = ['symbolDescription', 'orderAction', 'quantityType', 
                                'orderedQuantity', 'filledQuantity', 'estimatedCommission', 
                                'estimatedFees', 'Product']
            for xinstr in instrument_fields:
                temp_order_df[xinstr] = temp_order_df['Instrument'].apply(lambda x: x[0][xinstr])
            
            temp_order_df['symbol'] = temp_order_df['Product'].apply(lambda x: x['symbol'])
            temp_order_df['securityType'] = temp_order_df['Product'].apply(lambda x: x['securityType'])
            full_order_arr.append(temp_order_df)

        final_output = pd.concat(full_order_arr)
        final_output['orderAction'] = final_output['Instrument'].apply(lambda x: x[0]['orderAction'])
        final_output['orderedQuantity'] = final_output['Instrument'].apply(lambda x: x[0]['orderedQuantity'])
        final_output['placedTime'] = final_output['OrderDetail'].apply(lambda x: x[0]['placedTime'])
        final_output['placed_datetime'] = final_output['placedTime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
        final_output['placed_date'] = final_output['placed_datetime'].apply(lambda x: pd.to_datetime(x.strftime('%Y-%m-%d')))
        final_output['placed_hour'] = final_output['placed_datetime'].apply(lambda x: x.hour)
        final_output['placed_minute'] = final_output['placed_datetime'].apply(lambda x: x.minute)
        
        # Add rejection reason if available
        final_output['rejectionReason'] = final_output['OrderDetail'].apply(lambda x: x[0].get('rejectionReason', 'Not specified'))

        return final_output


    def get_open_orders(self):
        account_map=self.account_map
        session=self.session
        consumer_key=self.consumer_key
        consumer_secret=self.consumer_secret
        order_arr=[]
        account=account_map
        consumer_key=consumer_key
        headers = {"consumerkey": consumer_key}
        params_open = {"status": "OPEN",
                    'count':100}
        #            params_executed = {"status": "EXECUTED"}
        #            params_indiv_fills = {"status": "INDIVIDUAL_FILLS"}
        #            params_cancelled = {"status": "CANCELLED"}
        #            params_rejected = {"status": "REJECTED"}
        #            params_expired = {"status": "EXPIRED"}

        base_url ='https://api.etrade.com'
        orders_json=base_url + "/v1/accounts/" + account_map['accountIdKey'] + "/orders.json"
        response_open = session.get(orders_json, header_auth=True, params=params_open, headers=headers)
        all_orders=response_open.json()
        order_arr.append(all_orders)
        iterx=0
        while iterx<15:
            next_string=''
            next_token=''
            try:
                next_string = all_orders['OrdersResponse']['next']
                next_token=next_string.split('marker=')[1].split('&')[0]
            except:
                pass
            if next_token!='':
                account=account_map['accountId']
                consumer_key=consumer_key
                headers = {"consumerkey": consumer_key}
                params_open = {"status": "OPEN",
                            'count':100,'marker':next_token}
                #            params_executed = {"status": "EXECUTED"}
                #            params_indiv_fills = {"status": "INDIVIDUAL_FILLS"}
                #            params_cancelled = {"status": "CANCELLED"}
                #            params_rejected = {"status": "REJECTED"}
                #            params_expired = {"status": "EXPIRED"}

                base_url ='https://api.etrade.com'
                orders_json=base_url + "/v1/accounts/" + account_map['accountIdKey'] + "/orders.json"
                response_open = session.get(orders_json, header_auth=True, params=params_open, headers=headers)
                all_orders=response_open.json()
                order_arr.append(all_orders)
            iterx = iterx+1
        full_order_arr=[]
        for orders_to_work in order_arr:
            #orders_to_work=order_arr[0]
            #orders_to_work=order_arr[0]
            single_order_to_work=orders_to_work['OrdersResponse']['Order']
            temp_order_df = pd.DataFrame(single_order_to_work).copy()
            for xfield in ['placedTime', 
                               'orderValue', 
                               'status', 'orderTerm', 'priceType', 
                               'limitPrice', 'stopPrice', 'marketSession', 
                               'allOrNone', 'netPrice', 'netBid', 
                               'netAsk', 'gcd', 'ratio', 'Instrument']:
                temp_order_df[xfield]=temp_order_df['OrderDetail'].apply(lambda x: x[0][xfield])
            instrument_fields = ['symbolDescription', 'orderAction', 'quantityType', 
                                     'orderedQuantity', 'filledQuantity', 
                                     'estimatedCommission', 'estimatedFees', 'Product']
            for xinstr in instrument_fields:
                temp_order_df[xinstr]=temp_order_df['Instrument'].apply(lambda x: x[0][xinstr])
            #    temp_order_df[xinstr]=temp_order_df['Instrument'].apply(lambda x: x[0][xinstr])
            #    temp_order_df['symbol']=temp_order_df['Product'].apply(lambda x: x['symbol'])
            #    temp_order_df['securityType']=temp_order_df['Product'].apply(lambda x: x['securityType'])   
            temp_order_df['symbol']=temp_order_df['Product'].apply(lambda x: x['symbol'])
            temp_order_df['securityType']=temp_order_df['Product'].apply(lambda x: x['securityType'])
            full_order_arr.append(temp_order_df)
        final_output = pd.concat(full_order_arr)
        return final_output
    
    def cancel_order(self,order_id_to_cancel=59672):
        url = self.base_url + "/v1/accounts/" + self.account_map["accountIdKey"] + "/orders/cancel.json"

        # Add parameters and header information
        headers = {"Content-Type": "application/xml", "consumerKey": self.consumer_key}

        # Add payload for POST Request
        payload =f"""<CancelOrderRequest>
                        <orderId>{order_id_to_cancel}</orderId>
                    </CancelOrderRequest>
                """
        #payload = payload.format(order_list[int(selection) - 1])

        # Add payload for PUT Request
        response = self.session.put(url, header_auth=True, headers=headers, data=payload)
        print(response.text)
        
    def get_current_position_df(self):
        session=self.session
        account_map=self.account_map
        rblock=[]
        #account_map =account_map
        account=account_map
        base_url ='https://api.etrade.com'

        for xr in range(1,10):
            try:

                # URL for the API endpoint
                url =base_url + "/v1/accounts/" + account["accountIdKey"] + "/portfolio.json"
                params = {'count': '200','pageNumber':xr}
                response = session.get(url, header_auth=True, params=params)
                rblock.append(response)
            except:
                pass
        valid_responses=[i for i in rblock if i.status_code==200]


        def convert_response_to_df(resp_to_w=valid_responses[0]):
            posi_block=pd.DataFrame(resp_to_w.json()['PortfolioResponse']['AccountPortfolio'][0]['Position'])
            return posi_block
        full_df = pd.concat([convert_response_to_df(resp_to_w=x) for x in valid_responses])
        full_position_df = full_df
        return full_position_df


    def cancel_all_dummy_orders(self):
        order_df = self.get_orders()
        open_orders = order_df[order_df['status']=="OPEN"].copy()
        dummy_orders  = list(open_orders[open_orders['symbol']=="FLOT"]['orderId'])
        for xorder in dummy_orders:
            self.cancel_order(order_id_to_cancel=xorder)
            time.sleep(.1)


    def calculate_orders(self, ticker, current, target):
        y = [('DUMMY','BUY',0)]
        if (current >0) & (target == 0):
            # SELL
            y.append((ticker, 'SELL',np.abs(current)))

        if (current <0) & (target ==0):
            y.append((ticker, 'BUY_TO_COVER', np.abs(current)))
            # BUY_TO_COVER

        if (current >0) & (target <0):
            y.append((ticker,'SELL', np.abs(current)))
            y.append((ticker,'SELL_SHORT',np.abs(target)))
            # SELL AND GO SHORT

        if (current <0) & (target >0):
            y.append((ticker,'BUY_TO_COVER', np.abs(current)))
            y.append((ticker,'BUY', np.abs(target)))
            # BUY_TO_COVER AND GO LONG
        if (current >=0) & (target>0):
            if ((target-current) >0):
                y.append((ticker,'BUY', np.abs(target-current)))
            if ((target-current) <0):
                y.append((ticker,'SELL', np.abs(target-current)))
            # BUY

        if (current <=0)& (target<0):
            if ((current-target) >0):
                y.append((ticker,'SELL_SHORT', np.abs(target-current)))
            if ((current-target) <0):
                y.append((ticker,'BUY_TO_COVER', np.abs(target-current)))
        return y

    def try_place_market_on_close_order(self,symbol, order_action,quantity,parent_strategy,sleep_time=.5):
        try:
            time.sleep(sleep_time)
            self.place_market_on_close_order(symbol=symbol, order_action=order_action,
                                           quantity=quantity, parent_strategy=parent_strategy)
        except:
            print('failed placing MOC order on '+symbol)
            pass
        
    def try_place_market_on_open_order(self,symbol, order_action,quantity,parent_strategy,sleep_time=.5):
        try:
            time.sleep(sleep_time)
            self.place_market_on_open_order(symbol=symbol, order_action=order_action,
                                           quantity=quantity, parent_strategy=parent_strategy)
        except:
            print('failed placking market order on '+symbol)
            pass
        
    def try_place_limit_on_close_short_order(self,symbol,quantity,discount_to_work):
        try:
            #time.sleep(sleep_time)
            self.etrade_ux_driver.place_limit_on_close_short_order(ticker_to_work=symbol,share_count=quantity,discount_to_work=discount_to_work)
        except:
            print('failed placing LIMIT ON CLOSE UX SHORT order on '+symbol)
            pass

    def generate_production_realignment_order_frame_and_outstanding_orders(self,session_to_realign_to='nt'):
        #session_to_realign_to='nt'
        try:
            time.sleep(1)
            self.place_market_on_close_order(symbol='FLOT',
                        order_action='BUY',
                        quantity=1)
        except:
            print('failed FLOT buy')
            pass
        current_posis= self.get_current_position_df()
        current_values = current_posis.groupby('symbolDescription').sum(numeric_only=True)['quantity']
        target_values=self.google_sheet_manager.load_google_sheet_as_df(workbook='odv', worksheet='etrade_target')
        target_values=target_values[target_values['session']==session_to_realign_to].copy()

        tquantity=target_values.groupby('localSymbol').first()['position']
        cdf= pd.concat([current_values,tquantity],axis=1)
        cdf.columns=['current','target']
        cdf=cdf.astype(float)
        cdf = cdf.loc[[i for i in cdf.index if i.isalpha()]].copy()
        cdf.index.name = 'ticker'
        rebalance_df=cdf.reset_index()
        rebalance_df=rebalance_df.fillna(0)
        rebalance_df['all_orders']=rebalance_df.apply(lambda x: self.calculate_orders(x['ticker'],
                                                      x['current'],
                                                      x['target']),1)
        o_df = pd.DataFrame(list(itertools.chain.from_iterable(
            list(rebalance_df['all_orders']))))
        o_df.columns = ['symbol','order_action','quantity']
        full_order_df = o_df[o_df['quantity']!=0].copy()
        full_order_df['symbol__order_action'] = full_order_df['symbol']+'__'+full_order_df['order_action']
        existing_order_frame =self.get_open_orders()
        existing_order_frame['symbol__order_action']=existing_order_frame['symbol']+'__'+existing_order_frame['orderAction']
        existing_order_frame['quantity']=existing_order_frame['orderedQuantity'].astype(float)
        outstanding_order_map = existing_order_frame.groupby(
            'symbol__order_action').sum(numeric_only=True)['quantity']
        full_order_df['outstanding_order']=full_order_df['symbol__order_action'].map(outstanding_order_map)
        full_order_df['orders_to_work']=full_order_df['quantity'].fillna(0)-full_order_df['outstanding_order'].fillna(0)
        priority_map = {'SELL':1,'BUY_TO_COVER':2,'BUY':3,'SELL_SHORT':4}
        full_order_df['priority']=full_order_df['order_action'].map(priority_map)
        all_order_symbols= list(set(list(existing_order_frame['symbol'])))
        order_frame_to_work = full_order_df.sort_values('priority', ascending=True)
        order_frame_to_execute = order_frame_to_work.copy()
        order_frame_to_execute['orders_to_work']=order_frame_to_execute['orders_to_work'].astype(int)
        order_frame_to_execute['session_to_realign_to']=session_to_realign_to
        return {'order_frame_to_execute': order_frame_to_execute, 
                'existing_order_frame': existing_order_frame}

    def cancel_all_detritus_orders(self,session_to_realign_to='nt'):
        realignment_map = self.generate_production_realignment_order_frame_and_outstanding_orders(session_to_realign_to=session_to_realign_to)
        outstanding_order_actions = list(set(realignment_map['existing_order_frame']['symbol__order_action']))
        feasible_order_actions = list(set(realignment_map['order_frame_to_execute']['symbol__order_action']))
        exogenous_orders_to_cancel =[i for i in outstanding_order_actions if i not in feasible_order_actions]
        outstanding_orders_now = realignment_map['order_frame_to_execute'][
        realignment_map['order_frame_to_execute']['outstanding_order'].abs()>0].copy()
        outstanding_orders_with_wrong_quantity = outstanding_orders_now[
        outstanding_orders_now['outstanding_order']!=outstanding_orders_now['quantity']].copy()
        wrong_quantitity_orders_to_cancel = list(set(outstanding_orders_with_wrong_quantity['symbol__order_action']))
        symbol_order_actions_to_cxl = exogenous_orders_to_cancel+wrong_quantitity_orders_to_cancel
        all_order_ids_to_cxl= list(realignment_map['existing_order_frame'][
        realignment_map['existing_order_frame']['symbol__order_action'].apply(lambda x: 
                                                                              x in symbol_order_actions_to_cxl)]['orderId'])
        for xorder in list(set(all_order_ids_to_cxl)):
            self.cancel_order(xorder)
        print('cleared detritus')

    def execute_market_on_close_realignment(self):
        
        session_to_realign_to='nt'
        self.cancel_all_detritus_orders(session_to_realign_to=session_to_realign_to)
        op= self.generate_production_realignment_order_frame_and_outstanding_orders(session_to_realign_to=session_to_realign_to)
        if session_to_realign_to == 'nt':
            op['order_frame_to_execute_via_UX']= op['order_frame_to_execute'][
                op['order_frame_to_execute']['symbol__order_action'].apply(lambda x: 
                                                                           'SELL_SHORT' in x)].copy()
            op['order_frame_to_execute']= op['order_frame_to_execute'][
                op['order_frame_to_execute']['symbol__order_action'].apply(lambda x: 
                                                                           'SELL_SHORT' not in x)].copy()


        non_zero_orders = op['order_frame_to_execute'][op['order_frame_to_execute']['orders_to_work']!=0].copy()
        non_zero_orders.apply(lambda x: self.try_place_market_on_close_order(symbol=x['symbol'],
                                                                                     order_action=x['order_action'], quantity=x['orders_to_work'], 
                                                                                              parent_strategy='flw'),1)

        non_zero_orders_ux = op['order_frame_to_execute_via_UX'][op['order_frame_to_execute_via_UX']['orders_to_work']!=0].copy()
        non_zero_orders_ux.apply(lambda x: self.try_place_limit_on_close_short_order(symbol=x['symbol'],
                                                                                     quantity=x['orders_to_work'],
                                                                                     discount_to_work=.04),1)
    def execute_market_on_open_realignment(self):
        session_to_realign_to='dt'
        self.cancel_all_detritus_orders(session_to_realign_to=session_to_realign_to)
        op= self.generate_production_realignment_order_frame_and_outstanding_orders(session_to_realign_to=session_to_realign_to)
        non_zero_orders = op['order_frame_to_execute'][op['order_frame_to_execute']['orders_to_work']!=0].copy()
        non_zero_orders.apply(lambda x: self.try_place_market_on_open_order(symbol=x['symbol'],
                                                                                     order_action=x['order_action'], quantity=x['orders_to_work'], 
                                                                                              parent_strategy='flw'),1)
        

    def place_aggressor_moc_orders(self):
        op=self.generate_production_realignment_order_frame_and_outstanding_orders(session_to_realign_to='nt')
        oframe = op['order_frame_to_execute']
        oframe= oframe[oframe['orders_to_work']!=0].copy()
        rt_px_frm = self.tiingo_data_tool.output_tiingo_real_time_price_frame()
        tngo_last = rt_px_frm.groupby('ticker').first()['tngoLast']
        # last_price_df = self.td_ameritrade_tool.get_price_frame_for_ticker_list(list(set(list(oframe['symbol']))))
        oframe['lastPrice'] = oframe['symbol'].map(tngo_last)
        # Ensure the lastPrice column in oframe is of type float
        #oframe['lastPrice'] = oframe['symbol'].map(last_price_df.transpose()['lastPrice'].astype(float))
        # Split oframe into the three categories
        buy_above_102 = oframe[(oframe['order_action'] == 'BUY') & (oframe['lastPrice'] > 1.02)].copy()
        buy_below_102 = oframe[(oframe['order_action'] == 'BUY') & (oframe['lastPrice'] < 1.02)].copy()
        sell_short = oframe[oframe['order_action'] == 'SELL_SHORT'].copy()
        session_to_realign_to='nt'
        self.cancel_all_detritus_orders(session_to_realign_to=session_to_realign_to)
        def try_place_moc_buy_api(ticker_to_work,share_count):
            try:
                self.try_place_market_on_close_order(symbol=ticker_to_work, order_action='BUY',
                                                    quantity=share_count,parent_strategy='flw',sleep_time=0)
            except:
                pass
        
        def try_place_moc_buy__ux(ticker_to_work='NHS',share_count=5):
            try:
                self.etrade_ux_driver.place_market_on_close_buy_order(ticker_to_work=ticker_to_work, share_count=share_count)
                time.sleep(1)
                print(f'MOC BUY {share_count} of {ticker_to_work}')
            except:
                time.sleep(1)
                pass
        
        def try_place_loc_buy__ux(ticker_to_work='NHS',share_count=5):
            try:
                self.etrade_ux_driver.place_limit_on_close_buy_order(ticker_to_work=ticker_to_work, share_count=share_count, discount_to_work=.04)
                time.sleep(1)
                print(f'LOC BUY {share_count} of {ticker_to_work}')
            except:
                time.sleep(1)
                print(f'failed on {ticker_to_work}')
                pass
        
        def try_place_loc_short__ux(ticker_to_work='NHS',share_count=5):
            #if ticker_to_work not in self.et_ux_driver.hard_to_borrow_arr:
            try:
                self.etrade_ux_driver.place_limit_on_close_short_order(ticker_to_work=ticker_to_work, share_count=share_count, discount_to_work=.04)
                time.sleep(.2)
                print(f'MOC SHORT {share_count} of {ticker_to_work}')
            except:
                time.sleep(1)
                pass
        buy_above_102=buy_above_102.reset_index()#.set_index('symbol').truncate(after='SPMB')
        buy_below_102=buy_below_102.reset_index()#.set_index('symbol').truncate(after='SPMB')
        sell_short=sell_short.reset_index()#.set_index('symbol').truncate(after='SPMB')
        
        def parallel_execution_moc_buy_and_loc_short(moc_buy_df, moc_short_df):
            with ThreadPoolExecutor() as executor:
                future1 = executor.submit(moc_buy_df.apply, lambda x: try_place_moc_buy_api(x['symbol'], x['orders_to_work']), 1)
                future2 = executor.submit(moc_short_df.apply, lambda x: try_place_loc_short__ux(x['symbol'], x['orders_to_work']), 1)
                return future1.result(), future2.result()#,future3.result(),future4.result()
        buy_below_102.apply(lambda x: try_place_loc_buy__ux(ticker_to_work=x['symbol'], share_count=x['orders_to_work']),1)
        sell_short['notional']=sell_short['quantity']*sell_short['lastPrice']
        sell_short=sell_short.sort_values('notional',ascending=False)
        
        
        parallel_execution_moc_buy_and_loc_short(moc_buy_df=buy_above_102, moc_short_df=sell_short)
        op=self.generate_production_realignment_order_frame_and_outstanding_orders(session_to_realign_to='nt')
        oframe = op['order_frame_to_execute']
        oframe= oframe[oframe['orders_to_work']!=0].copy()
        # last_price_df = self.td_ameritrade_tool.get_price_frame_for_ticker_list(list(set(list(oframe['symbol']))))
        rt_px_frm = self.tiingo_data_tool.output_tiingo_real_time_price_frame()
        tngo_last = rt_px_frm.groupby('ticker').first()['tngoLast']
        oframe['lastPrice']=oframe['symbol'].map(tngo_last )
        # Ensure the lastPrice column in oframe is of type float
        oframe['lastPrice'] = oframe['symbol'].map(tngo_last)
        # Split oframe into the three categories
        buy_above_102 = oframe[(oframe['order_action'] == 'BUY') & (oframe['lastPrice'] > 1.02)].copy()
        buy_below_102 = oframe[(oframe['order_action'] == 'BUY') & (oframe['lastPrice'] < 1.02)].copy()
        sell_short = oframe[oframe['order_action'] == 'SELL_SHORT'].copy()
        self.cancel_all_detritus_orders(session_to_realign_to=session_to_realign_to)
        
        buy_below_102.apply(lambda x: try_place_loc_buy__ux(ticker_to_work=x['symbol'], share_count=x['orders_to_work']),1)
        try_place_loc_short__ux(ticker_to_work='IGSB', share_count=1)
        parallel_execution_moc_buy_and_loc_short(moc_buy_df=buy_above_102, moc_short_df=sell_short)

    
    def output_executed_order_df(self):
        account_map=self.account_map
        session=self.session
        consumer_key=self.consumer_key
        consumer_secret=self.consumer_secret
        order_arr=[]
        account=account_map
        consumer_key=consumer_key
        headers = {"consumerkey": consumer_key}
        params_open = {"status": "EXECUTED",
                    'count':100}
        #            params_executed = {"status": "EXECUTED"}
        #            params_indiv_fills = {"status": "INDIVIDUAL_FILLS"}
        #            params_cancelled = {"status": "CANCELLED"}
        #            params_rejected = {"status": "REJECTED"}
        #            params_expired = {"status": "EXPIRED"}

        base_url ='https://api.etrade.com'
        orders_json=base_url + "/v1/accounts/" + account_map['accountIdKey'] + "/orders.json"
        response_open = session.get(orders_json, header_auth=True, params=params_open, headers=headers)
        all_orders=response_open.json()
        order_arr.append(all_orders)
        iterx=0
        while iterx<15:
            next_string=''
            next_token=''
            try:
                next_string = all_orders['OrdersResponse']['next']
                next_token=next_string.split('marker=')[1].split('&')[0]
            except:
                pass
            if next_token!='':
                account=account_map['accountId']
                consumer_key=consumer_key
                headers = {"consumerkey": consumer_key}
                params_open = {"status": "EXECUTED",
                            'count':100,'marker':next_token}
                #            params_executed = {"status": "EXECUTED"}
                #            params_indiv_fills = {"status": "INDIVIDUAL_FILLS"}
                #            params_cancelled = {"status": "CANCELLED"}
                #            params_rejected = {"status": "REJECTED"}
                #            params_expired = {"status": "EXPIRED"}

                base_url ='https://api.etrade.com'
                orders_json=base_url + "/v1/accounts/" + account_map['accountIdKey'] + "/orders.json"
                response_open = session.get(orders_json, header_auth=True, params=params_open, headers=headers)
                all_orders=response_open.json()
                order_arr.append(all_orders)
            iterx = iterx+1
        full_order_arr=[]
        for orders_to_work in order_arr:
            #orders_to_work=order_arr[0]
            #orders_to_work=order_arr[0]
            single_order_to_work=orders_to_work['OrdersResponse']['Order']
            temp_order_df = pd.DataFrame(single_order_to_work).copy()
            for xfield in ['placedTime', 
                            'orderValue', 
                            'status', 'orderTerm', 'priceType', 
                            'limitPrice', 'stopPrice', 'marketSession', 
                            'allOrNone', 'netPrice', 'netBid', 
                            'netAsk', 'gcd', 'ratio', 'Instrument']:
                temp_order_df[xfield]=temp_order_df['OrderDetail'].apply(lambda x: x[0][xfield])
            instrument_fields = ['symbolDescription', 'orderAction', 'quantityType', 
                                    'orderedQuantity', 'filledQuantity', 
                                    'estimatedCommission', 'estimatedFees', 'Product']
            for xinstr in instrument_fields:
                temp_order_df[xinstr]=temp_order_df['Instrument'].apply(lambda x: x[0][xinstr])
            #    temp_order_df[xinstr]=temp_order_df['Instrument'].apply(lambda x: x[0][xinstr])
            #    temp_order_df['symbol']=temp_order_df['Product'].apply(lambda x: x['symbol'])
            #    temp_order_df['securityType']=temp_order_df['Product'].apply(lambda x: x['securityType'])   
            temp_order_df['symbol']=temp_order_df['Product'].apply(lambda x: x['symbol'])
            temp_order_df['securityType']=temp_order_df['Product'].apply(lambda x: x['securityType'])
            full_order_arr.append(temp_order_df)
        final_output = pd.concat(full_order_arr)
        final_output['execution_price']=final_output['orderValue']/final_output['orderedQuantity']
        final_output['has_execution_price']=final_output['Instrument'].apply(lambda x: x[0]).apply(lambda x: 'averageExecutionPrice' in x.keys())
        final_output['orderAction']=final_output['Instrument'].apply(lambda x: x[0]['orderAction'])
        final_output['averageExecutionPrice']=final_output['Instrument'].apply(lambda x: x[0]['averageExecutionPrice'])
        final_output['filledQuantity']=final_output['Instrument'].apply(lambda x: x[0]['filledQuantity'])
        final_output['executedTime']=final_output['OrderDetail'].apply(lambda x: x[0]['executedTime'])
        final_output['execution_datetime']=final_output['executedTime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
        final_output['execution_date']=final_output['execution_datetime'].apply(lambda x: pd.to_datetime(x.strftime('%Y-%m-%d')))
        final_output['execution_hour']=final_output['execution_datetime'].apply(lambda x: x.hour)
        final_output['execution_minute']=final_output['execution_datetime'].apply(lambda x: x.minute)
        return final_output


    def get_full_balance_json(self):
        # URL for the API endpoint
        url = self.base_url + "/v1/accounts/" + self.account_map["accountIdKey"] + "/balance.json"

        # Add parameters and header information
        params = {"instType": self.account_map["institutionType"], "realTimeNAV": "true"}
        headers = {"consumerkey": self.consumer_key}

        # Make API call for GET request
        response = self.session.get(url, header_auth=True, params=params, headers=headers)
        op_json= response.json()
        return op_json

    #def write_todays_balance_to_database(self):
    def get_total_account_value_df(self,user_name):
        balance_json =self.get_full_balance_json()
        total_account_value = balance_json['BalanceResponse']['Computed']['RealTimeValues']['totalAccountValue']

        balance_df = pd.DataFrame(['balance',
                    total_account_value, datetime.datetime.now(),user_name]).transpose()
        balance_df.columns=['field','value','pull_date','user']
        return balance_df
