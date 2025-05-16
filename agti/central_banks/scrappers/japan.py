import pandas as pd
import selenium
from selenium.webdriver.common.by import By
import logging
from agti.agti.central_banks.types import ExtensionType, MainMetadata
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import classify_extension
from ..utils import Categories
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

__all__ = ["JapanBankScrapper"]



# NOTE! before running read_html check for any pdf links and download them
# there can be also some zips or any other files, but we are not going to handle them
class JapanBankScrapper(BaseBankScraper):
    IGNORED_PATHS = [
        "/help.htm",
        "/about/abouthp.htm",
        "/mailing/index.htm",
        "/about/services/contact.htm",
    ]


    ##########################
    # Monetery Policy processing
    ##########################

    def process_monetery_policy(self):
        logger.info("Processing Monetary Policy")
        # NOTE we can ignore Outline of Monetary Policy
        # "https://www.boj.or.jp/en/mopo/outline/index.htm"

        all_urls = self.get_all_db_urls()

        # Monetary Policy Meeting
        
        ##########################
        ## Summary of Opinions
        ##########################
        logger.info("Processing MP meeting summary of opinions")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/mopo/mpmsche_minu/opinion_{}/index.htm", 
                2016,
            ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        ##########################
        ## Minutes
        ##########################
        logger.info("Processing MP meeting minutes")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/mopo/mpmsche_minu/minu_{}/index.htm",
                1998,
            ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        ##########################
        ## Others
        ##########################
        logger.info("Processing MP meeting others")
        self.get("https://www.boj.or.jp/en/mopo/mpmsche_minu/m_ref/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        
        # Monetary Policy Releases
        logger.info("Processing monetary policy releases")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/mopo/mpmdeci/mpr_{}/index.htm", 
                1998,
            ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value])

        # process_monetery_policy_measures
        # NOTE: we can ignore
        # based on our check everything is in the releases
        # https://www.boj.or.jp/en/mopo/measures/index.htms

        # Outlook for Economic Activity and Prices
        all_urls = self.get_all_db_urls()
        # we can ignore boxes, because they are part of outlooks
        # we can ignore higlihts, because they are part of outlooks

        ##########################
        # Outlook for Economic Activity and Prices
        ##########################
        # they are 2 tables, we let the first one to be parsed by process_href_table
        # and the second one we parse it here
        logger.info("Processing outlook for economic activity and prices")
        self.get("https://www.boj.or.jp/en/mopo/outlook/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])

        # find the second table
        tables = self.driver_manager.driver.find_elements(By.XPATH, "//table[@class='js-tbl' or @class='STDtable TAB_top']")
        if len(tables) != 2:
            raise ValueError("Expected 2 tables in outlook Japan bank")
        
        to_process = []
        table = tables[1]
        tbody = table.find_element(By.XPATH, ".//tbody")
        table_rows = tbody.find_elements(By.XPATH,".//tr")
        for row in table_rows:
            tds = list(row.find_elements(By.XPATH,".//td"))
            date = pd.to_datetime(tds[0].text)
            # try tds[2], if text is empty, try tds[1]
            if tds[2].text == "":
                link = tds[1].find_element(By.XPATH, ".//a")
            else:
                link = tds[2].find_element(By.XPATH, ".//a")
            href = link.get_attribute("href")
            if href in all_urls:
                logger.debug(f"Href is already in db: {href}")
                continue

            to_process.append((date, href))
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Monthly Report of Recent Economic and Financial Developments
        ##########################
        logger.info("Processing monthly report of recent economic and financial developments")
        for to_process in self.find_hrefs_tab_table_iter(
            "https://www.boj.or.jp/en/mopo/gp_{}/index.htm",
            1998,
        ):
            self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])
        
        
        ##########################
        # Semiannual Report on Currency and Monetary Control
        ##########################
        logger.info("Processing semiannual report on currency and monetary control")
        self.get("https://www.boj.or.jp/en/mopo/diet/d_report/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Statement concerning the Report to the Diet
        ##########################
        logger.info("Processing statement concerning the report to the diet")
        self.get("https://www.boj.or.jp/en/mopo/diet/d_state/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.NEWS_AND_EVENTS.value])

        # Research Papers, Reports, Speeches and Statements Related to Monetary Policy
        

        ##########################
        ## Statements
        ##########################
        logger.info("Processing MP statements")
        self.get("https://www.boj.or.jp/en/mopo/r_menu_dan/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.NEWS_AND_EVENTS.value])

        ##########################
        ## Reserach Papers
        ##########################
        logger.info("Processing MP research papers")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/mopo/r_menu_ken/index.htm?mylist=")

        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value])
        ##########################
        ## Speeches
        ##########################
        logger.info("Processing MP speeches")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/mopo/r_menu_kou/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MONETARY_POLICY.value, Categories.RESEARCH_AND_DATA.value, Categories.NEWS_AND_EVENTS.value])




    ##########################
    # Financial system processing
    ##########################

   
    
    def process_financial_system_reports(self):
        logger.info("Processing Financial System Reports")
        all_urls = self.get_all_db_urls()
        # on -site
        # https://www.boj.or.jp/en/finsys/exam_monit/exampolicy/index.htm
        logger.info("Processing financial system reports")
        self.get("https://www.boj.or.jp/en/finsys/exam_monit/exampolicy/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value])

        # financial reports (https://www.boj.or.jp/en/finsys/fsr/index.htm)
        # TODO: it is better to fetch it from: (Home>Research and Studies>BOJ Reports & Research Papers>Financial System Report)
        # https://www.boj.or.jp/en/research/brp/fsr/index.htm#p02
        # old markets reports
        logger.info("Processing financial old markets reports")
        self.get("https://www.boj.or.jp/en/research/brp/fmr/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])
        # new system reports and Annex series
        logger.info("Processing financial new system reports and annex series")
        self.get("https://www.boj.or.jp/en/research/brp/fsr/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=2)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        # policy
        # https://www.boj.or.jp/en/finsys/fs_policy/index.htm (table)
        # https://www.boj.or.jp/en/finsys/msfs/index.htm
        # https://www.boj.or.jp/en/finsys/spp/index.htm
        # https://www.boj.or.jp/en/finsys/rfs/index.htm
        logger.info("Processing financial policy")
        self.get("https://www.boj.or.jp/en/finsys/fs_policy/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        self.get("https://www.boj.or.jp/en/finsys/msfs/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value])

        self.get("https://www.boj.or.jp/en/finsys/spp/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value,Categories.INSTITUTIONAL_AND_GOVERNANCE.value])

        self.get("https://www.boj.or.jp/en/finsys/rfs/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        # coorination
        # https://www.boj.or.jp/en/finsys/macpru/index.htm
        # https://www.boj.or.jp/en/finsys/cofsa/index.htm (all tables)
        logger.info("Processing financial coordination")
        self.get("https://www.boj.or.jp/en/finsys/macpru/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.NEWS_AND_EVENTS.value])

        self.get("https://www.boj.or.jp/en/finsys/cofsa/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=3)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])

        # seminars Not important ommited
        # https://www.boj.or.jp/en/finsys/c_aft/index.htm (maybe not importnant)
        

        # research papers
        logger.info("Processing financial system research papers")
        to_process = self.find_hrefs_mylist_table(
            "https://www.boj.or.jp/en/finsys/r_menu_ron/index.htm?mylist=",
        )
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value])
        # speeches
        logger.info("Processing financial system speeches")
        to_process = self.find_hrefs_mylist_table(
            "https://www.boj.or.jp/en/finsys/r_menu_koen/index.htm?mylist="
        )
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.RESEARCH_AND_DATA.value, Categories.NEWS_AND_EVENTS.value])
        
        # statements
        logger.info("Processing financial system statements")
        self.get("https://www.boj.or.jp/en/finsys/r_menu_dan/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.FINANCIAL_STABILITY_AND_REGULATION.value, Categories.NEWS_AND_EVENTS.value])
        

    ##########################
    # Payments and Markets
    ########################## 
    def process_payment_and_settlement_systems(self):
        logger.info("Processing Payments and Markets")
        
        ##########################
        # Outline of Payment and Settlement Systems
        all_urls = self.get_all_db_urls()

        # Payment and Settlement Systems and the Bank
        logger.info("Processing payment and settlement systems")
        self.get("https://www.boj.or.jp/en/paym/outline/pay_boj/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        # oversight
        logger.info("Processing payment and settlement systems oversight")
        self.get("https://www.boj.or.jp/en/paym/outline/pay_os/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.FINANCIAL_STABILITY_AND_REGULATION.value])

        # forums
        logger.info("Processing payment and settlement systems forums")
        self.get("https://www.boj.or.jp/en/paym/outline/pay_forum/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value])

        # Payment and Settlement Systems Operated by the Private Sector
        logger.info("Processing payment and settlement systems operated by the private sector")
        self.get("https://www.boj.or.jp/en/paym/outline/pay_ps/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        ##########################
        # Operation of BOJ-NET

        # The Next-Generation RTGS Project
        logger.info("Processing next-generation RTGS project")
        self.get("https://www.boj.or.jp/en/paym/bojnet/next_rtgs/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        # RTGS (Real-Time Gross Settlement)
        logger.info("Processing RTGS")
        self.get("https://www.boj.or.jp/en/paym/bojnet/rtgs/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        # New BOJ-NET
        logger.info("Processing new BOJ-NET")
        self.get("https://www.boj.or.jp/en/paym/bojnet/new_net/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        # Forum Towards Making Effective Use of the BOJ-NET
        logger.info("Processing forum towards making effective use of the BOJ-NET")
        self.get("https://www.boj.or.jp/en/paym/bojnet/net_forum/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value])

        # Cross-border DVP Link
        logger.info("Processing cross-border DVP link")
        self.get("https://www.boj.or.jp/en/paym/bojnet/crossborder/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        # Others
        logger.info("Processing Payments and Markets others")
        self.get("https://www.boj.or.jp/en/paym/bojnet/other/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        ##########################
        # JGB Book-Entry System
        logger.info("Processing JGB book-entry system")
        self.get("https://www.boj.or.jp/en/paym/jgb_bes/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        ##########################
        # FinTech Center
        logger.info("Processing fintech center")
        self.get("https://www.boj.or.jp/en/paym/fintech/index.htm")
        to_process = self.process_href_table(all_urls,num_tables=2)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        ##########################
        # Central Bank Digital Currency
        logger.info("Processing central bank digital currency")
        self.get("https://www.boj.or.jp/en/paym/digital/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=4)
        # parse separately the last table
        table = self.driver_manager.driver.find_elements(By.XPATH, "//table[@class='js-tbl' or @class='STDtable TAB_top']")[-1]
        tbody = table.find_element(By.XPATH, ".//tbody")
        table_rows = tbody.find_elements(By.XPATH,".//tr")
        for row in table_rows:
            tds = list(row.find_elements(By.XPATH,".//td"))
            date = pd.to_datetime(tds[0].text)
            link = tds[2].find_element(By.XPATH, ".//a")
            href = link.get_attribute("href")
            if href in all_urls:
                logger.debug(f"Href is already in db: {href}")
                continue

            to_process.append((date, href))
        self.extract_data_update_tables(to_process, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value])

        # CBDC Forum
        logger.info("Processing CBDC forum")
        self.get("https://www.boj.or.jp/en/paym/digital/d_forum/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value, Categories.NEWS_AND_EVENTS.value]) 

        ##########################
        # Money Market
        logger.info("Processing Payments and Markets money market")
        self.get("https://www.boj.or.jp/en/paym/market/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=4)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])

        # ignore Cross-Industry Forum on Interest Rate Benchmarks
        # ignore Repo Market Forum

        # Cross-Industry Committee on Japanese Yen Interest Rate Benchmarks
        logger.info("Processing cross-industry committee on Japanese yen interest rate benchmarks")
        self.get("https://www.boj.or.jp/en/paym/market/jpy_cmte/index.htm")
        # we could theoretically ignore the first table
        to_process = self.process_href_table(all_urls, num_tables=3)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value])


        ##########################
        # Bond Market
        logger.info("Processing bond market")
        self.get("https://www.boj.or.jp/en/paym/bond/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=3)
        self.extract_data_update_tables(to_process, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value])

        # Bond Market Survey
        logger.info("Processing bond market survey")
        self.get("https://www.boj.or.jp/en/paym/bond/bond_list/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value, Categories.RESEARCH_AND_DATA.value])

        # Bond Market Group
        logger.info("Processing bond market group")
        self.get("https://www.boj.or.jp/en/paym/bond/mbond_list/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value, Categories.NEWS_AND_EVENTS.value])

        ##########################
        # ignore: Credit Market
        logger.info("Processing credit market")
        ##########################
        # Forums and Conferences
        logger.info("Processing Payments and Markets forums and conferences")
        self.get("https://www.boj.or.jp/en/paym/forum/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value])


        ##########################
        # Research Papers, Reports, Speeches and Statements Related to Payment and Markets
        logger.info("Processing Payments and Markets research papers, reports, speeches and statements")
        # Payment and Settlement Systems Report
        logger.info("Processing payment and settlement systems report")
        self.get("https://www.boj.or.jp/en/research/brp/psr/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=2)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.RESEARCH_AND_DATA.value])

        # Market Operations in Each Fiscal Year
        logger.info("Processing market operations in each fiscal year")
        self.get("https://www.boj.or.jp/en/research/brp/mor/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.RESEARCH_AND_DATA.value])

        # Market Functioning Survey concerning Climate Change
        logger.info("Processing market functioning survey concerning climate change")
        self.get("https://www.boj.or.jp/en/paym/m-climate/index.htm")
        to_process = self.process_href_table(all_urls, num_tables=3)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.RESEARCH_AND_DATA.value])

        # List of Research Papers Related to Payment and Markets
        logger.info("Processing list of research papers related to payment and markets")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/paym/r_menu_ron/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.RESEARCH_AND_DATA.value])

        # List of Speeches Related to Payment and Markets
        logger.info("Processing list of speeches related to payment and markets")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/paym/r_menu_koen/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value, Categories.NEWS_AND_EVENTS.value])

        # List of Statements Related to Payment and Markets
        logger.info("Processing list of statements related to payment and markets")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/paym/r_menu_dan/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value])




        ##########################
        # Other Releases Related to Payment and Markets
        logger.info("Processing other releases related to payment and markets")
        self.get("https://www.boj.or.jp/en/paym/release/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value])





    ##########################
    # Banknotes, The Bank's Treasury Funds and JGS Services
    ##########################
    # NOTE: we can ignore this section, because it is not important for our analysis


    ##########################
    # International Finance
    ##########################
    def process_international_finance(self):
        logger.info("Processing International Finance")
        # Outline of International Finance
        # ignore
        all_urls = self.get_all_db_urls()

        # International Meetings
        logger.info("Processing international meetings")
        self.get("https://www.boj.or.jp/en/intl_finance/meeting/index.htm") 
        to_process = self.process_href_table(all_urls, num_tables=2)
        self.extract_data_update_tables(to_process, [Categories.NEWS_AND_EVENTS.value])

        # Foreign Currency Assets
        logger.info("Processing foreign currency assets")
        self.get("https://www.boj.or.jp/en/intl_finance/ex_assets/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS.value])

        # Cooperation with Other Central Banks
        logger.info("Processing cooperation with other central banks")
        self.get("https://www.boj.or.jp/en/intl_finance/cooperate/index.htm")
        to_process = self.process_href_table(all_urls, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.INSTITUTIONAL_AND_GOVERNANCE.value, Categories.NEWS_AND_EVENTS.value])

        # List of Research Papers Related to International Finance
        logger.info("Processing list of research papers related to international finance")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/intl_finance/r_menu_ron/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.RESEARCH_AND_DATA.value])

        # List of Speeches Related to International Finance
        logger.info("Processing list of speeches related to international finance")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/intl_finance/r_menu_koen/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.RESEARCH_AND_DATA.value, Categories.NEWS_AND_EVENTS.value])

        # List of Statements Related to International Finance
        logger.info("Processing list of statements related to international finance")
        to_process = self.find_hrefs_mylist_table("https://www.boj.or.jp/en/intl_finance/r_menu_dan/index.htm?mylist=")
        self.extract_data_update_tables(to_process, [Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS.value, Categories.NEWS_AND_EVENTS.value])


        # Other Releases Related to International Finance
        logger.info("Processing other releases related to international finance")
        self.get("https://www.boj.or.jp/en/intl_finance/release/index.htm")
        to_process = self.process_href_table(all_urls)
        self.extract_data_update_tables(to_process, [Categories.NEWS_AND_EVENTS.value])

    ##########################
    # Research and Studies
    ##########################
    def process_research_and_studies(self):
        logger.info("Processing Research and Studies")
        all_urls = self.get_all_db_urls()
        # Outline of Research and Studies
        # ignore
        
        # List of Reports & Research Papers
        logger.info("Processing list of reports and research papers")
        for to_process in self.find_hrefs_tab_table_iter(
            "https://www.boj.or.jp/en/research/rs_all_{}/index.htm",
            1996,
            num_tables="ALL",
        ):
            self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        ##########################
        # BOJ Reports & Research Papers

        # Financial System Report (https://www.boj.or.jp/en/research/brp/fsr/index.htm)
        # NOTE: we already do this under process_financial_system_reports

        # Market Operations in Each Fiscal Year (https://www.boj.or.jp/en/research/brp/mor/index.htm)
        # NOTE: we already do this under process_payment_and_settlement_systems

        # Payment and Settlement Systems Report (https://www.boj.or.jp/en/research/brp/psr/index.htm)
        # NOTE: we already do this under process_payment_and_settlement_systems

        # Regional Economic Report
        logger.info("Processing regional economic report")
        self.get("https://www.boj.or.jp/en/research/brp/rer/index.htm")
        to_process = self.process_href_table(all_urls, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        # Research Papers
        logger.info("Processing research papers")
        for to_process in self.find_hrefs_tab_table_iter(
            "https://www.boj.or.jp/en/research/brp/ron_{}/index.htm",
            1996,
            num_tables="ALL"
        ):
            self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Bank of Japan Working Paper Series, Review Series, and Research Laboratory Series
        logger.info("Processing bank of japan working paper series, review series and research laboratory series")
        self.get("https://www.boj.or.jp/en/research/wps_rev/index.htm")
        to_process = self.process_href_table(all_urls, date_col=1, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])


        ##########################
        # Research Data
        # ignore

        ##########################
        # Research Papers Released by IMES
        current_year = pd.Timestamp.now().year
        # Monetary and Economic Studies
        logger.info("Processing monetary and economic studies")
        for year in range(1997,current_year + 1):
            short_year = str(year)[-2:]
            self.get(f"https://www.boj.or.jp/en/research/imes/mes/mes{short_year}.htm")
            to_process = []
            tables = self.driver_manager.driver.find_elements(By.XPATH, "//table")
            for table in tables:
                table_rows = list(table.find_elements(By.XPATH,".//tr"))
                if len(table_rows) <=1:
                    break
                for row in table_rows[1:]:
                    tds = list(row.find_elements(By.XPATH,".//td"))
                    a = tds[-1].find_element(By.XPATH, ".//a")
                    href = a.get_attribute("href")
                    if href in all_urls:
                        logger.debug(f"Href is already in db: {href}")
                        continue
                    to_process.append((None, href))

            self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        # IMES Discussion Paper E-Series
        logger.info("Processing IMES discussion paper E-series")
        for year in range(1997,current_year + 1):
            short_year = str(year)[-2:]
            self.get(f"https://www.boj.or.jp/en/research/imes/dps/dps{short_year}.htm")
            to_process = []
            tables = self.driver_manager.driver.find_elements(By.XPATH, "//table")
            for table in tables:
                table_rows = list(table.find_elements(By.XPATH,".//tr"))
                if len(table_rows) <=1:
                    break
                for row in table_rows[1:]:
                    tds = list(row.find_elements(By.XPATH,".//td"))
                    a = tds[-1].find_element(By.XPATH, ".//a")
                    href = a.get_attribute("href")
                    date = pd.to_datetime(tds[-2].text.split('\n')[0].replace(",",", "))
                    if href in all_urls:
                        logger.debug(f"Href is already in db: {href}")
                        continue
                    to_process.append((date, href))

            self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        # Conferences
        # ignore 

        ##########################
        # Study Group Reports
        logger.info("Processing study group reports")
        self.get("https://www.boj.or.jp/en/research/other_release/index.htm")
        to_process = self.process_href_table(all_urls, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Opinion Survey + Conferences
        # ignore

        ##########################
        # Alternative Data Analysis
        logger.info("Processing alternative data analysis")
        self.get("https://www.boj.or.jp/en/research/bigdata/index.htm")
        to_process = self.process_href_table(all_urls, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        ##########################
        # Discontinued Research Releases
        # ignore


    ##########################
    # Statistics
    ##########################
    def process_statistics(self):
        logger.info("Processing Statistics")
        all_urls = self.get_all_db_urls()


        ##########################
        # Related to BIS/FSB

        # Central Bank Survey of Foreign Exchange and Derivatives Market Activity
        logger.info("Processing central bank survey of foreign exchange and derivatives market activity")
        self.get("https://www.boj.or.jp/en/statistics/bis/deri/index.htm")
        to_process = self.process_href_table(all_urls, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        # Statistics on Securities Financing Transactions in Japan
        logger.info("Processing statistics on securities financing transactions in Japan")
        self.get("https://www.boj.or.jp/en/statistics/bis/repo_release/index.htm")
        to_process = self.process_href_table(all_urls, num_tables="ALL")
        self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        
        ##########################
        # Payment and Settlement

        # Payment and Settlement Statistics
        logger.info("Processing payment and settlement statistics")
        for to_process in self.find_hrefs_tab_table_iter(
                "https://www.boj.or.jp/en/statistics/set/kess/release/{}/index.htm",
                2021,
            ):
            self.extract_data_update_tables(to_process, [Categories.RESEARCH_AND_DATA.value])

        # We do not fetch the others, but if required we can do it
        



    ##########################
    # Helper function
    ########################## 
    def parse_html(self, url: str, date):
        self.get(url)
        year = str(date.year)
        if "www.imes.boj.or.jp" in urlparse(url):
            # find the iframe and switch to it
            try:
                iframe = self.driver_manager.driver.find_element(By.TAG_NAME, "iframe")
                self.driver_manager.driver.switch_to.frame(iframe)
            except selenium.common.exceptions.NoSuchElementException:
                logger.warning(f"No iframe found in {url}")
        try:
            main = self.driver_manager.driver.find_element(By.XPATH, "//*[@id='content' or @id='contents' or @id='app' or @id='container'] | //main")
        except selenium.common.exceptions.NoSuchElementException:
            logger.warning(f"No content found in {url}")
            return None, []
        scraping_time = pd.Timestamp.now()
        main_metadata = MainMetadata(
            url=url,
            date_published=str(date),
            scraping_time=str(scraping_time),
        )
        file_id = self.process_html_page(main_metadata, year)
        result = {
            "file_url": url,
            "date_published": date,
            "scraping_time": scraping_time,
            "file_id": file_id,
        }
        def f_get_links():
            links = []
            for link in main.find_elements(By.XPATH, ".//a"):
                link_text = link.get_attribute("textContent").strip()
                link_url = link.get_attribute("href")
                if link_url is None:
                    continue
                parsed_link = urlparse(link_url)
                if any(ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS):
                                continue
                links.append((link_text, link_url))
            return links
        processed_links = self.process_links(file_id, f_get_links, year=year)
        return result, processed_links
    
    def extract_data_update_tables(self, to_process, tags):
        for date, href in to_process:
            logger.info(f"Processing: {href}")
            urlType, extension = self.clasify_url(href)
            total_links = []
            allowed_outside = False
            extType = classify_extension(extension)
            scraping_time = pd.Timestamp.now()
            main_metadata = MainMetadata(
                url=href,
                date_published=str(date),
                scraping_time=str(scraping_time),
            )
            if extType == ExtensionType.FILE:
                main_id = self.download_and_upload_file(href, extension, main_metadata, year=str(date.year))
                if main_id is None:
                    continue
                result = {
                    "file_url": href,
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_id": main_id,
                }
            elif extType == ExtensionType.WEBPAGE:
                result, links_output = self.parse_html(href, date)
                if result is None:
                    continue
                total_links = [
                    {
                        "file_url": href,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
            else:
                if allowed_outside or urlparse(href).netloc == self.bank_config.NETLOC:
                    logger.error(f"Unknown file type: {href}", extra={
                        "url": href,
                        "urlType": urlType,
                        "extension_type": extension
                    })
                continue
            total_categories = [{
                "file_url": href,
                "category_name": tag,
                } for tag in tags
            ]
            self.add_all_atomic([result], total_categories, total_links)

    def find_hrefs_tab_table_iter(self,
        f_url,
        init_year,
        link_col=-1,
        date_col=0,
        date_format=None,
        num_tables=1,
        
        ):
        all_urls = self.get_all_db_urls()
        this_year = pd.Timestamp.now().year
        for year in range(init_year, this_year + 1):
            logger.info(f"Processing year: {year}")
            self.get(f_url.format(year))
            to_process = self.process_href_table(all_urls, link_col=link_col, date_col=date_col, date_format=date_format,num_tables=num_tables)
            yield to_process

    def find_hrefs_mylist_table(
            self, 
            url, 
            link_col=-1,
            date_col=0,
            date_format=None,
            num_tables=1
            ):
        all_urls = self.get_all_db_urls()
        i = 0
        to_process = []
        while True:
            logger.info(f"Processing mylist: {i*50 +1}")
            self.get(f"{url}{i*50 +1}")
            temp = self.process_href_table(all_urls, link_col=link_col, date_col=date_col, date_format=date_format,num_tables=num_tables)
            if len(temp) == 0:
                break
            to_process.extend(temp)
            i += 1
        return to_process
    

    def process_href_table(
            self,
            all_urls,
            link_col=-1,
            date_col=0,
            date_format=None,
            num_tables=1):
        to_process = []
        # while it can find
        tables = self.driver_manager.driver.find_elements(By.XPATH, "//table[@class='js-tbl' or @class='STDtable TAB_top']")
        if len(tables) == 0:
            logger.warning(f"No tables found for {self.driver_manager.driver.current_url}")
            return to_process
        if num_tables == "ALL":
            num_tables = len(tables)
        for table in tables[:num_tables]:
            #caption = table.find_element(By.XPATH, ".//caption").text
            #tbody = table.find_element(By.XPATH, ".//tbody")
            table_rows = list(table.find_elements(By.XPATH,".//tr"))
            if len(table_rows) <=1:
                return to_process
            for row in table_rows[1:]:
                tds = list(row.find_elements(By.XPATH,".//td"))
                date_text = tds[date_col].text
                # get pos of ',' and replace it with ', '
                date_text = date_text.replace(",", ", ")
                date = pd.to_datetime(date_text, format=date_format)
                try:
                    link = tds[link_col].find_element(By.XPATH, ".//a")
                except selenium.common.exceptions.NoSuchElementException:
                    logger.warning(f"No link found in url: {self.driver_manager.driver.current_url} and row {row.text}", extra={"url": self.driver_manager.driver.current_url})
                    continue
                href = link.get_attribute("href")
                if href in all_urls:
                    logger.debug(f"Href is already in db: {href}")
                    continue

                to_process.append((date, href))
        return to_process


    
    def process_all_years(self):
        self.process_statistics()
        self.process_research_and_studies()
        self.process_international_finance()
        self.process_payment_and_settlement_systems()
        self.process_financial_system_reports()
        self.process_monetery_policy()
    
