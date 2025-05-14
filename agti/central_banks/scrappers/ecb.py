import os
from urllib.parse import quote, urlparse
import logging
import pandas as pd
from selenium import webdriver
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from agti.agti.central_banks.types import ExtensionType, MainMetadata
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, classify_extension, download_and_read_pdf, pageBottom





__all__ = ["ECBBankScrapper"]

logger = logging.getLogger(__name__)

class ECBBankScrapper(BaseBankScraper):
    IGNORED_PATHS = [
        "/press/contacts/",
        "/press/pubbydate/",
        "/home/search/",
        "/pub/research/authors/",
    ]
    SCRIPT_FETCHER = """
const callback = arguments[0];
    (async () => {
        try {
            const databasePrefix = "publications";
            const host = "/foedb/dbs/foedb";
            const field_properties_r = {
                relatedPublications: ["recursive", "json"],
                childrenPublication: ["recursive", "json"],
                documentTypes: ["recursive", "json"],
                publicationProperties: ["recursive", "json"]
            };

            let e = await fetch("/foedb/frontend/release-version.json");
            let n = await e.json();
            const o = n.version;
            let i = "/foedb/frontend/" + o + "/index.js";
            let t = await import(i);
            t = t.default;
            n = `${databasePrefix}.en`;
            i = {
                type: host + ":" + databasePrefix + "_types!id_publication_type"
            };
            i = {
                foedb_host: host,
                database_name: n,
                variable_maps: i,
                on_status_update: e => { /* your update handler */ },
                field_properties: field_properties_r,
                maxLimit: 0,
                custom_sort: (e, i) => {
                    return i?.[o] - e?.[o];
                }
            };
            const resultDB = await t.init(i);
            const total_result = await resultDB.select({ "limit": Infinity });
            callback(total_result);
        } catch (err) {
            callback(err.toString());
        }
    })();
"""
    # cookies implementation, does not work
    """
    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get(self.bank_config.URL)
        wait = WebDriverWait(self.driver_manager.driver, 10)
        xpath = "//div[@id='cookieConsent']//div[@class='consentButtons initial cf']/button[@class='check linkButton linkButtonLarge floatLeft highlight-medium']"
        repeat = 3
        for i in range(repeat):
            try:
                # wait for div
                div_xpath = "//div[@id='cookieConsent']"
                wait.until(EC.presence_of_element_located((By.XPATH, div_xpath)))
                cookie_btn = wait.until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                # click the cookie banner
                cookie_btn.click()
                break
            except Exception as e:
                logger.warning(f"Could not click cookie banner", exc_info=True)
                if i == repeat - 1:
                    raise e
        self.cookies = self.driver_manager.driver.get_cookies()
    """

    def fetch_all_data(self):
        self.get(self.get_serach_url())
        return self.driver_manager.driver.execute_async_script(self.SCRIPT_FETCHER)


    def process_url(self, url: str, timestamp: pd.Timestamp, scraping_time: pd.Timestamp):
        year = timestamp.year
        allowed_outside = False
        urlType, extension = self.clasify_url(url)
        extType = classify_extension(extension)
        main_metadata = MainMetadata(
            url=url,
            date_published=str(timestamp),
            date_published_str=None,
            scraping_time=str(scraping_time),
        )
        if extType == ExtensionType.FILE:
            main_id = self.download_and_upload_file(url, extension, main_metadata, year=str(year))
            if main_id is None:
                return None
            return main_id, []
        elif extType == ExtensionType.WEBPAGE:
            self.get(url)
            main = self.driver_manager.driver.find_element(By.XPATH, "//main")
            main_id = self.process_html_page(main_metadata, year)
            def get_links():
                links_data = []
                for temp_link in main.find_elements(By.XPATH, ".//a"):
                    try:
                        link_href = temp_link.get_attribute("href")
                        if link_href is None:
                            continue
                        parsed_link = urlparse(link_href)
                        link_name = temp_link.get_attribute("textContent").strip()
                    except selenium.common.exceptions.StaleElementReferenceException:
                        continue
                    if any([ignored_path in parsed_link.path for ignored_path in self.IGNORED_PATHS]):
                        continue
                    links_data.append((link_name, link_href))
                return links_data
            links_output = self.process_links(
                main_id,
                get_links,
                year=str(year),
            )
            return main_id, links_output
        else:
            if allowed_outside or urlparse(url).netloc == self.bank_config.NETLOC:
                logger.error(f"Unknown file type: {url}", extra={
                    "url": url,
                    "urlType": urlType,
                    "extension_type": extension
                })
            return None


    def process_all_years(self):

        all_urls = self.get_all_db_urls()
        data = self.fetch_all_data()

        for d in data:
            publication_name = d["type"].get("publication_name", None) if d["type"] is not None else None
            taxonomy_list = d.get("Taxonomy")
            taxonnomies = taxonomy_list.split("|") if taxonomy_list is not None else []
            timestamp = pd.to_datetime(d["pub_timestamp"], unit='s')
            categories = self.get_categories(taxonnomies, publication_name)
            document_types_urls = {
                os.path.splitext(urlparse(self.bank_config.URL + url["id"]).path)[1].lstrip('.'): self.bank_config.URL + url["id"][1:]
                for url in d["documentTypes"]
            }
            if len(document_types_urls) == 0:
                continue
            total_links = []
            if "pdf" in document_types_urls:
                temp_url = document_types_urls["pdf"]
            elif "html" in document_types_urls or "htm" in document_types_urls:
                temp_url = document_types_urls.get("html", None)
                if temp_url is None:
                    temp_url = document_types_urls["htm"]
            else:
                temp_url = sorted(list(document_types_urls.values()))[0]
            if temp_url in all_urls:
                logger.debug(f"Href is already in db: {temp_url}")
                continue
            logger.info(f"Processing {temp_url}")
            scraping_time = pd.Timestamp.now()
            ret = self.process_url(temp_url, timestamp, scraping_time)
            if ret is None:
                continue
            main_id, links_output = ret
            total_links = [
                {
                    "file_url": temp_url,
                    "link_url": link,
                    "link_name": link_text,
                    "file_id": link_id,
                } for (link, link_text, link_id) in links_output
            ]
            result = {
                "file_url": temp_url,
                "date_published": timestamp,
                "scraping_time": scraping_time,
                "file_id": main_id,
            }
            total_categories = [
                {
                    "file_url": temp_url,
                    "category_name": x.value
                }
                for x in categories
            ]
            self.add_all_atomic([result], total_categories, total_links)



                
            
       
    

    def get_serach_url(self) -> str:
        return f"{self.bank_config.URL}/press/pubbydate/html/index.en.html"
    
    def get_categories(self, taxonomies: list[str], publication_name:str) -> set[Categories]:
        result_categories = set()
        found_any_publication_name = False
        found_any_taxonomy = False
        for mapping_tuple in ECB_TAXONOMY_MAPPING:
            if any(tax in taxonomies for tax in mapping_tuple[0]):
                found_any_taxonomy = True
                result_categories.update(mapping_tuple[1])
        for mapping_tuple in ECB_PUBLICATION_NAME_MAPPING:
            if publication_name in  mapping_tuple[0]:
                found_any_publication_name = True
                result_categories.update(mapping_tuple[1])

        if not found_any_publication_name:
            logger.warning(f"Publication name not found in mapping: {publication_name}")
        if len(taxonomies) > 0 and not found_any_taxonomy:
            logger.warning(f"Taxonomies not found in mapping: {taxonomies}")
        return result_categories
    

# Each tuple groups a set of ECB taxonomy strings with our assigned categories.
ECB_TAXONOMY_MAPPING: list[tuple[tuple[str, ...], set[Categories]]] = [
    (
        (
            "Accountability",
            "Banking union",
            "Central bank independence",
            "Capital key",
            "Governance",
            "Legal framework",
            "Rules and procedures",
            "Strategy review",
            "Economic and Monetary Union (EMU)",
            "Diversity and inclusion",
            "Policies"
        ),
        {Categories.INSTITUTIONAL_AND_GOVERNANCE}
    ),
    (
        (
            "Asset purchase programme (APP)",
            "Benchmark rates",
            "Deposit facility rate",
            "Euro overnight index average (EONIA)",
            "Euro short-term rate (\\u20acSTR)",
            "Excess reserves",
            "Forward guidance",
            "Inflation",
            "Interest rates",
            "Key ECB interest rates",
            "Main refinancing operations (MRO) rate",
            "Marginal lending facility rate",
            "Minimum reserve requirements",
            "Monetary policy",
            "Outright Monetary Transactions (OMTs)",
            "Pandemic emergency longer-term refinancing operations (PELTROs)",
            "Pandemic emergency purchase programme (PEPP)",
            "Price stability",
            "Targeted longer-term refinancing operations (TLTROs)",
            "Two-tier system",
            "Central banking"
        ),
        {Categories.MONETARY_POLICY}
    ),
    (
        (
            "Bank failure",
            "Bank profitability",
            "Bank resolution",
            "Banking regulation",
            "Banking supervision",
            "Basel III",
            "Macroprudential policy",
            "Microprudential policy",
            "Non-performing loans",
            "Stress tests",
            "Profits",
            "Financial crisis",
            "Cyber resilience",
            "Risks",
            "Financial stability",
            "Resilience"
        ),
        {Categories.FINANCIAL_STABILITY_AND_REGULATION}
    ),
    (
        (
            "Banking sector",
            "Financial assets",
            "History of the euro",
            "Labour market",
            "Statistics and data",
            "Economic development",
            "Financial markets"
        ),
        {Categories.RESEARCH_AND_DATA}
    ),
    (
        (
            "Payment systems",
            "TARGET Instant Payment Settlement (TIPS)",
            "TARGET2",
            "Instant payments",
            "Financial market infrastructures"
        ),
        {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS}
    ),
    (
        (
            "Banknotes and coins",
            "Currencies",
            "Euro",
            "Money",
            "Securities",
            "Bitcoin",
            "Crypto-assets"
        ),
        {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS}
    ),
    (
        (
            "Brexit",
            "Digitalisation",
            "Distributed ledger technology (DLT)",
            "Capital markets union",
            "Climate change",
            "European integration",
            "Financial integration",
            "Fintech",
            "Fiscal policy",
            "Innovation",
            "International relations",
            "Russian war against Ukraine",
            "Sanctions",
            "Structural reforms",
            "Technology",
            "Trade",
            "Protectionism",
            "Uncertainties",
            "null",
            "Coronavirus"
        ),
        {Categories.OTHER}
    ),
    (
        (
            "Collateral",
            "Haircuts",
            "Liquidity",
            "Repo lines",
            "Swap lines",
            "Transmission Protection Instrument (TPI)",
            "Liquidity lines"
        ),
        {Categories.MONETARY_POLICY, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS}
    ),
    (
        (
            "Exchange rates",
            "Euro area"
        ),
        {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA}
    ),
    (
        (
            "Central bank digital currencies (CBDC)",
            "Digital euro"
        ),
        {Categories.MONETARY_POLICY, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS}
    ),
    (
        ("Communication",),
        {Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.NEWS_AND_EVENTS}
    ),
    (
        ("Central counterparties (CCPs)",),
        {Categories.FINANCIAL_STABILITY_AND_REGULATION, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS}
    ),
    (
        ("International role of the euro",),
        {Categories.RESEARCH_AND_DATA, Categories.INSTITUTIONAL_AND_GOVERNANCE}
    ),
    (
        ("Emergency liquidity assistance (ELA)",),
        {Categories.MONETARY_POLICY, Categories.FINANCIAL_STABILITY_AND_REGULATION}
    )
]


# Each tuple groups a set of ECB publication_name strings with our assigned categories.
ECB_PUBLICATION_NAME_MAPPING: list[tuple[tuple[str, ...], set[Categories]]] = [
    (
        (
            "Card fraud report",
            "Climate-related financial disclosures",
            "Consultation response",
            "ECB Other publication",
            "ECB public consultation - statistics"
        ),
        {Categories.OTHER}
    ),
    (
        (
            "Digital Euro Investigation Phase - Scheme Rulebook Development Group documents",
            "Digital Euro Preparation Phase - Scheme Rulebook Development Group documents",
            "ECB Digital Euro Governance",
            "ECB Digital Euro Investigation Phase - Progress Report",
            "ECB Digital Euro Investigation Phase document",
            "ECB Digital Euro Preparation Phase - Progress Report",
            "ECB Digital Euro Preparation Phase document"
        ),
        {Categories.MONETARY_POLICY, Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS}
    ),
    (
        (
            "Combined monetary policy decisions and statement",
            "ECB Monetary developments in the euro area",
            "ECB Monetary policy account",
            "ECB Monetary policy decision",
            "ECB Monetary policy statement",
            "ECB Governing Council decisions - Other decisions",
            "ECB Governing Council statement",
            "ECB Survey of Monetary Analysts",
            "ECB Survey of Monetary Analysts - Aggregate results",
            "ECB Survey of Professional Forecasters",
            "€STR Annual Methodology Review",
            "€STR Transparency on errors",
            "ECB Economic Bulletin",
            "ECB Economic Bulletin - Article",
            "ECB Economic Bulletin - Box"
        ),
        {Categories.MONETARY_POLICY}
    ),
    (
        (
            "ECB Annual Accounts",
            "ECB Annual Report",
            "ECB Annual Report - Statistical annex",
            "ECB Annual consolidated balance sheet of the Eurosystem",
            "ECB Disaggregated financial statement",
            "ECB Environmental Statement",
            "ECB Legal Working Paper Series",
            "ECB Legal act",
            "ECB Letters to MEPs",
            "ECB Strategy review",
            "Eurosystem oversight report",
            "Feedback on the input provided by the European Parliament as part of its resolution on the ECB’s Annual Report",
            "Integrated Reporting Framework document",
            "Legal conference proceedings",
            "ECB Weekly financial statement",
            "ECB Weekly financial statement - Commentary"
        ),
        {Categories.INSTITUTIONAL_AND_GOVERNANCE}
    ),
    (
        (
            "EBA/ECB report",
            "ECB Balance of payments (monthly)",
            "ECB Balance of payments (quarterly)",
            "ECB Consumer Expectation Survey",
            "ECB Convergence Report",
            "ECB Discussion Paper Series",
            "ECB Macroeconomic projections for the euro area",
            "ECB Occasional Paper Series",
            "ECB Research Bulletin",
            "ECB Statistics Paper Series",
            "ECB Working Paper Series",
            "Euro area balance of payments and international investment position statistics - Quality report",
            "Euro area bank lending survey",
            "Euro area bank lending survey - Glossary",
            "Euro area bank lending survey - Questionnaire",
            "Euro area monetary and financial statistics - Quality report",
            "Euro area pension fund statistics",
            "Euro area quarterly financial accounts - Quality report",
            "Financial integration and structure article",
            "Financial integration and structure box",
            "Financial integration and structure in the euro area",
            "Survey on credit terms and conditions in euro-denominated securities financing and OTC derivatives markets",
            "Survey on the Access to Finance of Enterprises in the euro area",
            "The international role of the euro",
            "The international role of the euro - Box",
            "The international role of the euro - Special feature",
            "ECB Euro area economic and financial developments by institutional sector (early)",
            "ECB Euro area economic and financial developments by institutional sector (full)",
            "ECB Euro area financial vehicle corporation statistics",
            "ECB Euro area insurance corporation and pension fund statistics",
            "ECB Euro area insurance corporations statistics",
            "ECB Euro area investment fund statistics",
            "ECB Euro area securities issues statistics",
        
        ),
        {Categories.RESEARCH_AND_DATA}
    ),
    (
        (
            "ECB Forum on Central Banking - Conference proceedings",
        ),
        {Categories.RESEARCH_AND_DATA, Categories.NEWS_AND_EVENTS}
    ),
    (
        (
            "ECB Euro money market",
            "ECB Euro money market statistics",
            "ECB Payment instruments and systems",
            "T2S Annual Report",
            "T2S Harmonisation progress report",
            "T2S financial statement",
            "TARGET Annual Report"
        ),
        {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS}
    ),
    (
        ("ECB MFI interest rate statistics",),
        {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS, Categories.RESEARCH_AND_DATA}
    ),
    (
        (
            "ECB Financial Stability Review",
            "ECB Financial Stability Review - Article",
            "ECB Financial Stability Review - Box",
            "ECB Macroprudential Bulletin",
            "ECB Macroprudential Bulletin - Annex",
            "ECB Macroprudential Bulletin - Article",
            "ECB Macroprudential Bulletin - Focus",
            "ECB Macroprudential Bulletin - Foreword"
        ),
        {Categories.FINANCIAL_STABILITY_AND_REGULATION}
    ),
    (
        (
            "ECB Interview",
            "ECB Podcast",
            "ECB Press release",
            "ECB Speech",
            "The ECB Blog"
        ),
        {Categories.NEWS_AND_EVENTS}
    ),
    (
        ("Use of cash by companies in the euro area",),
        {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS}
    )

]