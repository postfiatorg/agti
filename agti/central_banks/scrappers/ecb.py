import os
from urllib.parse import urlparse
import logging
import pandas as pd
from selenium import webdriver
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories, download_and_read_pdf, pageBottom





__all__ = ["ECBBankScrapper"]

logger = logging.getLogger(__name__)

class ECBBankScrapper(BaseBankScraper):
    COUNTRY_CODE_ALPHA_3 = "EUE"
    COUNTRY_NAME = "European Union"

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

    def fetch_all_data(self):
        self._driver.get(self.get_serach_url())
        return self._driver.execute_async_script(self.SCRIPT_FETCHER)


    def parse_html(self, url: str):
        url_parsed = urlparse(url)
        self._driver.get(url)
        current_url_parsed = urlparse(self._driver.current_url)
        # check if it is pdf
        if current_url_parsed.path.endswith("pdf"):
            return download_and_read_pdf(url, self.datadump_directory_path), []
        # select all text from dev with class section
        main = self._driver.find_element(By.XPATH, "//main")
        text = main.text
        # find all links and process them
        links = main.find_elements(By.XPATH, ".//a")
        links_data = []
        total_links = []
        for temp_link in links:
            try:
                link_href = temp_link.get_attribute("href")
                link_name = temp_link.text
                links_data.append((link_href, link_name))
            except selenium.common.exceptions.StaleElementReferenceException:
                continue

        if len(links_data) != len(links):
            logger.warning(f"Links length mismatch: Found {len(links)} vs obtained {len(links_data)}")
        
        for link_href, link_name in links_data:
            if link_href is None:
                continue
            link_href_parsed = urlparse(link_href)
            link_text = None
            if link_href_parsed.fragment != '':
                if url_parsed[:3] == link_href_parsed[:3]:
                    # we ignore links to the same page (fragment identifier)
                    continue
                # NOTE: we do not parse the text yet
            elif link_href.endswith("pdf"):
                link_text = download_and_read_pdf(link_href, self.datadump_directory_path)
            # NOTE add support for different file types
            total_links.append({
                "file_url": url,
                "link_url": link_href,
                "link_name": link_name,
                "full_extracted_text": link_text,
            })
        return text, total_links



    def process_all_years(self):

        all_urls = self.get_all_db_urls()
        data = self.fetch_all_data()
        

        result = []
        total_categories = []
        total_links = []
        for d in data:
            publication_name = d["type"]["publication_name"]
            taxonomy_list = d["Taxonomy"]
            taxonnomies = taxonomy_list.split("|") if taxonomy_list is not None else []
            timestamp = pd.to_datetime(d["pub_timestamp"], unit='s')
            categories = self.get_categories(taxonnomies, publication_name)
            document_types_urls = {
                os.path.splitext(urlparse(self.get_base_url() + url["id"]).path)[1][1:]: self.get_base_url() + url["id"]
                for url in d["documentTypes"]
            }
            if len(document_types_urls) == 0:
                continue
            if "pdf" in document_types_urls:
                temp_url = document_types_urls["pdf"]
                if temp_url in all_urls:
                    logger.debug(f"PDF already in db: {temp_url}")
                    continue
                logger.info(f"Processing PDF: {temp_url}")
                text = download_and_read_pdf(temp_url, self.datadump_directory_path)
                result.append({
                    "file_url": temp_url,
                    "date_published": timestamp,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text
                })
            elif "html" in document_types_urls or "htm" in document_types_urls:
                temp_url = document_types_urls.get("html", None)
                if temp_url is None:
                    temp_url = document_types_urls["htm"]
                if temp_url in all_urls:
                    logger.debug(f"HTML/HTM already in db: {temp_url}")
                    continue
                logger.info(f"Processing HTML: {temp_url}")
                text, links = self.parse_html(temp_url)
                result.append({
                    "file_url": temp_url,
                    "date_published": timestamp,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": text
                })
                total_links.extend(links)
            else:
                temp_url = sorted(list(document_types_urls.values()))[0]
                if temp_url in all_urls:
                    logger.debug(f"URL already in db: {temp_url}")
                    continue
                logger.info(f"Processing URL: {temp_url}")
                result.append({
                    "file_url": temp_url,
                    "date_published": timestamp,
                    "scraping_time": pd.Timestamp.now(),
                    "full_extracted_text": None
                })
            total_categories.extend(
                [
                    {
                        "file_url": temp_url,
                        "category_name": x.value
                    }
                    for x in categories
                ]
            )

        self.add_all_atomic(result, total_categories, total_links)



                
            
       
    

    def get_serach_url(self) -> str:
        return f"{self.get_base_url()}/press/pubbydate/html/index.en.html"
    
    def get_base_url(self) -> str:
        return "https://www.ecb.europa.eu"
    
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