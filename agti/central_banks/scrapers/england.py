import time
from typing import Dict, Set
from urllib.parse import urlparse
import pandas as pd
import logging
import copy
import selenium
from selenium.webdriver.common.by import By
from agti.agti.central_banks.types import MainMetadata
from agti.utilities.settings import CredentialManager
from agti.utilities.settings import PasswordMapLoader
from agti.utilities.db_manager import DBConnectionManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base_scrapper import BaseBankScraper
from ..utils import Categories


logger = logging.getLogger(__name__)

__all__ = ["EnglandBankScraper"]

class EnglandBankScraper(BaseBankScraper):
    MAX_OLD_YEAR = 2000
    IGNORED_PATHS = [
        "/subscribe-to-emails",
        "/contact",
        "/news",
        "/search",
    ]

    def initialize_cookies(self, go_to_url=False):
        if go_to_url:
            self.driver_manager.driver.get(self.bank_config.URL)
        wait = WebDriverWait(self.driver_manager.driver, 10)
        xpath = "//button[@class='cookie__button btn btn-default']"
        repeat = 3
        for i in range(repeat):
            try:
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

    def initialize_my_js(self):
        # https://www.bankofengland.co.uk/scripts/news.js additional scripts
        INITIALIZE_JS_SCRIPT = """
window.agti_initAjaxCall = async function initAjaxCall(obj, reload, callback) {

    /* ---------- 1. normalise paging ---------- */
    if (reload) {
        obj.Page = 1;
        CP.NEWS.Page = 1;
    }

    /* ---------- 2. pick the target container ---------- */
    const $loadResults = (reload || obj.Page === 1)
        ? $('#SearchResults')
        : (CP.BOE.InfiniteScrolling ? $('#LoadMore') : $('#SearchResults'));

    /* ---------- 3. show the loader, hide results ---------- */
    $loadResults.hide();
    $('.loading')
        .show()
        .html('<img src="/assets/img/loader.svg" role="progressbar" aria-busy="true" alt="" />' +
              '<p class="loading-text">Content loading</p>');

    try {
        /* ---------- 4. synchronous‑looking AJAX call ---------- */
        const result = await $.ajax({
            url: '/_api/News/RefreshPagedNewsList',
            method: 'POST',
            data: obj,
            dataType: 'json'          // server returns { Results, Refiners, … }
        });

        /* ---------- 5. update the DOM (no animations) ---------- */
        if (CP.BOE.InfiniteScrolling && obj.Page > 1) {
            // append for infinite scroll
            $loadResults.replaceWith(result.Results);
        } else {
            // first page or classic paging
            $loadResults.html(result.Results).show();
            // live‑region count for screen readers
            const textCount = $('#resultCount').text();
            $('#resultCountAnnounce').text(textCount);
        }

        $('.sidebar-filters.taxonomy-filters').html(result.Refiners).show();
        $('.loading').empty().hide();

        if ($('.no-results').length) {
            $('.no-results').show();
        } else {
            window.loading = false;   // keeps your old flag logic intact
        }

        window.IEimages();            

        /* ---------- 6. reflect state in URL/history ---------- */
        if (!reload) {
            window.setState(obj);
        }

    } catch (xhr) {
        callback(false);
    }
    callback(true);
}

window.setState = function (obj) {
    if (!CP.BOE.InfiniteScrolling) {
        history.pushState(obj, null, null);
        //console.info("SetState Done");
    }
}
window.IEimages = function() {
    if (!Modernizr.objectfit) {
        $('.release').each(function () {
            var $imageContainer = $(this).find('.release-img'),
                imgUrl = $imageContainer.find('img').prop('src');
            if (imgUrl) {
                $imageContainer
                    .css("background-image", "url('" + imgUrl + "')")
                    .addClass('object-fit');
            }
        });
    }
}
var callback = arguments[0];
$(document).ready(function(){
    window.agti_obj = null;
    callback(); // this garantees that document is ready before executing other scripts
});
        """
        self.driver_manager.driver.execute_async_script(INITIALIZE_JS_SCRIPT)



    def init_filter(self, topic):
        self.get(self.get_base_url())

        # initialize js
        self.initialize_my_js()

        topics = ["Research blog", "Event", "News", "Publication", "Speech", "Statistics"]
        SET_TOPICS_SCRIPT = """
var topics_to_set = arguments[0];
var callback = arguments[1];
(async function(){
    
    typeValues = $('.sidebar-filters.type-filters').find('input[type="checkbox"]').toArray();
    typeValues.forEach((el) => {
        const parent = el.parentElement;
        const el_text = parent.textContent.trim();
        if (topics_to_set.includes(el_text)) {
            $(el).prop('checked', true);
            // remove it from topics_to_set
            const index = topics_to_set.indexOf(el_text);
            if (index > -1) {
                topics_to_set.splice(index, 1);
            }
            
        } else {
            $(el).prop('checked', false);
        }
    });
    // check if all topics are set
    var obj = CP.NEWS.initGetFilters();
    await window.agti_initAjaxCall(obj, true, (x) => {});
    callback(topics_to_set);
})();   
        """
        not_setted = self.driver_manager.driver.execute_async_script(SET_TOPICS_SCRIPT, topics)
        if len(not_setted) > 0:
            raise ValueError(
                f"Not all topics were set: {not_setted} for topic: {topic}."
                "Please check the topics in the script."
            )
        SET_TAXONOMY_SCRIPT = """
const taxonomy_to_set =  arguments[0];
const callback = arguments[1];
(async function(){
    var taxonomy_set = false;
    taxonomyValues = $('.sidebar-filters.taxonomy-filters').find('input[type="checkbox"]').toArray();
    taxonomyValues.forEach((el) => {
        const parent = el.parentElement;
        const el_text = parent.textContent.trim();
        if (el_text === taxonomy_to_set) {
            $(el).prop('checked', true);
            taxonomy_set = true;
        } else {
            $(el).prop('checked', false);
        }
    });
    if (!taxonomy_set) {
        callback(false);
        return;
    }
    window.agti_obj = CP.NEWS.initGetFilters();
    await window.agti_initAjaxCall(window.agti_obj, true, callback);
})();
        """
        taxonomy_success = self.driver_manager.driver.execute_async_script(SET_TAXONOMY_SCRIPT, topic)
        if not taxonomy_success:
            raise ValueError(
                f"Could not set taxonomy: {topic}. "
                "Please check the taxonomy in the script."
            )
        

    def get_number_of_pages(self) -> int:
        SCRIPT_GET_NUMBER_OF_PAGES = """
const callback = arguments[0];
$(document).ready(function(){
    var pageCount = $(".container-list-pagination").data("pagecount");
    callback(pageCount);
});
        """
        num_pages = self.driver_manager.driver.execute_async_script(SCRIPT_GET_NUMBER_OF_PAGES)
        if num_pages is None:
            raise ValueError("Could not get number of pages. Please check the script.")
        if not isinstance(num_pages, int):
            raise ValueError(f"Number of pages is not an integer: {num_pages}. Please check the script.")
        return num_pages

    def next_page(self):

        NEXT_PAGE_SCRIPT = """
const callback = arguments[0];
( async function(){
    window.agti_obj.Page += 1;
    var callSuccesful = true;
    await window.agti_initAjaxCall(window.agti_obj, false, (x) => {
        callSuccesful = x;
    });
    if (!callSuccesful) {
        callback(-1);
    }
    callback(window.agti_obj.Page);
})();
        """

        current_page = self.driver_manager.driver.execute_async_script(NEXT_PAGE_SCRIPT)
        if not isinstance(current_page, int):
            raise ValueError(f"Current page is not an integer: {current_page}. Please check the script.")
        return current_page

    def parse_html(self, url: str, date: pd.Timestamp, scraping_time: pd.Timestamp):
        # find main with id="main-content"
        self.get(url)
        year = str(date.year)
        xpath = "//main[@id='main-content']"
        main = self.driver_manager.driver.find_element(By.XPATH, xpath)
        main_metadata = MainMetadata(
            url=url,
            date_published=str(date),
            date_published_str=None,
            scraping_time=str(scraping_time),
        )
        file_id = self.process_html_page(main_metadata,year)
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
        return file_id, processed_links




    def process_all_years(self):
        wait = WebDriverWait(self.driver_manager.driver, 10)
        self.get(self.get_base_url())
        wait.until(EC.visibility_of_element_located((By.ID, "SearchResults")))
        all_urls = self.get_all_db_urls()
        all_categories = [(url, category_name) for url, category_name in self.get_all_db_categories()]
        topics = [
            "Quarterly Bulletin",
            "News release",
            "Monetary Policy Committee (MPC)",
            "Research",
            "Working Paper",
            "Monetary policy",
            "Financial Policy Committee (FPC)",
            "Weekly report",
            "Minutes",
            "Financial stability"
        ]

       
        
        for topic in topics:
            logger.info(f"Processing topic: {topic}")
            self.init_filter(topic)
            year = pd.Timestamp.now().year
            to_process = []
            num_pages = self.get_number_of_pages()
            current_page = 1
            while year >= self.MAX_OLD_YEAR:
                # get id = SearchResults div
                search_results = self.driver_manager.driver.find_element(By.ID, "SearchResults")
                # find all elements with class="col3"
                elements = search_results.find_elements(By.XPATH, ".//div[@class='col3']")
                for element in elements:
                    a = element.find_element(By.TAG_NAME, "a")
                    href = a.get_attribute("href")

                    # tag is under a in class="release-tag" div 
                    tag = a.find_element(By.CLASS_NAME, "release-tag-wrap").text

                    # get date using time tag with datetime attribute
                    time_tag = element.find_element(By.TAG_NAME, "time")
                    date = pd.to_datetime(time_tag.get_attribute("datetime"))
                    to_process.append((tag, href, date))
                    year = min(year, date.year)
                if current_page == num_pages:
                    logger.info(f"Reached the last page for topic: {topic} in year: {year}")
                    break
                current_page = self.next_page()
                    
            for tag, href, date in to_process:
                total_categories = [
                    {"file_url": href, "category_name": category.value}
                    for category in get_categories(tag)
                    if (href, category.value) not in all_categories
                ]
                if href in all_urls:
                    logger.debug(f"Href is already in db: {href}")
                    continue
                logger.info(f"Processing: {href}")
                scraping_time = pd.Timestamp.now()
                main_id, links_output = self.parse_html(href, date, scraping_time)
                result = {
                    "date_published": date,
                    "scraping_time": scraping_time,
                    "file_url": href,
                    "file_id": main_id,
                }
                total_links = [
                    {
                        "file_url": href,
                        "link_url": link,
                        "link_name": link_text,
                        "file_id": link_id,
                    } for (link, link_text, link_id) in links_output
                ]
                self.add_all_atomic([result],total_categories,total_links)



    def get_base_url(self) -> str:
        return "https://www.bankofengland.co.uk/news"
    



def get_categories(tag: str) -> Set[Categories]:
    """
    Return a set of Categories based on the input tag.
    The mapping is based on the Bank of England taxonomy mapped to our unified categories.
    If no mapping is found, returns {Categories.OTHER}.
    """
    # Mapping from tag patterns to a set of Categories.
    mapping = {
        # --- Event Items ---
        "Event": {Categories.NEWS_AND_EVENTS},
        "Event // Agency briefing": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Event // Bank of England agenda for research": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Centre for Central Banking Studies (CCBS)": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Conference": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Parliamentary hearing": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Event // Seminar": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Event // Treasury Select Committee (TSC)": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Event // Webinar": {Categories.NEWS_AND_EVENTS},
        "Event // Workshop": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        
        # --- News Items ---
        "News": {Categories.NEWS_AND_EVENTS},
        "News // Annual Report": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Archive": {Categories.NEWS_AND_EVENTS},
        "News // Banknotes": {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS},
        "News // CBDC Technology Forum minutes": {Categories.MONETARY_POLICY, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Corporate responsibility": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Court minutes": {Categories.NEWS_AND_EVENTS, Categories.MONETARY_POLICY},
        "News // Financial Policy Committee (FPC)": {Categories.MONETARY_POLICY},
        "News // Financial Policy Committee statement": {Categories.MONETARY_POLICY},
        "News // Financial Stability Report (FSR)": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Financial stability": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Financial stability policy": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Fintech": {Categories.NEWS_AND_EVENTS, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Funding for Lending Scheme (FLS)": {Categories.MONETARY_POLICY},
        "News // Independent Evaluation Office (IEO)": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Index-Linked Treasury Stocks": {Categories.CURRENCY_AND_FINANCIAL_INSTRUMENTS},
        "News // Letter": {Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.NEWS_AND_EVENTS},
        "News // Markets": {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Minutes": {Categories.MONETARY_POLICY},
        "News // Monetary Policy Committee (MPC)": {Categories.MONETARY_POLICY},
        "News // News release": {Categories.NEWS_AND_EVENTS},
        "News // Open Forum": {Categories.NEWS_AND_EVENTS},
        "News // Payment systems": {Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Prudential regulation": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Record": {Categories.NEWS_AND_EVENTS},
        "News // Research": {Categories.RESEARCH_AND_DATA},
        "News // Schools competition": {Categories.OTHER},
        "News // Semi-Annual FX Turnover Survey results": {Categories.RESEARCH_AND_DATA, Categories.MARKET_OPERATIONS_AND_PAYMENT_SYSTEMS},
        "News // Staff": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "News // Statement": {Categories.MONETARY_POLICY},
        "News // Statistical article": {Categories.RESEARCH_AND_DATA},
        "News // Statistics": {Categories.RESEARCH_AND_DATA},
        "News // Stress testing": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "News // Treasury Select Committee (TSC)": {Categories.NEWS_AND_EVENTS, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        
        # --- Publication Items ---
        "Publication": {Categories.RESEARCH_AND_DATA},
        "Publication // Andy Haldane": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Publication // Approach document": {Categories.MONETARY_POLICY},
        "Publication // Balance sheet": {Categories.INSTITUTIONAL_AND_GOVERNANCE, Categories.RESEARCH_AND_DATA},
        "Publication // Centre for Central Banking Studies (CCBS)": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Publication // Consultation paper": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Countercyclical capital buffer (CCyB)": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Discussion paper": {Categories.RESEARCH_AND_DATA},
        "Publication // External MPC Unit Discussion Paper": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Financial Policy Committee (FPC)": {Categories.MONETARY_POLICY},
        "Publication // Financial Stability Paper": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Financial Stability Report (FSR)": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Financial Stability in Focus": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Financial stability": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Independent Evaluation Office (IEO)": {Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Publication // Inflation Attitudes Survey": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Inflation Report (IR)": {Categories.MONETARY_POLICY},
        "Publication // Minutes": {Categories.MONETARY_POLICY},
        "Publication // Monetary Policy Committee (MPC)": {Categories.MONETARY_POLICY},
        "Publication // Monetary Policy Report (MPR)": {Categories.MONETARY_POLICY},
        "Publication // Paper": {Categories.RESEARCH_AND_DATA},
        "Publication // Policy statement": {Categories.MONETARY_POLICY},
        "Publication // Quarterly Bulletin": {Categories.RESEARCH_AND_DATA},
        "Publication // Report": {Categories.RESEARCH_AND_DATA, Categories.INSTITUTIONAL_AND_GOVERNANCE},
        "Publication // Research": {Categories.RESEARCH_AND_DATA},
        "Publication // Response to a paper": {Categories.RESEARCH_AND_DATA},
        "Publication // Statement of policy": {Categories.MONETARY_POLICY},
        "Publication // Stress testing": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Systemic Risk Survey Results": {Categories.FINANCIAL_STABILITY_AND_REGULATION},
        "Publication // Weekly report": {Categories.MONETARY_POLICY, Categories.RESEARCH_AND_DATA},
        "Publication // Working Paper": {Categories.RESEARCH_AND_DATA},
        
        # --- Research Blog Items ---
        "Research blog": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        "Research blog // Bank Overground": {Categories.NEWS_AND_EVENTS, Categories.RESEARCH_AND_DATA},
        
        # --- Speech Items ---
        "Speech": {Categories.NEWS_AND_EVENTS},
        "Speech // Webinar": {Categories.NEWS_AND_EVENTS},
    }
    
    result : Set[Categories] = set()
    
    # Check for an exact match first.
    if tag in mapping:
        result.update(mapping[tag])
    else:
        # Fallback: check if any mapping key is a prefix of the tag.
        for key, cats in mapping.items():
            if tag.startswith(key):
                result.update(cats)
    
    # If no category was matched, assign it to OTHER.
    if not result:
        result.add(Categories.OTHER)
    
    return result