from __future__ import annotations

from dataclasses import dataclass, field
from functools import cache
from enum import Enum

from agti.agti.utilities.db_manager import DBConnectionManager



@dataclass
class BotoS3Config:
    REGION_NAME: str
    ACCESS_KEY: str
    SECRET_KEY: str
    BUCKET_NAME: str
    ENDPOINT_URL: str | None = None

@dataclass
class Country:
    COUNTRY_CODE_ALPHA_3: str
    COUNTRY_NAME: str


@dataclass
class CountryCB(Country):
    NETLOC: str
    PROXY_COUNTRIES: list[str] = field(default_factory=list)
    language_path : str | None = None


    @property
    def URL(self) -> str:
        url = f"https://{self.NETLOC}/"
        if self.language_path:
            url += self.language_path + "/"
        return url


@dataclass
class SQLDBCONFIG:
    USER_NAME: str
    TABLE_NAME: str
    CONNECTION_MANAGER: DBConnectionManager


@dataclass
class SCRAPERCONFIG:
    SLEEP_MIN: float = 0
    SLEEP_MAX: float = 3
    DISABLE_PDF_PARSING: bool = False
    SESSION_REFRESH_INTERVAL: int = 10

    def __post_init__(self):
        if self.SLEEP_MIN < 0:
            raise ValueError("SLEEP_MIN must be non-negative")
        if self.SLEEP_MAX < self.SLEEP_MIN:
            raise ValueError("SLEEP_MAX must be greater than or equal to SLEEP_MIN")
        if self.SESSION_REFRESH_INTERVAL <= 0:
            raise ValueError("SESSION_REFRESH_INTERVAL must be positive")

class SupportedScrapers(Enum):
    AUSTRALIA = CountryCB(
        COUNTRY_CODE_ALPHA_3="AUS",
        COUNTRY_NAME="Australia",
        NETLOC="www.rba.gov.au",
    )
    
    CANADA = CountryCB(
        COUNTRY_CODE_ALPHA_3="CAN",
        COUNTRY_NAME="Canada",
        NETLOC="www.bankofcanada.ca",
    )
    
    EUROPE = CountryCB(
        COUNTRY_CODE_ALPHA_3="EUE",
        COUNTRY_NAME="European Union",
        NETLOC="www.ecb.europa.eu",
    )
    
    ENGLAND = CountryCB(
        COUNTRY_CODE_ALPHA_3="ENG",
        COUNTRY_NAME="England",
        NETLOC="www.bankofengland.co.uk",
    )
    
    USA = CountryCB(
        COUNTRY_CODE_ALPHA_3="USA",
        COUNTRY_NAME="United States of America",
        NETLOC="www.federalreserve.gov",
    )
    
    JAPAN = CountryCB(
        COUNTRY_CODE_ALPHA_3="JPN",
        COUNTRY_NAME="Japan",
        NETLOC="www.boj.or.jp",
        language_path="en"
    )
    
    NORGES = CountryCB(
        COUNTRY_CODE_ALPHA_3="NOR",
        COUNTRY_NAME="Norway",
        NETLOC="www.norges-bank.no",
        language_path="en"
    )
    
    SWEDEN = CountryCB(
        COUNTRY_CODE_ALPHA_3="SWE",
        COUNTRY_NAME="Sweden",
        NETLOC="www.riksbank.se",
        language_path="en-gb",
        PROXY_COUNTRIES=['se','fi','no']
    )
    
    SWITZERLAND = CountryCB(
        COUNTRY_CODE_ALPHA_3="CHE",
        COUNTRY_NAME="Switzerland",
        NETLOC="www.snb.ch",
        language_path="en",
    )


    @classmethod
    def get_member_based_on_country_code(cls, country_code: str) -> SupportedScrapers | None:
        for member in cls:
            if member.value.COUNTRY_CODE_ALPHA_3 == country_code:
                return member
        return None