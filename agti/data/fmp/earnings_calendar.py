import requests
import datetime
import pandas as pd
class FMPEarningsCalendar:
    def __init__(self, pw_map):
        self.pw_map=pw_map
        self.api_key = pw_map['financialmodelingprep']
    
    def get_todays_earnings_calendar(self):
        today = datetime.date.today().strftime('%Y-%m-%d')
        next_business_day = pd.bdate_range(start=datetime.date.today(), periods=2)[1].date().strftime('%Y-%m-%d')
        api_key = self.api_key
        url = f'https://financialmodelingprep.com/api/v3/earning_calendar?from={today}&to={next_business_day}&apikey={api_key}'
        response = requests.get(url)
        data = response.json()
        todays_earnings_calendar = pd.DataFrame(data)
        return todays_earnings_calendar
