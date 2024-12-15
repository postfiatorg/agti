import datetime
import urllib
import pandas as pd
import numpy as np
import requests
import datetime
import urllib
import pandas as pd
import numpy as np
import requests
import re
from basic_utilities.database_tools import DBConnectionManager
from ai_systems.openai_tooling import OpenAIRequestTool

class WikipediaDataTool:
    def __init__(self):
        print('initiated wiki tool')
    def get_recent_wiki_trends_for_page_url(self, page_url='XPO_Logistics'):
        #article='Peter_Thiel',
        start_date=datetime.datetime.strptime('2015-01-01','%Y-%m-%d')
        ''' article='Peter Thiel', start_date=pd.to_datetime('2015-01-01') '''
        pd.options.mode.chained_assignment = None  # default='warn'
        #w_page=wikipedia.page(title=article)


        term_version =  urllib.request.unquote(page_url).replace('_',' ')

        REQUEST_TPL = ('http://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/'
                       'en.wikipedia/all-access/user/{}/daily/{}/{}')
        start_date = start_date.strftime('%Y%m%d')
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        resp = requests.get(REQUEST_TPL.format(page_url, start_date, end_date),headers=headers)
        resp_json = resp.json()
        wiki_daily = pd.DataFrame(resp_json['items'])
        wiki_daily['timestamp'] = wiki_daily['timestamp'].apply(lambda x: pd.to_datetime(x[0:8]))
        wiki_daily['views']=wiki_daily['views'].astype(float)
        wiki_output=wiki_daily[['timestamp','views','article']]
        wiki_output.columns = ['date','value','page_name']
        wiki_output['search_term'] = term_version
        wiki_output['source'] = 'api'
        return wiki_output
    
    def get_short_term_wiki_trends_for_page_url(self, page_url='XPO_Logistics'):
        #article='Peter_Thiel',
        start_date=datetime.datetime.strptime('2021-01-01','%Y-%m-%d')
        ''' article='Peter Thiel', start_date=pd.to_datetime('2015-01-01') '''
        pd.options.mode.chained_assignment = None  # default='warn'
        #w_page=wikipedia.page(title=article)
        term_version =  urllib.request.unquote(page_url).replace('_',' ')

        REQUEST_TPL = ('http://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/'
                       'en.wikipedia/all-access/user/{}/daily/{}/{}')
        start_date = start_date.strftime('%Y%m%d')
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        resp = requests.get(REQUEST_TPL.format(page_url, start_date, end_date),headers=headers)
        resp_json = resp.json()
        wiki_daily = pd.DataFrame(resp_json['items'])
        wiki_daily['timestamp'] = wiki_daily['timestamp'].apply(lambda x: pd.to_datetime(x[0:8]))
        wiki_daily['views']=wiki_daily['views'].astype(float)
        wiki_output=wiki_daily[['timestamp','views','article']]
        wiki_output.columns = ['date','value','page_name']
        wiki_output['search_term'] = term_version
        wiki_output['source'] = 'api'
        return wiki_output
