import requests
import time
import pandas as pd
import numpy as np


class AppleProductRequester:
    def __init__(self):
        self.apple_links= {'Australia':'au',
         'Brazil': 'br',
         'Canada':'ca',
         'Chile':'cl',
         'China':'cn',
         'Colombia':'co',
         'Czech':'cz',
         'Europe':'fr',
         'Hungary':'hu',
         'India':'in',
         'Japan':'jp',
         'Korea':'kr',
         'Mexico':'mx',
         'New Zealand':'nz',
         'Norway':'no',
         'Poland':'pl',
         'Russia':'ru',
         'Singapore':'sg',
         'South Africa':'za',
         'Sweden':'se',
         'Switzerland':'ch-de',
         'Taiwan':'tw',
         'Thailand':'th',
         'Turkey':'tr',
         'UK':'uk',
         'US':'us'}
    def request_apple_product(self, country_to_work='Norway', product_line='ipad',product_name='ipad-pro'):
        '''
        EXAMPLE country_to_work='Norway', product_line='ipad',product_name='ipad-pro'''
        country_string = self.apple_links[country_to_work]
        apple_link= f'https://www.apple.com/{country_string}/shop/buy-{product_line}/{product_name}'
        rget = requests.get(apple_link)
        if rget.status_code == 200:
            return rget.text
        
    def get_all_country_apple_product_map(self, product_line='ipad',product_name='ipad-pro'):
        all_countries = self.apple_links.keys()
        xmap = {}
        for xcountry in all_countries:
            try:
                xmap[xcountry]=self.request_apple_product(country_to_work=xcountry, 
                                           product_line=product_line,
                                           product_name=product_name)
                time.sleep(1.5)
            except:
                print(xcountry)
                pass
        return xmap
    
    def create_full_apple_product_price_df(self, product_line='ipad',product_name = 'ipad_pro'):

        all_country_product_map= self.get_all_country_apple_product_map(product_line=product_line,
                                                                              product_name=product_name)
        yarr=[]
        farr=[]
        for country_to_work in list(all_country_product_map.keys()):
            try:
                sku_list = all_country_product_map[country_to_work].split(
                    '"products":[')[1].split(',"sectionEngagement"')[0].split('},{')
                sku_map ={}
                for temp_item in sku_list:
                    try:
                        sku=temp_item.split('sku":')[1].split(',')[0].replace('"','')
                        price=temp_item.split('"fullPrice":')[1].split('}')[0]
                        name = temp_item.split('"name"')[1].split('"')[1]
                        sku_map[sku]= {'price':price,'name':name, 
                         'country': country_to_work,
                         'product_line': product_line,
                         'product_name':product_name}
                    except:
                        pass
                full_sku_df = pd.DataFrame(sku_map).transpose()
                full_sku_df.index.name='sku'
                full_sku_df=full_sku_df.reset_index()
                yarr.append(full_sku_df)
                print('worked at '+country_to_work)
            except:
                print('failed at '+country_to_work)
                farr.append(country_to_work)
                pass
        basic_ppp1=pd.concat(yarr)
        return basic_ppp1
