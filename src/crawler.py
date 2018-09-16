# -*- coding: utf-8 -*-
import requests
import lxml.html
from bs4 import BeautifulSoup
import urllib.request
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import time
import pickle
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append('/Users/toshio/project/real_estate')
from config import rent_col, purchase_col

class Crawler:
    def __init__(self, data_version, target):
        self.data_version = data_version
        self.target = target
        if self.target == 'rent':
            self.url = 'http://www.fudousan.or.jp/system/?act=l&type=31&pref=13'
            self.bid_url = 'http://www.fudousan.or.jp/system/?act=d&type=31&pref=13&n=100&p=1&v=off&s=&bid='
        elif self.target == 'purchase':
            self.url = 'http://www.fudousan.or.jp/system/?act=l&type=12&pref=13'
            self.bid_url = 'http://www.fudousan.or.jp/system/?act=d&type=12&pref=13&n=100&p=1&v=off&s=&bid='
            
        self.page_url = lambda i: self.url+'&n=100&p='+str(i)+'&v=off,s='
        self.store_dir = '../storage/'
        self.save_dir = '../intermediate_data/'
        self.save_path = self.save_dir + \
            '{}_spec_{}.pickle'.format(self.target, self.data_version)
            
        self.page_num = self.get_page_num()
        self.urls = self.get_urls(self.page_num)
        self.bids = self.get_bids(self.urls)
        self.get_link_pages(self.bids)
        self.specs = self.load_link_pages(self.data_version)
        self.output(self.specs)

    def get_page_num(self):
        print('検索結果ページ数を取得中')
        soup = BeautifulSoup(requests.get(self.url).text, "lxml")
        soup_bodys = []
        soup_bodys.append(soup.body)
        for i in soup_bodys[0].find_all('th'):
            if (i.text).find('検索結果') != -1:
                end = (i.text).find('件')
                num = int(i.text[6:end])
        page_num = num//100+1
        print('検索結果ページ数:', page_num)
        return page_num

    def get_urls(self, page_num):
        print('検索結果ページのurlを取得中')
        # 検索全ページのurlを取得
        urls = []
        for i in range(2, page_num):
            urls.append(self.page_url(i))
        return urls

    def get_bids(self, urls):
        set_timeout = 300
        bids = []
        for i in range(len(urls)):
#         for i in range(1):
            if i % 10 == 0:
                print('bid番号取得中...{0}/{1}'.format(i, len(urls)))
            page_i = requests.get(urls[i], timeout = set_timeout)
            soup_j = BeautifulSoup(page_i.text, "lxml")
            for i, tag in enumerate(soup_j.find_all('a')):
                if (tag.get('href') != None):
                    bid_st = (tag.get('href')).find('bid=')
                    bid_len = (tag.get('href'))[bid_st:].find('&')
                    if bid_st != -1:
                        bid_num = tag.get('href')[bid_st+4:bid_st+bid_len]
                        bids.append(bid_num)
        
        bids = self.delete_duplication(bids) 
        return bids

    def load_bids(self):
        with open(self.store_dir+'bids.pickle', 'rb') as f:
            all_bids = pickle.load(f)
        return all_bids
    
    def delete_duplication(self, bids):
        # 重複したbit番号を削除
        return list(set(bids))

    def get_links(self, bids):
        print('各物件のlink取得')
        links = []
        for i in bids:
            links.append(self.bid_url+str(i))
        return links
    
    def get_link_pages(self, bids):
        links = self.get_links(bids)
        connect_timeout = 1000
        read_timeout = 1000
        retry = 5
        st = 0
        print('各物件のhtmlを取得中')
        link_pages = []
        for i in range(len(links)):
#         for i in range(5):
            if i % 10 == 0:
                print('各物件のhtml取得中...{0}/{1}'.format(i, len(links)))
            if (i % 100 == 0) & (i !=0):
                end = i
                print('一旦link_pagesをpickle化...{}-{}'.format(st, end))
                with open(self.store_dir+'response_{}-{}_{}.pickle'.format(st, end, self.data_version), 'wb') as f:
                    pickle.dump(link_pages, f)
                link_pages = []
                st = end
            session = requests.Session()
            session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
            session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))
            link_page_i  = session.get(url=links[i], stream=True, timeout=(connect_timeout, read_timeout))
            link_pages.append(link_page_i)
        end = len(links)
        with open(self.store_dir+'response_{}-{}_{}.pickle'.format(st, end, self.data_version), 'wb') as f:
            pickle.dump(link_pages, f)
        
    def load_link_pages(self, load_date):
        # htmlをpickle化したものをロード
        specs = []
        for j in range(0,len(self.bids),100):
            st = j
            end = j+100
            with open(self.store_dir+'response_{}-{}_{}.pickle'.format(st, end, load_date), 'rb') as f:
                link_pages = pickle.load(f)

            print('各物件ページのhtmlをパースして、bodyを取得')
            link_bodys = []
            cnt = 0
            for i in link_pages:
                cnt += 1
                if cnt % 100 == 0:
                    print(
                        '各物件ページのhtmlをパース中...{0}/{1}'.format(cnt, len(link_pages)))
                link_soup_i = BeautifulSoup(i.text, "lxml")
                link_bodys.append(link_soup_i)

            print('各物件情報取得中')
            for i, link_bd in enumerate(link_bodys):
                if i % 100 == 0:
                    print('各物件情報取得中...{0}/{1}'.format(i, len(link_bodys)))
                keys = []
                values = []

                for s in link_bd.find_all('span'):
                    if (s.text).find('価格') != -1:
                        price = s.text.split()[1][:-1]
                        keys.append('price')
                        values.append(price)

                for t in link_bd.find_all('tr'):
                    if (t.find('th') != None) & (t.find('td') != None):
                        th_tags = t.find_all('th')
                        td_tags = t.find_all('td')
                        for i in range(len(th_tags)):
                            keys.append(th_tags[i].text)
                            values.append(td_tags[i].text)
                spec = dict(zip(keys, values))
                specs.append(spec)
        return specs

    def output(self, specs):
        # dictionaryに保存したデータをpickleに出力する
        df_specs = pd.DataFrame(specs)
        if self.target == 'rent':
            col = rent_col
        elif self.target == 'purchase':
            col = purchase_col
        output = pd.DataFrame(columns = col)
        for i, c in enumerate(col):
            output[output.columns[i]] = df_specs[c]
        with open(self.save_path, 'wb') as f:
            pickle.dump(output, f)
        print('Got spec!!')


if __name__ == '__main__':
    data_version = datetime.now().strftime("%Y%m%d")
    load_date = data_version
    target = 'purchase'
    crawler = Crawler(data_version, target)