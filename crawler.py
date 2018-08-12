# -*- coding: utf-8 -*-
import requests
import lxml.html
from bs4 import BeautifulSoup
import urllib.request
import time
import pickle
import pandas as pd
from datetime import datetime
from config import rent_col, purchase_col

class Crawler:
    def __init__(self, data_version, target):
        self.data_version = data_version
        self.target = target
        if self.target == 'Rent':
            self.url = 'http://www.fudousan.or.jp/system/?act=l&type=31&pref=13'
            self.bid_url = 'http://www.fudousan.or.jp/system/?act=d&type=31&pref=13&n=20&p=1&v=on&s=&bid='
        elif self.target == 'Purchase':
            self.url = 'http://www.fudousan.or.jp/system/?act=l&type=12&pref=13'
            self.bid_url = 'http://www.fudousan.or.jp/system/?act=d&type=12&pref=13&n=20&p=1&v=on&s=&bid='
            
        self.page_url = lambda i: self.url+'&n=20&p='+str(i)+'&v=off,s='
        self.store_dir = 'storage/'
        self.save_dir = 'intermediate_data/'
        self.save_path = self.save_dir + \
            'spec_{}.pickle'.format(self.data_version)

    def get_page_num(self):
        print('検索結果ページ数を取得中')
        soup = BeautifulSoup(requests.get(self.url).text, "lxml")
        soup_bodys = []
        soup_bodys.append(soup.body)
        for i in soup_bodys[0].find_all('th'):
            if (i.text).find('検索結果') != -1:
                end = (i.text).find('件')
                num = int(i.text[6:end])
        page_num = num//20+1
        print('検索結果ページ数:', page_num)
        return page_num

    def get_urls(self, page_num):
        print('検索結果ページのurlを取得中')
        # 検索全ページのurlを取得
        urls = []
        for i in range(2, page_num):
            urls.append(self.page_url(i))
        return urls

    def get_pages(self, urls):
        interval = 0.5
        print('検索結果ページのhtmlを{}秒おきに取得'.format(interval))
        pages = []
#         for i in range(len(urls)):
        for i in range(1):
            if i % 10 == 0:
                print('検索結果ページのhtml取得中...{0}/{1}ページ'.format(i, len(urls)))
            time.sleep(interval)
            page_i = requests.get(urls[i])
            pages.append(page_i)
        return pages

    def save_pages(self, pages):
        print('取得したページのhtmlをpickle化')
        with open(self.store_dir+'pages_{}.pickle'.format(self.data_version), 'wb') as f:
            pickle.dump(pages, f)

    def load_pages(self, load_date):
        # htmlをpickle化したものをロード
        with open(self.store_dir+'pages_{}.pickle'.format(load_date), 'rb') as f:
            pages = pickle.load(f)
        return pages

    def get_bodys(self, pages):
        print('検索結果ページのhtmlをパースして、bodyを取得')
        bodys = []
        for i, p in enumerate(pages):
            if i % 100 == 0:
                print('検索結果ページのhtmlをパース中...{0}/{1}ページ'.format(i, len(pages)))
            soup_i = BeautifulSoup(p.text, "lxml")
            bodys.append(soup_i)
        return bodys

    def get_bids(self, bodys):
        print('検索結果ページ内のbit番号を取得')
        bids = []
        for j, bd in enumerate(bodys):
            if j % 100 == 0:
                print('検索結果ページ内のbit番号を取得中...{0}/{1}ページ'.format(j, len(bodys)))
            for i, tag in enumerate(bd.find_all('a')):
                if (tag.get('href') != None):
                    bid_st = (tag.get('href')).find('bid=')
                    bid_len = (tag.get('href'))[bid_st:].find('&')
                    if bid_st != -1:
                        bid_num = tag.get('href')[bid_st+4:bid_st+bid_len]
                        bids.append(bid_num)
        return bids

    def delete_duplication(self, bids):
        # 重複したbit番号を削除
        return list(set(bids))

    def get_links(self, bids):
        print('各物件のlink取得')
        links = []
        for i in bids:
            links.append(self.bid_url+str(i))
        return links

    def get_link_pages(self, links):
        interval = 0.5
        print('各物件のhtmlを{}秒おきに取得'.format(interval))
        link_pages = []
#         for i in range(len(links)):
        for i in range(5):
            if i % 10 == 0:
                print('各物件のhtml取得中...{0}/{1}'.format(i, len(links)))
            time.sleep(interval)
            link_page_i = requests.get(links[i])
            link_pages.append(link_page_i)
        return link_pages

    def save_link_pages(self, link_pages):
        print('取得した物件ページのhtmlをpickle化')
        with open(self.store_dir+'link_pages_{}.pickle'.format(self.data_version), 'wb') as f:
            pickle.dump(link_pages, f)

    def load_link_pages(self, load_date):
        # htmlをpickle化したものをロード
        with open(self.store_dir+'link_pages_{}.pickle'.format(load_date), 'rb') as f:
            link_pages = pickle.load(f)
        return link_pages

    def get_link_bodys(self, link_pages):
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
        return link_bodys

    def crawling(self, link_bodys):
        print('各物件情報取得中')
        specs = []

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

    def save_specs(self, specs):
        print('取得した物件ページのspecをpickle化')
        with open(self.store_dir+'specs_{}.pickle'.format(self.data_version), 'wb') as f:
            pickle.dump(specs, f)

    def load_specs(self, load_date):
        # htmlをpickle化したものをロード
        with open(self.store_dir+'specs_{}.pickle'.format(load_date), 'rb') as f:
            specs = pickle.load(f)
        return specs

    def output(self, specs):
        # dictionaryに保存したデータをcsvに出力する
        df_specs = pd.DataFrame(specs)
        if self.target == 'Rent':
            col = rent_col
        elif self.target == 'Purchase':
            col = purchase_col
        output = pd.DataFrame(columns = col)
        for i, c in enumerate(col):
            output[output.columns[i]] = df_specs[c]
        with open(self.save_path, 'wb') as f:
            pickle.dump(output, f)
        print('Got spec!!')


if __name__ == '__main__':
    data_version = datetime.now().strftime("%Y%m%d")
    target = 'Purchase'
    crawler = Crawler(data_version, target)
    page_num = crawler.get_page_num()
    urls = crawler.get_urls(page_num)
    pages = crawler.get_pages(urls)
    crawler.save_pages(pages)
#     load_date = 'original'
#     pages = crawler.load_pages(load_date)
    bodys = crawler.get_bodys(pages)
    bids = crawler.get_bids(bodys)
    bids = crawler.delete_duplication(bids)
    links = crawler.get_links(bids)
    link_pages = crawler.get_link_pages(links)
    crawler.save_link_pages(link_pages)
#     link_pages = crawler.load_link_pages(load_date)
    link_bodys = crawler.get_link_bodys(link_pages)
    specs = crawler.crawling(link_bodys)
    crawler.save_specs(specs)
#     specs = crawler.load_specs(load_date)
    crawler.output(specs)