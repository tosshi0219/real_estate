import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from sklearn import preprocessing
import pickle

import sys
import os
sys.path.append('/Users/toshio/MyProject/real_estate')
from config import dropcol_rent

class PreprocessRent:
    def __init__(self,mode,data_version,encoder_version):
        self.mode=mode
        if mode=='inference':
            self.data_version=data_version #推論対象となるデータセットのversion
            self.encoder_version=encoder_version #モデルのversion
            self.encoder_path='label/label_encoder_rent_{}.pickle'.format(self.encoder_version)
            self.save_path='intermediate_data/preprocessed_rent_for_inference_{}.pickle'.format(self.data_version)

        elif mode=='learning':
            self.data_version=data_version  #学習データセットのversion
            self.encoder_version=self.data_version
            self.encoder_path='label/label_encoder_rent_{}.pickle'.format(self.encoder_version)
            self.save_path='intermediate_data/preprocessed_rent_for_learning_{}.pickle'.format(self.data_version)

        self.load_path='intermediate_data/rent_spec_{}.pickle'.format(self.data_version)

    def load_spec(self):
        with open(self.load_path,'rb') as f:
            spec=pickle.load(f)
        spec = pd.DataFrame(spec)
        spec=spec.drop(dropcol_rent, axis=1)
        return spec
    
    def pre_price(self, df):
        df=df[~df['price'].isnull()]
        df=df.reset_index(drop=True)
        df['price']=df['price'].astype('str')
        df['price']=df['price'].str.replace(',','')
        price=[]
        for i in df['price']:
            if i.find('万') != -1:
                man,sen = i.split('万')
                if sen == '':
                    sen = 0
                price.append(int(man)*10000+int(sen))
            else:
                price.append(int(i))
        df['price']=price
        return df

    def pre_bid(self, df):
        df=df[~df['詳細情報'].isnull()]
        df=df.reset_index(drop=True)
        bid=[]
        for i,v in enumerate(df['詳細情報']):
            a,b=v.split('／')
            bid.append(a[len('物件番号:'):])
        df['詳細情報']=pd.DataFrame(bid)
        return df

    def pre_balcony(self, df):
        col = 'バルコニー等面積'
        df[col] = df[col].fillna(0)
        df[col] = df[col].str.replace('㎡','')
        df[col] = df[col].str.replace('-','0')
        df[col] = df[col].astype(float)
        return df

    def pre_transportation(self, df):
        station=[]
        method=[]
        time=[]
        for i,s in enumerate(df['交通']):
            f1=s.find('「')
            r1=s.find('」')
            station.append(s[f1+1:r1])
            rest=s[r1:]
            r2=rest.find('分')
            s1=s[r1:r1+r2]
            if s1.find('徒歩')!=-1:
                method.append('徒歩')
                try:
                    time.append(int(s1[s1.find('徒歩')+2:]))
                except:
                    time.append(np.nan)
            elif (s1.find('バス')!=-1)&(s1.find('バスにて')==-1):
                method.append('バス')
                time.append(int(s1[s1.find('バス')+2:]))
            elif s1.find('バスにて')!=-1:
                method.append('バス')
                time.append(int(s1[s1.find('バスにて')+4:]))
            else:
                method.append(np.nan)
                time.append(np.nan)

        df=df.drop('交通',axis=1)
        df['最寄り駅']=pd.DataFrame(station)
        df['駅からの手段']=pd.DataFrame(method)
        df['駅からの時間']=pd.DataFrame(time)

        df=df.dropna()
        df=df.reset_index(drop=True)

        return df

    def pre_deposit(self, df):
        col = '保証金・権利金'
        df[col] = df[col].str.replace('保証金なし・-','0')
        df[col] = df[col].str.replace('-・-','0')
        df[col] = df[col].str.replace('円・-','')
        df[col] = df[col].str.replace(',','')
        price = []
        for i,v in enumerate(df[col]):
            if v.find('億') != -1:
                oku,man = v.split('億')
                if man == '':
                    man = 0
                else:
                    man = man[:-1]
                price.append(int(oku)*10000+int(man))
            elif v.find('ヶ月') != -1:
                p = df.loc[i,'price']
                m = float(v[:v.find('ヶ月')])
                price.append(p * m)
            elif v.find('万') != -1:
                man,sen = v.split('万')
                if sen == '':
                    sen = 0
                price.append(int(man) + int(sen) / 10000)
            else:
                price.append(v)
        df[col] = price
        return df

    def pre_insurance(self, df):
        col = '保険'
        ins = []
        for i,v in enumerate(df[col]):
            if v.find('有') != -1:
                ins.append('有')
            else:
                ins.append('無')
        df[col] = ins
        return df

    def pre_timing(self, df):
        col = '入居時期'
        timing = []
        for i,v in enumerate(df[col]):
            if (v.find('相談') == -1) & (v.find('即時') == -1) :
                timing.append('その他')
            else:
                timing.append(v)
        df[col] = timing
        return df

    def pre_term(self, df):
        col = '契約期間'
        term = []
        df[col] = df[col].str.replace('期間:','')
        for i,v in enumerate(df[col]):
            if (v.find('年') != -1) & (v.find('ヶ月') != -1) :
                year, month = v.split('年')
                month = month[:-2]
                term.append(float(year) + float(month)/12)
            elif (v.find('年') != -1):
                term.append(float(v[:-1]))
            elif (v.find('ヶ月') != -1):
                term.append(float(v[:-2])/12)
            else:
                term.append(0)
        df[col] = term
        return df

    def pre_area(self, df):
        area=[]
        for i in df['専有面積']:
            area.append(i[:i.find('㎡')])
        df['専有面積']=pd.DataFrame(area)
        return df

    def pre_zipcode(self, df):
        with open('supplemental_data/zipcode_dictionary.pickle','rb') as f:
            zip_dict=pickle.load(f)

        address = []
        for i,v in enumerate(df['所在地']):
            if v.find('丁目'):
                address.append(v[:v.find('丁目')-1])
            else:
                address.append(v)

        df['住所'] = address
        df['郵便番号'] = df['住所'].map(zip_dict)
        df = df.drop(['所在地', '住所'], axis = 1)
        return df

    def pre_dep_key(self, df):
        col = '敷金・礼金'
        dep = []
        key = []
        df[col] = df[col].str.replace('円','')
        df[col] = df[col].str.replace(',','')
        for i,v in enumerate(df[col]):
            dep_i, key_i = v.split('・')
            if (dep_i.find('敷金なし') != -1) | (dep_i.find('-') != -1) :
                dep.append(0)
            elif (dep_i.find('ヶ月') != -1):
                d_p = df.loc[i,'price']
                d_m = float(dep_i[:dep_i.find('ヶ月')])
                dep.append(d_p * d_m)
            elif dep_i.find('万') != -1:
                man, sen = dep_i.split('万')
                if sen == '':
                    sen = 0
                dep.append(int(man) + int(sen) / 10000)
            else:
                dep.append(int(dep_i))

            if (key_i.find('礼金なし') != -1) | (key_i.find('-') != -1) :
                key.append(0)
            elif (key_i.find('ヶ月') != -1):
                k_p = df.loc[i,'price']
                k_m = float(key_i[:key_i.find('ヶ月')])
                key.append(k_p * k_m)
            elif key_i.find('万') != -1:
                man, sen = key_i.split('万')
                if sen == '':
                    sen = 0
                key.append(int(man) + int(sen) / 10000)
            else:
                key.append(int(key_i))

        df['敷金'] = dep
        df['礼金'] = key
        df = df.drop(col, axis = 1)
        return df

    def pre_renewal(self, df):
        col = '更新料'
        df[col] = df[col].str.replace('円','')
        df[col] = df[col].str.replace(',','')
        renewal = []
        for i,v in enumerate(df[col]):
            if (v.find('ヶ月') != -1):
                p = df.loc[i,'price']
                m = float(v[:v.find('ヶ月')])
                renewal.append(p * m)
            elif (v.find('-') != -1):
                renewal.append(0)
            elif v.find('億') != -1:
                oku,man = v.split('億')
                if man == '':
                    man = 0
                else:
                    man = man[:-1]
                renewal.append(int(oku)*100000000+int(man))
            elif v.find('万') != -1:
                man, sen = v.split('万')
                if sen == '':
                    sen = 0
                renewal.append(int(man) + int(sen) / 10000)
            else:
                renewal.append(int(v))
        df[col] = renewal
        return df

    def pre_structure(self, df):
        col = '構造'
        new = []
        for i,v in enumerate(df[col]):
            if len(v.split())==0:
                new.append('-')
            else:
                new.append(v)
        df[col] = new
        df[col]=df[col].str.replace('-','その他')
        df[col]=df[col].str.replace('ブロック造','その他')
        df[col]=df[col].str.replace('プレキャストコンクリート（ＰＣ）','その他')
        df[col]=df[col].str.replace('鉄骨プレキャストコンクリート（ＨＰＣ）','その他')
        df[col]=df[col].str.replace('木造','その他')
        df[col]=df[col].str.replace('軽量鉄骨','その他')
        df[col]=df[col].str.replace('ＡＬＣ','鉄骨造')
        return df

    def pre_state(self, df):
        col = '現況'
        df[col]=df[col].str.replace('-','不明')
        return df

    def pre_mcost(self, df):
        col = '管理費等'
        df[col]=df[col].str.replace(',','')
        df[col]=df[col].str.replace('円','')

        kanri = []
        kyoeki = []
        for i,v in enumerate(df[col]):
            if (v.find('管理費:') != -1) & (v.find('共益費:') != -1):
                kan, kyo = v.split('共益費:')
                if kan[4:] == 'なし':
                    kanri.append(0)
                else:
                    if kan[4:].find('万')!= -1:
                        man, sen=kan[4:].split('万')
                        if sen=='':
                            sen=0
                        kanri.append(int(man)*10000 + int(sen))
                    else:
                        kanri.append(int(kan[4:]))

                if kyo == 'なし':
                    kyoeki.append(0)
                else:
                    if kyo.find('万')!= -1:
                        man, sen=kyo.split('万')
                        if sen=='':
                            sen=0
                        kyoeki.append(int(man)*10000 + int(sen))
                    else:
                        kyoeki.append(int(kyo))

            elif (v.find('管理費:') != -1) & (v.find('共益費:') == -1):
                kyoeki.append(0)
                if v[4:] == 'なし':
                    kanri.append(0)
                else:
                    if v[4:].find('万')!=-1:
                        man, sen=v[4:].split('万')
                        if sen=='':
                            sen=0
                        kanri.append(int(man)*10000 + int(sen))
                    else:
                        kanri.append(int(v[4:]))

            elif (v.find('管理費:') == -1) & (v.find('共益費:') != -1):
                kanri.append(0)
                if v[4:] == 'なし':
                    kyoeki.append(0)
                else:
                    if v[4:].find('万')!=-1:
                        man, sen=v[4:].split('万')
                        if sen=='':
                            sen=0
                        kyoeki.append(int(man)*10000 + int(sen))
                    else:
                        kyoeki.append(int(v[4:]))

            elif (v.find('管理費:') == -1) & (v.find('共益費:') == -1):
                kanri.append(0)
                kyoeki.append(0)

        df['管理費'] = kanri
        df['共益費'] = kyoeki
        df[col] = df['管理費'] + df['共益費']
        df = df.drop(['管理費','共益費'],axis = 1)
        return df

    def pre_age(self, df):
        this_year = datetime.now().strftime("%Y")
        df=df.reset_index(drop=True)
        year=[]
        for i in df['築年月']:
            year.append(int(this_year)-int(i[:i.find('年')]))
        df['築年月']=year
        df=df.rename(columns={'築年月':'築年数'})
        return df

    def pre_floor(self, df):
        df['階数']=df['階数'].str.replace('地下','-')
        df['階数']=df['階数'].str.replace('地上','')
        floor=[]
        for i in df['階数']:
            floor.append(i[:i.find('階')])
        df['階数']=floor
        return df

    def encode_cat_to_label(self, df):
        mode = 'learning'
        if self.mode=='inference':
            with open(self.encoder_path,'rb') as f:
                labels=pickle.load(f)
        elif self.mode=='learning':
            labels=self.get_encoder(spec)
            self.save_encoder(labels)
        for key in labels.keys():
            spec[key]=labels[key].transform(spec[key])
        return spec

    def get_encoder(self, df):
        le_insurance = preprocessing.LabelEncoder()
        le_insurance.fit(spec['保険'])
        le_timing = preprocessing.LabelEncoder()
        le_timing.fit(spec['入居時期'])
        le_type = preprocessing.LabelEncoder()
        le_type.fit(spec['取引態様'])
        le_direction = preprocessing.LabelEncoder()
        le_direction.fit(spec['向き'])
        le_structure = preprocessing.LabelEncoder()
        le_structure.fit(spec['構造'])
        le_cat = preprocessing.LabelEncoder()
        le_cat.fit(spec['物件種目'])
        le_state = preprocessing.LabelEncoder()
        le_state.fit(spec['現況'])
        le_layout = preprocessing.LabelEncoder()
        le_layout.fit(spec['間取り'])
        le_way = preprocessing.LabelEncoder()
        le_way.fit(spec['駅からの手段'])
        le_station = preprocessing.LabelEncoder()
        le_station.fit(spec['最寄り駅'])
        labels={
            '保険': le_insurance,
            '入居時期': le_timing,
            '取引態様': le_type,
            '向き': le_direction,
            '構造':le_structure,
            '物件種目':le_cat,
            '現況':le_state,
            '間取り':le_layout,
            '駅からの手段':le_way,
            '最寄り駅':le_station
           }
        return labels

    def save_encoder(self,labels):
#         print(self.encoder_path)
        with open(self.encoder_path,'wb') as f:
            pickle.dump(labels, f)

    def save_spec(self,spec):
        with open(self.save_path,'wb') as f:
            pickle.dump(spec, f)

if __name__ == '__main__':
    mode='learning'
    data_version='20180808'
    encoder_version='20180808'
    prep=PreprocessRent(mode, data_version, encoder_version)
    spec = prep.load_spec()
    spec = prep.pre_price(spec)
    spec = prep.pre_bid(spec)
    spec = prep.pre_balcony(spec)
    spec = prep.pre_transportation(spec)
    spec = prep.pre_deposit(spec)
    spec = prep.pre_insurance(spec)
    spec = prep.pre_timing(spec)
    spec = prep.pre_term(spec)
    spec = prep.pre_area(spec)
    spec = prep.pre_zipcode(spec)
    spec = prep.pre_dep_key(spec)
    spec = prep.pre_renewal(spec)
    spec = prep.pre_structure(spec)
    spec = prep.pre_state(spec)
    spec = prep.pre_mcost(spec)
    spec = prep.pre_age(spec)
    spec = prep.pre_floor(spec)
    spec = prep.encode_cat_to_label(spec)        
#     if mode =='learning':
#         spec=prep.pre_outlier(spec)
#         spec=prep.del_bid(spec)
    prep.save_spec(spec)