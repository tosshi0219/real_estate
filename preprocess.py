import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from sklearn import preprocessing
import pickle
from config import dropcol

class Preprocess:
    def __init__(self,mode,data_version,encoder_version):
        self.mode=mode
        if mode=='inference':
            self.data_version=data_version #推論対象となるデータセットのversion
            self.encoder_version=encoder_version #モデルのversion
            self.encoder_path='label/label_encoder_{}.pickle'.format(self.encoder_version)
            self.save_path='intermediate_data/preprocessed_spec_for_inference_{}.pickle'.format(self.data_version)

        elif mode=='learning':
            self.data_version=data_version  #学習データセットのversion
            self.encoder_version=self.data_version
            self.encoder_path='label/label_encoder_{}.pickle'.format(self.encoder_version)
            self.save_path='intermediate_data/preprocessed_spec_for_learning_{}.pickle'.format(self.data_version)

#         self.load_path='intermediate_data/spec_{}.csv'.format(self.data_version)
        self.load_path='intermediate_data/spec_{}.pickle'.format(self.data_version)

    def load_spec(self):
        with open(self.load_path,'rb') as f:
            spec=pickle.load(f)
#         spec=pd.read_csv(self.load_path)
#         spec=spec.drop(spec.columns[0],axis=1)
#         spec=spec.rename(columns={spec.columns[1]:'詳細情報'})
        spec=spec.drop(dropcol,axis=1)
        return spec

    def pre_price(self,spec):
        spec=spec[~spec['price'].isnull()]
        spec=spec.reset_index(drop=True)
        spec['price']=spec['price'].astype('str')
        spec['price']=spec['price'].str.replace(',','')
        price=[]
        for i in spec['price']:
            if i.find('億')!=-1:
                oku,man=i.split('億')
                if man=='':
                    man=0
                else:
                    man=man[:-1]
                price.append(int(oku)*10000+int(man))
            else:
                if i.find('万')!=-1:
                    man,sen=i.split('万')
                    if sen=='':
                        sen=0
                    price.append(int(man)+int(sen)/10000)
                else:
                    price.append(int(i))
        spec['price']=price
        return spec

    def pre_bid(self,spec):
        spec=spec.rename(columns={spec.columns[1]: '詳細情報'})
        spec=spec[~spec['詳細情報'].isnull()]
        spec=spec.reset_index(drop=True)
        bid=[]
        for i,v in enumerate(spec['詳細情報']):
            a,b=v.split('／')
            bid.append(a[len('物件番号:'):])
        spec['詳細情報']=pd.DataFrame(bid)
        return spec

    def pre_loc(self,spec):
        city=[]
        address=[]
        for s in spec['所在地']:
            r1=s[s.find('都')+1:]
            if r1.find('区')!=-1:
                city.append(r1[:r1.find('区')])
                r2=r1[r1.find('区')+1:]
            elif  r1.find('市')!=-1:
                city.append(r1[:r1.find('市')])
                r2=r1[r1.find('市')+1:]
            elif  r1.find('郡')!=-1:
                city.append(r1[:r1.find('郡')])
                r2=r1[r1.find('郡')+1:]
            else:
                print('想定外')
            address.append(r2)
        spec['city']=pd.DataFrame(city)
        spec['address']=pd.DataFrame(address)
        spec=spec.drop('所在地',axis=1)
        return spec

    def pre_transportation(self,spec):
        station=[]
        method=[]
        time=[]
        for i,s in enumerate(spec['交通']):
            f1=s.find('「')
            r1=s.find('」')
            station.append(s[f1+1:r1])
            rest=s[r1:]
            r2=rest.find('分')
            s1=s[r1:r1+r2]
            if s1.find('徒歩')!=-1:
                method.append('徒歩')
                time.append(int(s1[s1.find('徒歩')+2:]))
            elif (s1.find('バス')!=-1)&(s1.find('バスにて')==-1):
                method.append('バス')
                time.append(int(s1[s1.find('バス')+2:]))
            elif s1.find('バスにて')!=-1:
                method.append('バス')
                time.append(int(s1[s1.find('バスにて')+4:]))
            else:
                method.append(np.nan)
                time.append(np.nan)

        spec=spec.drop('交通',axis=1)
        spec['最寄り駅']=pd.DataFrame(station)
        spec['駅からの手段']=pd.DataFrame(method)
        spec['駅からの時間']=pd.DataFrame(time)

        spec=spec.dropna()
        spec=spec.reset_index(drop=True)

        return spec

    def pre_land(self,spec):
        spec=spec.rename(columns={'㎡／坪単価':'平米単価'})
        spec['平米単価']=spec['平米単価'].str.replace(',','')

        land=[]
        for i in spec['平米単価']:
            buf=i[:i.find('円/㎡')]
            if buf.find('万')==-1:
                land.append(0)
            else:
                man,sen=buf.split('万')
                if sen=='':
                    sen=0
                    land.append(int(man)*10000+int(sen))
                else:
                    land.append(int(man)*10000+int(sen))

        spec['平米単価']=pd.DataFrame(land)
        return spec

    def pre_area(self,spec):
        area=[]
        for i in spec['専有面積']:
            area.append(i[:i.find('㎡')])
        spec['専有面積']=pd.DataFrame(area)
        return spec

    def pre_structure(self,spec):
        spec['構造']=spec['構造'].fillna('-')
        spec['構造']=spec['構造'].str.replace('-','その他')
        spec['構造']=spec['構造'].str.replace('ブロック造','その他')
        spec['構造']=spec['構造'].str.replace('プレキャストコンクリート（ＰＣ）','その他')
        spec['構造']=spec['構造'].str.replace('木造','その他')
        return spec

    def pre_age(self,spec):
        spec=spec[spec['築年月']!='1657年6月']
        spec=spec.reset_index(drop=True)
        year=[]
        for i in spec['築年月']:
            year.append(2018-int(i[:i.find('年')]))
        spec['築年月']=year
        spec=spec.rename(columns={'築年月':'築年数'})
        return spec

    def pre_floor(self,spec):
        spec['階数']=spec['階数'].str.replace('地下','-')
        spec['階数']=spec['階数'].str.replace('地上','')
        floor=[]
        for i in spec['階数']:
            floor.append(i[:i.find('階')])
        spec['階数']=floor
        return spec

    def pre_direction(self,spec):
        spec=spec[spec['向き']!='-']
        spec=spec.reset_index(drop=True)
        return spec

    def pre_mcost(self,spec):
        spec['管理費等']=spec['管理費等'].str.replace(',','')
        spec['管理費等']=spec['管理費等'].str.replace('円','')
        spec['管理費等']=spec['管理費等'].str.replace('-','0')
        spec['管理費等']=spec['管理費等'].str.replace('管理費なし','0')
        mcost=[]
        for i in spec['管理費等']:
            if i.find('万')!=-1:
                man,sen=i.split('万')
                if sen=='':
                    sen=0
                mcost.append(int(man)*10000+int(sen))
            else:
                mcost.append(int(i))
        spec['管理費等']=mcost
        return spec

    def pre_rcost(self,spec):
        spec['修繕積立金']=spec['修繕積立金'].str.replace(',','')
        spec['修繕積立金']=spec['修繕積立金'].str.replace('円','')
        spec['修繕積立金']=spec['修繕積立金'].str.replace('-','0')
        spec['修繕積立金']=spec['修繕積立金'].str.replace('積立金なし','0')
        rcost=[]
        for i in spec['修繕積立金']:
            if i.find('万')!=-1:
                man,sen=i.split('万')
                if sen=='':
                    sen=0
                rcost.append(int(man)*10000+int(sen))
            else:
                rcost.append(int(i))
        spec['修繕積立金']=rcost
        return spec

    def encode_cat_to_label(self,spec):
        if self.mode=='inference':
            with open(self.encoder_path,'rb') as f:
                labels=pickle.load(f)
        elif self.mode=='learning':
            labels=self.get_encoder(spec)
            self.save_encoder(labels)
        for key in labels.keys():
            spec[key]=labels[key].transform(spec[key])
        return spec

    def get_encoder(self,spec):
        le_station = preprocessing.LabelEncoder()
        le_station.fit(spec['最寄り駅'])
        le_way = preprocessing.LabelEncoder()
        le_way.fit(spec['駅からの手段'])
        le_layout = preprocessing.LabelEncoder()
        le_layout.fit(spec['間取り'])
        le_structure = preprocessing.LabelEncoder()
        le_structure.fit(spec['構造'])
        le_direction = preprocessing.LabelEncoder()
        le_direction.fit(spec['向き'])
        le_right = preprocessing.LabelEncoder()
        le_right.fit(spec['敷地の権利形態'])
        le_type = preprocessing.LabelEncoder()
        le_type.fit(spec['取引態様'])
        labels={
            '間取り':le_layout,
            '構造':le_structure,
            '向き':le_direction,
            '敷地の権利形態':le_right,
            '取引態様':le_type,
            '最寄り駅':le_station,
            '駅からの手段':le_way
           }
        return labels

    def save_encoder(self,labels):
#         print(self.encoder_path)
        with open(self.encoder_path,'wb') as f:
            pickle.dump(labels, f)

    def pre_outlier(self,spec):
        q1 = spec.quantile(.25)
        q3 = spec.quantile(.75)
        iqr = q3 - q1
        bias=1.5
        outlier_min=100
        # outlier_min = (q1 - (iqr) * bias)['price']
        outlier_max = (q3 + (iqr) * bias)['price']
        spec=spec[(spec['price']>outlier_min)&(spec['price']<outlier_max)]

        #専有面積1000以上,1以下を消去
        spec['専有面積']=spec['専有面積'].astype(float)
        spec=spec[spec['専有面積']<1000]
        spec=spec[spec['専有面積']>1.1]
        return spec

    def del_bid(self,spec):
        spec=spec.reset_index(drop=True)
        return spec.drop('詳細情報',axis=1)

    def save_spec(self,spec):
#         print(self.save_path)
        with open(self.save_path,'wb') as f:
            pickle.dump(spec, f)
#         spec.to_csv(self.save_path)

if __name__ == '__main__':
    mode='inference'
    data_version='original'
    encoder_version='original'
    prep=Preprocess(mode,data_version,encoder_version)
    spec=prep.load_spec()
    spec=prep.pre_price(spec)
    spec=prep.pre_bid(spec)
#     spec=prep.pre_loc(spec)
    spec=prep.pre_transportation(spec)
    spec=prep.pre_land(spec)
    spec=prep.pre_area(spec)
    spec=prep.pre_structure(spec)
    spec=prep.pre_age(spec)
    spec=prep.pre_floor(spec)
    spec=prep.pre_direction(spec)
    spec=prep.pre_mcost(spec)
    spec=prep.pre_rcost(spec)
    spec=prep.encode_cat_to_label(spec)
    if mode =='learning':
        spec=prep.pre_outlier(spec)
        spec=prep.del_bid(spec)
    prep.save_spec(spec)