import pandas as pd
import numpy as np
from datetime import datetime
from sklearn import preprocessing
import pickle
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate

class Postprocess:
    def __init__(self,data_version,encoder_version):
        self.data_version=data_version
        self.load_path='result/result_{}.pickle'.format(self.data_version)
        self.encoder_version=encoder_version
        self.encoder_path='label/label_encoder_{}.pickle'.format(self.encoder_version)
        self.bid_url='http://www.fudousan.or.jp/system/?act=d&type=12&pref=13&n=20&p=1&v=on&s=&bid='
        
    def load_result(self):
        with open(self.load_path,'rb') as f:
            spec=pickle.load(f)
        spec=spec.reset_index(drop=True)
        return spec
        
    def decode(self,spec):
        with open(self.encoder_path,'rb') as f:
            labels=pickle.load(f)
        for key in labels.keys():
            spec[key]=labels[key].inverse_transform(spec[key].astype(int))
        return spec

    def urls(self,spec):
        body='以下のリンクの物件はお得な物件の可能性があります\n'
        body=body+'対象物件数:{}件\n'.format(spec.shape[0])
        for i in range(spec.shape[0]):
            row0='**********************************'
            row1='bit番号:{}'.format(spec['詳細情報'][i])
            row2='掲載価格:{}万円'.format(str(int(spec['price'][i])))
            row3='予測価格:{}万円'.format(str(int(spec['prediction'][i])))
            row4='誤差割合:{}%'.format(str(int(spec['error_percent'][i])))
            row5='URL:'+self.bid_url+str(int(spec['詳細情報'][i]))
            body=body+row0+'\n'+row1+'\n'+row2+'\n'+row3+'\n'+row4+'\n'+row5+'\n'+row0+'\n'
        print(body)
        return body

class Email:
    def __init__(self):        
        self.from_addr = 'toshiomiyamoto5555@gmail.com'
        self.mypass = 'Antarctica@1040'
        self.to_addr = 'toshiomiyamoto0219@yahoo.co.jp'
        # BCC = 'xxxx'
        self.subject = '不動産ジャパンお得物件情報'

    def create_message(self,body):
        msg = MIMEText(body)
        msg['Subject'] = self.subject
        msg['From'] = self.from_addr
        msg['To'] = self.to_addr
    #     msg['Bcc'] = bcc_addrs
        msg['Date'] = formatdate()
        return msg

    def send(self,msg):
        smtpobj = smtplib.SMTP('smtp.gmail.com', 587)
        smtpobj.ehlo()
        smtpobj.starttls()
        smtpobj.ehlo()
        smtpobj.login(self.from_addr,self.mypass)
        smtpobj.sendmail(self.from_addr, self.to_addr, msg.as_string())
        smtpobj.close()

if __name__ == '__main__':
    data_version='original'
    model_version='original' #基本的にencoder_versionはmodel_versionと同じ
    post=Postprocess(data_version,model_version)
    spec=post.load_result()
    spec=post.decode(spec)
    spec=spec[spec['price']>10]
    spec=spec.reset_index(drop=True)
    body=post.urls(spec)
    em=Email()
    msg=em.create_message(body)
    em.send(msg)