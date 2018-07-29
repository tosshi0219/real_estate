# libraries
import numpy as np
import pandas as pd
import xgboost as xgb
import pickle
from datetime import datetime

class Inference():
    def __init__(self,data_version,model_version):
        self.data_version=data_version
        self.load_path='intermediate_data/preprocessed_spec_for_inference_{}.pickle'.format(self.data_version)
        self.model_version=model_version
        self.model_path='model/model_xgb_{}.pickle'.format(self.model_version)
        self.save_path='result/result_{}.pickle'.format(self.data_version)
        
    def load_prep_spec(self):
        with open(self.load_path,'rb') as f:
            spec=pickle.load(f)
        return spec

    def load_model(self):
        with open(self.model_path,'rb') as f:
            model=pickle.load(f)
        return model
    
    def inference(self,spec):
        #データの前処理
        X = spec.drop(['price','詳細情報'], axis=1).as_matrix()
        y = spec['price']
        indices=spec.index

        #モデルのロード
        xgb=self.load_model()

        #推論
        prediction=xgb.predict(X)
        
        #結果の比較
        error=y-prediction
        error_per=abs(error)/y*100
        result=pd.DataFrame({
                            'prediction':prediction,
                            'error':error,
                            'error_percent':error_per,
                            },index=indices)
        result_all=pd.concat([spec,result],axis=1)
        treasure=result_all[(result_all['error_percent']>5)&(result_all['error']<0)]
        
#         #結果を出力
        print(result_all)
        return treasure
 
    def save_treasure(self,treasure):
#         print(self.save_path)
        with open(self.save_path,'wb') as f:
            pickle.dump(treasure, f)

if __name__ == '__main__':
    data_version='original'
    model_version='original'
    inf=Inference(data_version,model_version)
    spec=inf.load_prep_spec()
    treasure=inf.inference(spec)
    inf.save_treasure(treasure)