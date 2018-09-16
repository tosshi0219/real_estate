import os

class Status:
    def __init__(self):
        self.model_path='model'
        self.label_path='label'
        self.data_path='intermediate_data'
        self.result_path='result'
        self.model_files = os.listdir(self.model_path)
        self.label_files = os.listdir(self.label_path)
        self.data_files = os.listdir(self.data_path)
        self.result_files = os.listdir(self.result_path)

    def model_version(self):
        models=[f[:-7] for f in self.model_files if f.find('pickle')!=-1]
        models.sort()
        return models

    def label_version(self):
        labels=[f[:-7] for f in self.label_files if f.find('pickle')!=-1]
        labels.sort()
        return labels

    def spec_version(self):
        spec_data=[f[:-7] for f in self.data_files if (f.find('pickle')!=-1)&(f[:4]=='spec')]
        spec_data.sort()
        return spec_data    

    def inference_version(self):
        inference_data=[f[:-7] for f in self.data_files if (f.find('pickle')!=-1)&(f[:4]=='prep')&(f.find('inference')!=-1)]
        inference_data.sort()
        return inference_data

    def learning_version(self):
        learning_data=[f[:-7] for f in self.data_files if (f.find('pickle')!=-1)&(f[:4]=='prep')&(f.find('learning')!=-1)]
        learning_data.sort()
        return learning_data

    def result_version(self):
        result_data=[f[:-7] for f in self.result_files if (f.find('pickle')!=-1)]
        result_data.sort()
        return result_data

if __name__ == '__main__':
    status=Status()
    models=status.model_version()
    labels=status.label_version()
    spec_data=status.spec_version()
    inference_data=status.inference_version()
    learning_data=status.learning_version()
    result_data=status.result_version()
    print('model=',models)
    print('label=',labels)
    print('spec=',spec_data)
    print('inference=',inference_data)
    print('learning=',learning_data)
    print('result=',result_data)    