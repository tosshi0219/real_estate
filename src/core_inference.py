from crawler import Crawler
from preprocess_purchase import PreprocessPurchase
from inference import Inference
from learning_purchase import Learning
from postprocess import Postprocess,Email
from datetime import datetime

data_version = datetime.now().strftime("%Y%m%d")
load_date = data_version
target = 'purchase'
mode='inference'
encoder_version='original'
model_version='original'

#crawling
crawler = Crawler(data_version, target)
#preprocess
prep=PreprocessPurchase(mode,data_version,encoder_version)
#inference
inf=Inference(data_version,model_version)
#postprocess
post=Postprocess(data_version,model_version)
body=post.urls()
em=Email()
msg=em.create_message(body)
em.send(msg)
