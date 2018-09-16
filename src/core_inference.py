import sys
sys.path.append('/Users/toshio/project/real_estate')
from src.crawler import Crawler
from src.preprocess_purchase import PreprocessPurchase
from src.inference import Inference
from src.learning_purchase import Learning
from src.postprocess import Postprocess,Email
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