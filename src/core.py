from get_spec import GetSpec
from preprocess import Preprocess
from inference import Inference
from learning import Learning
from postprocess import Postprocess,Email
from datetime import datetime

mode=input('which mode do you use "learning" or "inference"?')

if mode == 'learning':
    data_version=input('which data do you learn? ex.new, original, 20180722 etc.')
    if data_version=='new':
        #新たにspec取得
        data_version=datetime.now().strftime("%Y%m%d")
        get_spec=GetSpec()
        page_num=get_spec.get_page_num()
        urls=get_spec.get_urls(page_num)
        pages=get_spec.get_pages(urls)
        get_spec.save_pages(pages)
        bodys=get_spec.get_bodys(pages)
        bids=get_spec.get_bids(bodys)  
        bids=get_spec.delete_duplication(bids)    
        links=get_spec.get_links(bids)
        link_pages=get_spec.get_link_pages(links)
        get_spec.save_link_pages(link_pages)
        link_bodys=get_spec.get_link_bodys(link_pages)
        specs=get_spec.get_specs(link_bodys)
        get_spec.save_specs(specs)
        get_spec.output(specs)

    #preprocess
    prep=Preprocess(mode)
    spec=prep.load_spec()
    spec=prep.pre_price(spec)
    spec=prep.pre_bid(spec)
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
    spec=prep.pre_outlier(spec)
    spec=prep.del_bid(spec)
    prep.save_spec(spec)

    #learning
    learning=Learning(data_version)
    gs=learning.xgb_learning(spec)
    learning.show_results(gs)
    learning.save_model(gs)
#     spec_all,spec_bad=learning.check_prediction(spec)
#     learning.save_prediction(spec_all)

elif mode == 'inference':
    data_version=datetime.now().strftime("%Y%m%d")
    model_version=input('which model do you use? ex.original...')
    #spec取得
    get_spec=GetSpec(data_version)
    page_num=get_spec.get_page_num()
    urls=get_spec.get_urls(page_num)
    pages=get_spec.get_pages(urls)
    get_spec.save_pages(pages)
#     load_date='original'
#     pages=get_spec.load_pages(load_date)
    bodys=get_spec.get_bodys(pages)
    bids=get_spec.get_bids(bodys)  
    bids=get_spec.delete_duplication(bids)    
    links=get_spec.get_links(bids)
    link_pages=get_spec.get_link_pages(links)
    get_spec.save_link_pages(link_pages)
#     link_pages=get_spec.load_link_pages(load_date)
    link_bodys=get_spec.get_link_bodys(link_pages)
    specs=get_spec.get_specs(link_bodys)    
    get_spec.save_specs(specs)
#     specs=get_spec.load_specs(load_date)
    get_spec.output(specs)

    #preprocess        
    prep = Preprocess(mode,data_version,model_version)
    spec=prep.load_spec()
    spec=prep.pre_price(spec)
    spec=prep.pre_bid(spec)
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
    prep.save_spec(spec)    

    #inference
    inf=Inference(data_version,model_version)
    spec=inf.load_prep_spec()
    treasure=inf.inference(spec)
    inf.save_treasure(treasure)

    #postprocess
    post=Postprocess(data_version,model_version)
    spec=post.load_result()
    spec=post.decode(spec)
    body=post.urls(spec)
    em=Email()
    msg=em.create_message(body)
    em.send(msg)

else:
    print('input "learning" or "inference"')