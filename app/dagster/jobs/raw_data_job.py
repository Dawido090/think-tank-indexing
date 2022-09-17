from dagster import get_dagster_logger, job, op, mem_io_manager, in_process_executor
from requests_html import HTMLSession
from minio import Minio, error
import datetime
import time
import os
import json
import shutil
import requests
import re
import io

@op
def get_blogs():
    # think about multi blogs loop
    blogs = ['https://www.cfr.org/blog/asia-unbound',
            'https://www.cfr.org/blog/africa-transition'
    ]
    get_dagster_logger().info(blogs)
    return blogs

@op
def get_already_in_raw():
    client = get_minio_client()
    if check_minio_connection(client) == False:
        get_dagster_logger().error('Minio is down!')
    if client.bucket_exists("articles") == False:
        return None
    possible_list = [x.object_name.strip('.json').split('/')[1] for x in client.list_objects("articles",prefix = "raw-data/")]
    get_dagster_logger().info('\t'.join([x for x in possible_list]))
    if possible_list == 0:
        return None
    elif len(possible_list) >= 1:
        return possible_list
    


@op
def get_links(blogs, already_in_raw):
    result = []
    # check creds in future
    for blog in blogs:
        base_page = blog
        # implement stop of program after certain time if isn't able to find new data in future
        flag = True
        page_num = 1
        while (len(result) >= 30) and (flag == True):
            headers ={
                    "accept": "application/json, text/javascript, */*; q=0.01",
                    "accept-language": "en-GB,en;q=0.9,pl;q=0.8,en-US;q=0.7",
                    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjI0NTYxNjAiLCJhcCI6IjM0NTY3NTgwMyIsImlkIjoiNjc5ZWRlOThiMDAzMjE0MiIsInRyIjoiYTkxODUxODhhYmRkMzM1N2Q0NzM1YjM5MjQ3MTM4YmMiLCJ0aSI6MTY2MzQyOTEwMjQ4MSwidGsiOiI2NjY4NiJ9fQ==",
                    "sec-ch-ua": "\"Microsoft Edge\";v=\"105\", \" Not;A Brand\";v=\"99\", \"Chromium\";v=\"105\"",
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": "\"Windows\"",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "traceparent": "00-a9185188abdd3357d4735b39247138bc-679ede98b0032142-01",
                    "tracestate": "66686@nr=0-1-2456160-345675803-679ede98b0032142----1663429102481",
                    "x-newrelic-id": "VgICV1dVCBADUFNSBwkCX1M=",
                    "x-requested-with": "XMLHttpRequest",
                    "cookie": "_cb=C3VoU5C2o5cYmR7OP; _fbp=fb.1.1659107102835.468199250; cookie-agreed=2; _hjSessionUser_1768366=eyJpZCI6IjE4NTJkNmM1LWMzOTAtNTYzMC04NjlhLTBiM2JiMDg2ZWExNCIsImNyZWF0ZWQiOjE2NTkxMDcxMDI5NjIsImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.209379316.1662914268; _dc_gtm_UA-3596942-1=1; _hjIncludedInSessionSample=0; _hjSession_1768366=eyJpZCI6ImFjZGE5Yzg4LTZmOGUtNDgyMS1hMGUyLTlhYjZhMWFlYzY0NSIsImNyZWF0ZWQiOjE2NjM0MjkwNDQ2NjEsImluU2FtcGxlIjpmYWxzZX0=; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0; _chartbeat2=.1659107102163.1663429085053.0000000001111111.BcT2dxDfWnWz3nKVgC548VDBX34c_.1; _cb_svref=null; amp_2be1ae=kqJ8mfrNA3YX_SXS4cIY6F...1gd61hps7.1gd61j1ta.7p.0.7p; _ga=GA1.2.15477803.1659107103; sailthru_pageviews=8; sailthru_content=38036bc56a0e62c97d78e56b829b03904b3f625bda7c6096eccc30092fa2a9ff173f3875ca7f38c6c3fc17feeee42b23df2442e3d1a933f6b952ca5eccdc1154dd06b0fd15773ca12b66bfa036f37b9f299173f1f5c606c2cf70a699da89e6cbdcb04b9a318e83ab02959872f45748133135555ea904fe10f803730c02d66922dd2d2502ddcc667a3fa7b723dbe2287168556862283f56ebbd17761a7aa365b2cff8804e237f6cdf3a085c14986cbc99143f360e12472f43f8144249faf8a54923dc42ffd45adfe5ee0f41ffe597f6164806c15541b01bd1e67e3f8f13a1e41afa681288dcbd8752070f837e74a0a6df5293a3eac0d13f9847e43c5e382fc2d0; sailthru_visitor=586ad90b-1cf6-45ce-adf9-9e8108a1bd32; _ga_24W5E70YKH=GS1.1.1663429044.32.1.1663429093.0.0.0; _ga_RBVKHZHP8E=GS1.1.1663429044.32.1.1663429093.11.0.0; _chartbeat5=524|4485|%2Fblog%2Fasia-unbound|https%3A%2F%2Fwww.cfr.org%2Fblog%2Fasia-unbound%3F_wrapper_format%3Dhtml%26topics%3DAll%26regions%3DAll%26page%3D1|lqlc9ByeQe4C6Ovp0WcITUC89s4y||c|BxNaC6Baul6wB72DEUCKWW08DlTVRR|cfr.org|",
                    "Referer": "https://www.cfr.org/blog/asia-unbound",
                    "Referrer-Policy": "strict-origin-when-cross-origin"
                    }
            post_data = F"view_name=blog_posts&view_display_id=block_latest_blog_posts&view_args=4%2F242728&view_path=%2Ftaxonomy%2Fterm%2F4&view_base_path=&view_dom_id=84192d5f83580205b355b4b8a16202628b8a30cebb844d05713c404973ebe7b1&pager_element=0&_wrapper_format=html&topics=All&regions=All&page={page_num}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=cfr_theme&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=cfr_chartbeat%2Fcfr-chartbeat%2Ccfr_homepage_sections%2Fadvanced_autocomplete%2Ccfr_sailthru%2Fjavascript_api_library%2Ccfr_theme%2Falert%2Ccfr_theme%2Fbg-image-switch%2Ccfr_theme%2Fblog-series%2Ccfr_theme%2Fbuttons%2Ccfr_theme%2Ffacebook-pixel%2Ccfr_theme%2Fglobal-legacy%2Ccfr_theme%2Fheader%2Ccfr_theme%2Fheader-contextual%2Ccfr_theme%2Fnewsletter-form%2Ccfr_theme%2Fnewsletter-form-common%2Ccfr_theme%2Fright-rail%2Ccore%2Fdrupal.ajax%2Ccore%2Fdrupal.autocomplete%2Ccore%2Finternal.jquery.form%2Cdatalayer%2Fbehaviors%2Cdatalayer%2Fhelper%2Cdd_datalayer_tools%2Famplitude%2Cdd_datalayer_tools%2FcustomDimensionUserCategory%2Cdd_datalayer_tools%2FdatalayerItems%2Cdd_datalayer_tools%2FpodcastFinish%2Cdd_datalayer_tools%2FpodcastStart%2Cdd_datalayer_tools%2FvideoFinish%2Cdd_datalayer_tools%2FvideoStart%2Ceu_cookie_compliance%2Feu_cookie_compliance_default%2Clazy%2Flazy%2Csearch_autocomplete%2Ftheme.minimal.css%2Csystem%2Fbase%2Cviews%2Fviews.ajax%2Cviews%2Fviews.module%2Cviews_infinite_scroll%2Fviews-infinite-scroll"
            post_request = requests.post("https://www.cfr.org/views/ajax?_wrapper_format=drupal_ajax",headers = headers, data = post_data )
            response = json.loads(post_request.content.decode())
            # get_dagster_logger().info(headers)
            # get_dagster_logger().info('\t'.join([x for x in already_in_raw]))
            data = response.json()[1]['data']
            find_links = list(set([x.strip() for x in re.findall(r'(?<=<a href=")(.*)(?=" class="card-article-large__link">)',data)]))
            # get_dagster_logger().info('\t'.join([x for x in find_links]))
            if already_in_raw != None:
                shorter = [x.split('/')[-1] for x in find_links]
                shorted_filtered = [x for x in shorter if x not in already_in_raw]
                find_links_filtered = ["https://www.cfr.org/blog/" + x for x in shorted_filtered]
                # get_dagster_logger().info('\t'.join([x for x in shorter]))
                # get_dagster_logger().info('\t'.join([x for x in shorted_filtered]))
                get_dagster_logger().info('\t'.join([x for x in find_links_filtered]))
            elif already_in_raw == None:
                find_links_filtered = ["https://www.cfr.org" + x for x in find_links]
            # get_dagster_logger().info('\t'.join([x for x in find_links_filtered]))
            
            for link in find_links_filtered:
                if link not in result:
                    result.append(link)
            page_num += 1
            get_dagster_logger().info(page_num)
            get_dagster_logger().info('\t'.join([x for x in result]))
    # get_dagster_logger().info('\t'.join([x for x in result]))
    return result


@op
def get_data(list_of_links):
    # collect raw data from portal
    # think about date zones
    result = []
    session = HTMLSession()
    for article_link in list_of_links:
        body_content = []
        ses = session.get(article_link)
        ses_html = ses.html
        body = ses_html.xpath('/html/body/div[1]/div/div/main/div/article/div/div/div[1]/div')
        body_p = body[0].find('p')
        subtitle = ses_html.xpath('/html/body/div[1]/div/div/main/div/article/div/header/div/div[2]')[0].text
        tag_blogs = [
            x.split('/')[-1].replace('-',' ') for x in
            ses_html.xpath('/html/body/div[1]/div/div/main/div/article/div/header/div/div[1]/section')[0].links]
        try:        
            post_date = ses_html.xpath('/html/body/div[1]/div/div/main/div/article/div/header/div/div[4]/div[2]')[0].text
            post_date = post_date.split('(')[0].strip()
            post_date = datetime.datetime.strptime(post_date,'%B %d, %Y %H:%M %p' )
        except (ValueError,IndexError) as error:
            get_dagster_logger().info(f'The {error} heppend at {article_link}')
            post_date = None

        data = {
            'link':article_link,
            'subtitle':subtitle,
            'blog':tag_blogs,
            'post date':post_date,
            'body':''
            }
        for i in body_p:
            if 'class' not in str(i) and i != '':
                body_content.append(i.text)
        data['body'] = body_content
        result.append(data)
    get_dagster_logger().info(f'Collection len:{len(result)}')
    # parsed_articles = '\t'.join([x for x in result])
    # get_dagster_logger().info(f'Parsed articles:{parsed_articles}')
    return result

def get_minio_client():
    Client = Minio(
        'minio-raw:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure = False
    )
    return Client

def check_minio_connection(client):
    try:
        if not client.bucket_exists("nonexistingbucket"):
            get_dagster_logger().info("Object storage connected")
            return True
    except:
        # raise error if storage not reachable
        get_dagster_logger().error("Object storage not reachable")
        return False


@op
def to_minio(data):
    client = get_minio_client()
    if check_minio_connection(client) == False:
        get_dagster_logger().error('Minio is down!')
    else:
        client.bucket_exists('articles')
        if client.bucket_exists('articles') == False:
           client.make_bucket('articles') 
        for count_of_files_added, file in enumerate(data):
            file_name = 'raw-data/'+ file['link'].split('/')[-1] +'.json'
            # client.fput_object('articles',object_name = 'raw-data/' + file,file_path=file)
            client.put_object('articles',file_name, data = io.BytesIO(str(file).encode()),length=len(str(file).encode()))
        get_dagster_logger().info(f'{count_of_files_added +1} files added to raw-data')
        # add mechanism to check minio content to avoid replication
        

@job(resource_defs={"io_manager": mem_io_manager}, executor_def=in_process_executor)
def collect_articles_job():
    already_in_raw_data = get_already_in_raw()
    blogs = get_blogs()
    links = get_links(blogs,already_in_raw_data)
    data = get_data(links)
    to_minio(data)



