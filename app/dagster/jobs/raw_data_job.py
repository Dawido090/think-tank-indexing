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
    # check creds in future
    for blog in blogs:
        base_page = blog
        result = []
        querystring = {"_wrapper_format":"drupal_ajax"}
        # implement stop of program after certain time if isn't able to find new data in future
        flag = True
        page_num = 1
        while (len(result) < 14) and (flag == True):
            headers = {
                "authority": "www.cfr.org",
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "en-GB,en;q=0.9,pl;q=0.8,en-US;q=0.7",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "cookie": "_cb=C3VoU5C2o5cYmR7OP; _fbp=fb.1.1659107102835.468199250; cookie-agreed=2; _hjSessionUser_1768366=eyJpZCI6IjE4NTJkNmM1LWMzOTAtNTYzMC04NjlhLTBiM2JiMDg2ZWExNCIsImNyZWF0ZWQiOjE2NTkxMDcxMDI5NjIsImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.209379316.1662914268; _hjSession_1768366=eyJpZCI6IjM2MzViYjc5LTc5ZmMtNGE3NS04MWE5LTIzMzM5MTViYjYzMSIsImNyZWF0ZWQiOjE2NjI5MTQyNjc5NjksImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _hjIncludedInSessionSample=0; _chartbeat2=.1659107102163.1662916256223.0000000000000001.DoVOWrBGUiY38ml3bCDEdfw_8hfG.3; _ga=GA1.2.15477803.1659107103; _ga_RBVKHZHP8E=GS1.1.1662914267.12.1.1662916257.57.0.0; _ga_24W5E70YKH=GS1.1.1662914267.12.1.1662916257.0.0.0; sailthru_content=74433b1ca507a7e3b74fcab27c6469dcafd46984a2a480a2c6c366ba71fe94b39358023936010efb18c285bd9742f8f10cc9c0d0ae45974b61decf8386d358132127943bfd9b2383353828d7fd753a286745dc3c4d41f8234b58b6005f72f7d0adaf628dd34ddd9a6defc182393023089c2f45aa9ed550ec7295c64b1bd63f204806c15541b01bd1e67e3f8f13a1e41a7fae3d2665c413c99a3c9cb59636edd4965d8d08c0e21f85d9226469e3e46613dcb04b9a318e83ab02959872f457481338036bc56a0e62c97d78e56b829b0390cff8804e237f6cdf3a085c14986cbc99299173f1f5c606c2cf70a699da89e6cb5293a3eac0d13f9847e43c5e382fc2d0; sailthru_visitor=586ad90b-1cf6-45ce-adf9-9e8108a1bd32; amp_2be1ae=kqJ8mfrNA3YX_SXS4cIY6F...1gcmmk2d3.1gcmrfm4b.53.0.53; _chartbeat5=438^|18648^|^%^2Fblog^%^2Fasia-unbound^|https^%^3A^%^2F^%^2Fwww.cfr.org^%^2Fblog^%^2Fasia-unbound^%^3Ftopics^%^3DAll^%^26regions^%^3DAll^%^26page^%^3D6^|lqlc9ByeQe4C6Ovp0WcITUC89s4y^|^|c^|BxNaC6Baul6wB72DEUCKWW08DlTVRR^|cfr.org^|",
                "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjI0NTYxNjAiLCJhcCI6IjM0NTY3NTgwMyIsImlkIjoiZTFhMGRlNGZmMWRiNGZiNiIsInRyIjoiYzk4ZWFjNWRhYWQxMjVjYThmZmQ1MGI4Y2I3MGE2YjIiLCJ0aSI6MTY2MjkxOTcxNDY5OCwidGsiOiI2NjY4NiJ9fQ==",
                "topics":"All",
                "regions":"All",
                "page":f"{page_num}"
            }

            response = requests.request("GET", base_page, headers=headers, params=querystring)
            get_dagster_logger().info('\t'.join([x for x in already_in_raw]))
            data = response.json()[4]['data']
            find_links = list(set([x.strip() for x in re.findall(r'(?<=<a href=")(.*)(?=" class="card-article-large__link">)',data)]))
            get_dagster_logger().info('\t'.join([x for x in find_links]))
            if already_in_raw != None:
                find_links_filtered = ["https://www.cfr.org" + x for x in find_links if x.split('/')[-1] not in already_in_raw]
            elif already_in_raw == None:
                find_links_filtered = ["https://www.cfr.org" + x for x in find_links]
            get_dagster_logger().info('\t'.join([x for x in find_links_filtered]))
            
            result.extend(find_links_filtered)
            page_num += 1
    get_dagster_logger().info('\t'.join([x for x in result]))
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



