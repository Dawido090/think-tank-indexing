from dagster import get_dagster_logger, job, op, mem_io_manager, in_process_executor
from requests_html import HTMLSession
from minio import Minio, error
import datetime
import time


@op
def get_blogs():
    # think about multi blogs loop
    blogs = ['https://www.cfr.org/blog/asia-unbound']
    get_dagster_logger().info(blogs)
    return blogs

@op
def get_links(blogs):
    # check in future
    for blog in blogs:
        base_page = blog
        result = []
        session = HTMLSession()
        ses = session.get(base_page)
        for article_link in ses.html.xpath('//*[@id="latest"]/div/div/div')[0].absolute_links:
            if (base_page != article_link) and ('blog' in article_link):
                result.append(article_link)
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
        post_date = ses_html.xpath('/html/body/div[1]/div/div/main/div/article/div/header/div/div[4]/div[2]')[0].text
        post_date = post_date.split('(')[0].strip()
        post_date = datetime.datetime.strptime(post_date,'%B %d, %Y %H:%M %p' )
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


# @op
# def joined_content(filtered_body):
#     # joining content of article
#     joined_content = '\n'.join(filtered_body)
#     get_dagster_logger().info(joined_content)

@op
def get_minio_client():
    Client = Minio(
        'localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure = False
    )
    return Client

@op
def check_minio_connection(client):
    try:
        if not client.bucket_exists("nonexistingbucket"):
            get_dagster_logger().info("Object storage connected")
            return True
    except:
        # raise error if storage not reachable
        get_dagster_logger().info("Object storage not reachable")
        return False

@op
def insert_to_minio(client, conf_attr, data):
    client.put_object(bucket_name=conf_attr[0],object_name=conf_attr[1],data=data,length=-1)
    get_dagster_logger().info(f"Object {conf_attr[0]}, inserted to {conf_attr[1]}")

@op
def minio_basic_conf():
    return ['articles-raw-data','test123.txt']


@job(resource_defs={"io_manager": mem_io_manager}, executor_def=in_process_executor)
def collect_articles_job():
    blogs = get_blogs()
    links = get_links(blogs)
    data = get_data(links)
    # joined = joined_content(data)
    # client = get_minio_client()
    # conf_list = minio_basic_conf()
    # # if check_minio_connection(client) == True:
    # insert_to_minio(client=client,conf_attr=conf_list,data=joined)


