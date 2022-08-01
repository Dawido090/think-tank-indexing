from dagster import get_dagster_logger, job, op
from requests_html import HTMLSession


@op
def get_data():
    result = []
    session = HTMLSession()
    ses = session.get('https://www.cfr.org/blog/family-ties-led-sri-lankas-collapse-what-does-mean-south-asia')
    body = ses.html.xpath('/html/body/div[1]/div/div/main/div/article/div/div/div[1]/div')
    body_p = body[0].find('p')
    for i in body_p:
        if 'class' not in str(i) and i != '':
            result.append(i.text)
    get_dagster_logger().info(len(result))
    return result


@op
def joined_content(filtered_body):
    joined_content = '\n'.join(filtered_body)
    get_dagster_logger().info(joined_content)



@job
def collect_articles_job():
    data = get_data()
    joined = joined_content(data)        
# @op
# def get_data():
#     session = HTMLSession()
#     ses = session.get('https://www.cfr.org/blog/family-ties-led-sri-lankas-collapse-what-does-mean-south-asia')
#     return ses.html

# @op
# def find_content(raw_html):
#     body = raw_html.xpath('/html/body/div[1]/div/div/main/div/article/div/div/div[1]/div')[0].text
#     return body

# @op
# def filter_content(body):
#     result = []
#     body_p = body[0].find('p')
#     for i in body_p:
#         if 'class' not in str(i) and i != '':
#             result.append(i.text)
#     get_dagster_logger().info(len(result))

# @op
# def joined_content(filtered_body):
#     joined_content = '\n'.join(filtered_body)
#     get_dagster_logger().info(joined_content)

# @job
# def run():
#     data = get_data()
#     raw = find_content(data)
#     filtered = filter_content(raw)
#     joined = joined_content(filtered)        
