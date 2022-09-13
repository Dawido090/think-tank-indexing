from dagster import repository

from jobs.raw_data_job import collect_articles_job
from jobs.transformation_job import transform_raw_data_job
from schedules.transformation_scheduler import every_weekday_8am
from schedules.raw_data_scheduler import every_weekday_9am


@repository
def collect_store_articles():
    return [collect_articles_job,transform_raw_data_job, every_weekday_9am,every_weekday_8am]
