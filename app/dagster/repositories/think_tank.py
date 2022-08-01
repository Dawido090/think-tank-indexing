from dagster import repository

from jobs.raw_data_job import collect_articles_job
from schedules.raw_data_scheduler import every_weekday_9am


@repository
def hello_cereal_repository():
    """Collection of cereal jobs and other definitions used by Dagster."""
    return [collect_articles_job, every_weekday_9am]
