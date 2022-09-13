"""Collection of Cereal schedules"""

from dagster import schedule

from jobs.transformation_job import transform_raw_data_job
  # pylint: disable=unused-import


# https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
@schedule(
    cron_schedule="0 8 * * 1-5",
    job=transform_raw_data_job,
    execution_timezone="Europe/Stockholm",
)
def every_weekday_8am(context):
    """Example of how to setup a weekday schedule for a job."""
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"download_cereals": {"config": {"date": date}}}}
