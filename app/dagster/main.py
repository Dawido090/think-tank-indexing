"""Example of how to run a Dagster op from normal Python script."""
from jobs.raw_data_job import collect_articles_job

if __name__ == "__main__":
    result = collect_articles_job.execute_in_process()
