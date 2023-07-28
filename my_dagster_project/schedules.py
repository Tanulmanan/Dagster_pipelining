from .jobs import *
from dagster import ScheduleDefinition


ercot_job_dam_schedule=ScheduleDefinition(job=partitioned_asset_job_ercot_as, cron_schedule="0 0 * * *")
"""
Schedule definition for the ERCOT DAM job.

Attributes:
    job (callable): The job function to be executed.
    cron_schedule (str): The cron schedule expression for the job.
"""
ercot_job_as_schedule=ScheduleDefinition(job=partitioned_asset_job_ercot_as, cron_schedule="0 0 * * *")
"""
Schedule definition for the ERCOT AS job.

Attributes:
    job (callable): The job function to be executed.
    cron_schedule (str): The cron schedule expression for the job.
"""
ercot_job_rtm_schedule=ScheduleDefinition(job=partitioned_asset_job_ercot_as, cron_schedule="0 0 * * *")
"""
Schedule definition for the ERCOT RTM job.

Attributes:
    job (callable): The job function to be executed.
    cron_schedule (str): The cron schedule expression for the job.
"""


