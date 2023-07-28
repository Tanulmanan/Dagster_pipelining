from dagster import Definitions

from .ERCOT_RTM_assets import *

from .ERCOT_AS_assets import *
from .ERCOT_DAM_assets import *
from .jobs import *
from .schedules import *

defs = Definitions(
    assets=[
        process_ercot_as,
        split_and_upload_ercot_as,
        process_ercot_dam,
        split_and_upload_ercot_dam,
        rtm_raw_prices,
        run_daily_rtm
    ],
    jobs=[
        partitioned_asset_job_ercot_rtm,
        partitioned_asset_job_ercot_as,
        partitioned_asset_job_ercot_dam],


    schedules=[
        ercot_job_dam_schedule,
              ercot_job_as_schedule,
               ercot_job_rtm_schedule
        ]

    
)
