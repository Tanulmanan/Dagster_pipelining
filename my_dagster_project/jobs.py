from dagster import AssetSelection,  define_asset_job

from .ERCOT_DAM_assets import *

from .ERCOT_AS_assets import *

from .ERCOT_RTM_assets import *

partitioned_asset_job_ercot_dam = define_asset_job(
    "ercot_dam_job",
    selectilon=AssetSelection.assets(process_ercot_dam, split_and_upload_ercot_dam),
    partitions_def= DailyPartitionsDefinition(start_date="2022-01-01")
)
"""
Defines a partitioned asset job for processing and splitting ERCOT DAM assets.

Attributes:
    name (str): The name of the job.
    selection (AssetSelection): The asset selection for the job.
    partitions_def (PartitionsDefinition): The definition of partitions for the job.
"""


partitioned_asset_job_ercot_as = define_asset_job(
    "ercot_as_job",
    selection=AssetSelection.assets(split_and_upload_ercot_as, process_ercot_as),
    partitions_def= DailyPartitionsDefinition(start_date="2022-01-01")
)
"""
Defines a partitioned asset job for splitting and processing ERCOT AS assets.

Attributes:
    name (str): The name of the job.
    selection (AssetSelection): The asset selection for the job.
    partitions_def (PartitionsDefinition): The definition of partitions for the job.
"""

partitioned_asset_job_ercot_rtm = define_asset_job(
    "ercot_rtm_job",
    selection=AssetSelection.assets(rtm_raw_prices, run_daily_rtm),
    partitions_def= DailyPartitionsDefinition(start_date="2022-01-01")
)
"""
Defines a partitioned asset job for running daily RTM processing.

Attributes:
    name (str): The name of the job.
    selection (AssetSelection): The asset selection for the job.
    partitions_def (PartitionsDefinition): The definition of partitions for the job.
"""
