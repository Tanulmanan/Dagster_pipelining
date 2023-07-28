from dagster import DailyPartitionsDefinition, FreshnessPolicy, MonthlyPartitionsDefinition, OpExecutionContext, asset
from .s3 import S3Wrapper
from .ops import *
import re
import os
import datetime
import pandas as pd
import gzip
import io

AS_NODES = [
    "ANCSVCS_HB_NORTH",
    "ANCSVCS_HB_SOUTH",
    "ANCSVCS_HB_WEST",
    "ANCSVCS_HB_HOUSTON",
    "ANCSVCS_HB_PAN",
]
CURVES = ["regup", "regdown", "spin", "nonspin"]

S3_CLIENT = S3Wrapper()
UPLOAD_BUCKET = "enine-test"

@asset(partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"))
def process_ercot_as(context: OpExecutionContext):
    """
    Asset function that retrieves and processes the ERCOT AS (Ancillary Services) data from S3.

    Args:
        context (OpExecutionContext): The execution context provided by Dagster.

    Returns:
        pd.DataFrame: The processed ERCOT AS data.
    """

    bucket = "us-ercot"
    prefix = "00012329"

    files = S3_CLIENT.list(bucket, prefix)
    context.log.info(f"Total {len(files)} files for AS")

    partition_date = context.asset_partition_key_for_output()
    date_pattern1 = r"_(\d{8})_"
    date_pattern2 = r"(?:[^.]*\.){3}([^.]*)\."
    prefix = os.path.join(prefix, partition_date[:4], partition_date[5:7])
    filtered_files = []

    for file in files:
        # Extract the date from the file name using regex pattern matching
        match1 = re.search(date_pattern1, file)
        match2 = re.search(date_pattern2, file)
        if match1:
            file_date = datetime.datetime.strptime(match1.group(1), "%Y%m%d").date()
        elif match2:
            file_date = datetime.datetime.strptime(match2.group(1), "%Y%m%d").date()

        file_date += datetime.timedelta(days=1)
        if file_date.strftime("%Y-%m-%d") == partition_date:
            filtered_files.append(file)
            break

    df = S3_CLIENT.parallel_df_download(bucket, filtered_files)
    df = transform_as_ercot(df)

    context.log.info(f"ERCOT AS Raw Prices Shape {df.shape}")

    return df.sort_values("date") if "date" in df.columns else df



@asset(partitions_def=MonthlyPartitionsDefinition(start_date="2022-01-01"),freshness_policy=FreshnessPolicy(maximum_lag_minutes=60, cron_schedule="0 0 * * *"))
def split_and_upload_ercot_as(context: OpExecutionContext, process_ercot_as: dict):
    """
    Asset function that splits and uploads the ERCOT AS data to S3 in monthly files.

    Args:
        context (OpExecutionContext): The execution context provided by Dagster.
        process_ercot_as (dict): The processed ERCOT AS data.

    Returns:
        None
    """
    
    print(f"process-ercot - {list(process_ercot_as.values())[1]}")
    
    first_date = list(process_ercot_as.keys())[1]
    
    as_df = pd.concat(process_ercot_as.values())
    print(f"columns-{as_df.columns}")
    
    for node in AS_NODES:
        monthly_df = as_df
        year, month = first_date.split("-")[:2]
        for curve in CURVES:
            curve_df = monthly_df[["date", curve, "repeated_hour_flag"]]
            
            output_path = os.path.join(
                "actuals",
                curve,
                "ercot",
                node,
                str(year),
                f"{node}_{curve}_{str(year)}{str(month).zfill(2)}.csv.gz",
            )
            output_path = output_path.replace("\\", "/")
            print(f"curve_df - {curve_df}, output_path - {output_path}")
            
            create_monthly_file(curve_df,output_path)

        context.log.info("Done Daily ERCOT AS processing for:%s", node)


