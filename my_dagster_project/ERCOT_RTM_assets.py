import os
import datetime
import pandas as pd

from dagster import (
        MonthlyPartitionsDefinition,
        OpExecutionContext,
        asset,  
        DailyPartitionsDefinition, 
)

from .ops import *
import re
from .s3 import S3Wrapper


S3_CLIENT = S3Wrapper()
UPLOAD_BUCKET = "enine-test"

@asset(partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"))
def rtm_raw_prices(context: OpExecutionContext):
    """
    Asset function that retrieves raw prices data for the RTM (Real-Time Market) from S3.
    
    Args:
        context (OpExecutionContext): The execution context provided by Dagster.

    Returns:
        pd.DataFrame: The raw prices data for the RTM.
    """
    
    bucket = "us-ercot"
    prefix = "00012301"
    results = []
    files = S3_CLIENT.list(bucket, prefix)
    context.log.info(f"Total {len(files)} files for RTM")
    
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

        results.append(S3_CLIENT.download_df(bucket, filtered_files))

    df = pd.concat(results) if len(results) > 0 else pd.DataFrame()
    context.log.info(f"RTM Raw Prices Shape {df.shape}")
    
    return df


@asset(partitions_def=MonthlyPartitionsDefinition(start_date="2022-01-01"))
def run_daily_rtm(context: OpExecutionContext, rtm_raw_prices: dict):
    """
    Asset function that processes the daily RTM data for each node and generates monthly output files.
    
    Args:
        context (OpExecutionContext): The execution context provided by Dagster.
        rtm_raw_prices (pd.DataFrame): The raw prices data for the RTM.

    Returns:
        None
    """
    
    rtm_df = transform_rtm_ercot(rtm_raw_prices).sort_values(by=["node"]).drop_duplicates()
    processed_data = {}
    first_date = list(rtm_raw_prices.keys())[1]
    for node, _rtm_df in rtm_df.groupby("node"):
            year, month = first_date.split("-")[:2]
            output_path = os.path.join(
                "actuals",
                "rtm",
                "ercot",
                node,
                str(year),
                f"{node}_rtm_{str(year)}{str(month).zfill(2)}.csv.gz",
            )

            output_path = output_path.replace("\\", "/")
            monthly_df = _rtm_df.drop(columns="node").sort_values("date")
            processed_data[output_path] = monthly_df

            context.log.info("Processed Daily RTM for: %s", node)
            create_monthly_file(monthly_df,output_path)


   

