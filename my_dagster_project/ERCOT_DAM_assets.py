from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition, OpExecutionContext, asset
from .s3 import S3Wrapper
from .ops import *
import re
import os
import datetime
import pandas as pd

S3_CLIENT = S3Wrapper()
UPLOAD_BUCKET = "enine-test"

@asset(partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"))
def process_ercot_dam(context: OpExecutionContext):
    """
        Process the ERCOT DAM (Daily Auction Market) data.

        Args:
            context (OpExecutionContext): The execution context.

        Returns:
            pd.DataFrame: Processed DAM data.
        """


    bucket = "us-ercot"
    prefix = "00012331"

    files = S3_CLIENT.list(bucket, prefix)
    context.log.info(f"Total {len(files)} files for DAM")

   

    partition_date=context.asset_partition_key_for_output()
    date_pattern1 = r"_(\d{8})_"  
    date_pattern2 = r"(?:[^.]*\.){3}([^.]*)\."  
    prefix=os.path.join(prefix,partition_date[:4],partition_date[5:7]) 
    filtered_files=[]

    for file in files:
        # Extract the date from the file name using regex pattern matching
        match1 = re.search(date_pattern1, file)
        match2 = re.search(date_pattern2, file)
        if match1:
            file_date = datetime.datetime.strptime(match1.group(1), "%Y%m%d").date()
        elif match2:
            file_date = datetime.datetime.strptime(match2.group(1), "%Y%m%d").date()
        
        file_date += datetime.timedelta(days=1)
        if file_date.strftime("%Y-%m-%d")==partition_date:
            filtered_files.append(file)
            break
        
    df = S3_CLIENT.parallel_df_download(bucket, filtered_files)
    df = transform_dam_ercot(df)
    
    
    # context.log.info(f"ERCOT DAM Raw Prices Shape {transformed_df.shape}")
    
    
    return df
    



@asset(partitions_def=MonthlyPartitionsDefinition(start_date="2022-01-01"))
def split_and_upload_ercot_dam(context: OpExecutionContext,  process_ercot_dam: dict):
    """
    This runs on daily DAM prices report 00012331
     
    Split and upload the processed ERCOT DAM data.

    Args:
        context (OpExecutionContext): The execution context.
        process_ercot_dam (dict): Processed ERCOT DAM data.

    Returns:
        None
    """
    

    first_date = list(process_ercot_dam.keys())[1]

    dam_df = pd.concat(process_ercot_dam.values())
    print(dam_df)
    processed_data = {}
    for node, _dam_df in dam_df.groupby("node"):
        
        year, month = first_date.split("-")[:2]
        output_path = os.path.join(
                "actuals",
                "dam",
                "ercot",
                node,
                str(year),
                f"{node}_dam_{str(year)}{str(month).zfill(2)}.csv.gz",
            )
        output_path = output_path.replace("\\", "/")
        monthly_df = _dam_df.drop(columns="node").sort_values("date")
        processed_data[output_path] = monthly_df
            

        context.log.info("Done ERCOT DAM processing for: %s", node)
        create_monthly_file(monthly_df,output_path)