
import io
from dagster import op
import pandas as pd
from .s3 import S3Wrapper
import botocore
import gzip

S3_CLIENT = S3Wrapper()
UPLOAD_BUCKET = "enine-test"
@op
def transform_dam_ercot(df):
    """
    Transforms the DAM ERCOT DataFrame by renaming columns, assigning new columns, dropping unnecessary columns,
    and converting data types.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The transformed DataFrame.
    """
    columns = {
        "DeliveryDate": "date",
        "SettlementPointPrice": "dam",
        "SettlementPoint": "node",
        "DSTFlag": "repeated_hour_flag",
    }
    df = (
        df.rename(columns=columns)
        .assign(
            hour=lambda x: x["HourEnding"].str.split(":", expand=True)[0].astype(int)
            - 1,
            date=lambda x: pd.to_datetime(
                x["date"] + " " + x["hour"].astype(str), format="%m/%d/%Y %H"
            ),
        )
        .drop(columns=["HourEnding", "hour"])
        .reset_index(drop=True)
        .dropna()
    )
    df.loc[df["repeated_hour_flag"] == "N", "repeated_hour_flag"] = 0
    df.loc[df["repeated_hour_flag"] == "Y", "repeated_hour_flag"] = 1

    return df

@op
def transform_as_ercot(df):
    """
    Transforms the AS ERCOT DataFrame by assigning new columns, pivoting the table, renaming columns, and dropping
    unnecessary columns.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The transformed DataFrame.
    """
    df = (
        df.assign(
            hour=lambda x: x["HourEnding"].str.split(":", expand=True)[0].astype(int)
            - 1,
            date=lambda x: pd.to_datetime(
                x["DeliveryDate"] + " " + x["hour"].astype(str), format="%m/%d/%Y %H"
            ),
        )
        .pivot_table(index=["date", "DSTFlag"], columns="AncillaryType", values="MCPC")
        .rename(
            columns={
                "RRS": "spin",
                "REGUP": "regup",
                "REGDN": "regdown",
                "NSPIN": "nonspin",
            }
        )
        .reset_index()
        .rename(columns={"DSTFlag": "repeated_hour_flag"})
        .dropna()
    )
    df.loc[df["repeated_hour_flag"] == "N", "repeated_hour_flag"] = 0
    df.loc[df["repeated_hour_flag"] == "Y", "repeated_hour_flag"] = 1
    return df

@op
def transform_rtm_ercot(df):
    """
    Transforms the RTM ERCOT DataFrame by renaming columns, grouping by specific columns, calculating the mean,
    assigning new columns, dropping unnecessary columns, and converting data types.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The transformed DataFrame.
    """
    df = (
        df.rename(
            columns={
                "SettlementPointPrice": "rtm",
                "SettlementPointName": "node",
                "DSTFlag": "repeated_hour_flag",
            }
        )
        .groupby(["node", "repeated_hour_flag", "DeliveryDate", "DeliveryHour"])
        .mean(numeric_only=True)
        .reset_index()
        .assign(
            hour=lambda x: (x["DeliveryHour"] - 1).astype(str),
            date=lambda x: pd.to_datetime(
                x["DeliveryDate"] + " " + x["hour"].astype(str), format="%m/%d/%Y %H"
            ),
        )
        .drop(
            columns=[
                "DeliveryHour",
                "DeliveryDate",
                "hour",
                "DeliveryInterval",
            ]
        )
        .dropna()
    )
    df.loc[df["repeated_hour_flag"] == "N", "repeated_hour_flag"] = 0
    df.loc[df["repeated_hour_flag"] == "Y", "repeated_hour_flag"] = 1

    return df


@op
def create_monthly_file(df, output_path):
    """
    Creates a monthly file by compressing the DataFrame, checking if the file already exists in S3, retrieving
    the existing file if it exists, combining the existing file with the input DataFrame, removing duplicates,
    compressing the combined DataFrame, and uploading the file to S3.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        output_path (str): The output file path in S3.

    Returns:
        None
    """
    csv_data = df.to_csv(index=False)
    compressed_data = gzip.compress(bytes(csv_data, 'utf-8'))
    
    # Check if the file exists in S3
    try:
        S3_CLIENT.head_object(Bucket=UPLOAD_BUCKET, Key=output_path)
        file_exists = True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            file_exists = False
        else:
            raise

    if file_exists:
        # Retrieve existing file from S3
        response = S3_CLIENT.get_object(Bucket=UPLOAD_BUCKET, Key=output_path)
        existing_data = response['Body'].read()
        
        existing_data = gzip.decompress(existing_data).decode('utf-8')
        existing_df = pd.read_csv(io.StringIO(existing_data))

        combined_df = pd.concat([existing_df, df], ignore_index=True)

        combined_df = combined_df.drop_duplicates()

        combined_compressed_data = gzip.compress(bytes(combined_df.to_csv(index=False), 'utf-8'))

        # Upload the updated file to S3
        S3_CLIENT.put_object(
            Bucket=UPLOAD_BUCKET,
            Key=output_path,
            Body=combined_compressed_data,
            ContentEncoding='gzip',
            ContentType='text/csv'
        )
    else:
        # Upload the new file to S3
        S3_CLIENT.put_object(
            Bucket=UPLOAD_BUCKET,
            Key=output_path,
            Body=compressed_data,
            ContentEncoding='gzip',
            ContentType='text/csv'
        )