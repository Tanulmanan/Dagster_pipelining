import gzip
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import botocore
import boto3
import pandas as pd


class S3Wrapper:
    """
    S3Wrapper is a class that provides a wrapper around the s3 resource in boto3.
    It provides methods to list buckets, upload files, and list files in a bucket.
    """

    def __init__(self):
        aws_access_key_id = os.getenv("AWS_S3_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_S3_SECRET_ACCESS_KEY")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def list(self, bucket_name, prefix="", full_path=False):
        try:
            if prefix and prefix > "":
                response = self.client.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix
                )
            else:
                response = self.client.list_objects_v2(Bucket=bucket_name)
            return [x["Key"] for x in response["Contents"]]

        except Exception as e:
            print(e)
            return None

    def download_df(self, bucket_name, filename):
        buffer = BytesIO()
        try:
            self.client.download_fileobj(bucket_name, filename, buffer)
            buffer.seek(0)
            if filename.endswith(".zip"):
                return self.zip_to_df(buffer)
            elif filename.endswith(".csv"):
                return pd.read_csv(buffer)
            return pd.DataFrame()
        except Exception as e:
            print(e)
            return None

    def download_file_buffer(self, bucket_name, filename):
        buffer = BytesIO()
        try:
            self.client.download_fileobj(bucket_name, filename, buffer)
            buffer.seek(0)
            return buffer
        except Exception as e:
            print(e)
            return None

    def upload_df_to_s3_in_gzip(self, df, bucket_name, file_key):
        csv_data = df.to_csv(index=False)
        compressed_data = gzip.compress(bytes(csv_data, "utf-8"))
        fileobj = BytesIO(compressed_data)
        self.client.upload_fileobj(
            fileobj,
            bucket_name,
            file_key,
            ExtraArgs={"ContentEncoding": "gzip", "ContentType": "text/csv"},
        )
        return True
    
    def upload_monthlydf_to_s3_in_gzip(self, data, bucket_name, file_key):
        csv_data = data.to_csv(index=False)
        compressed_data = gzip.compress(bytes(csv_data, "utf-8"))
        fileobj = BytesIO(compressed_data)
        
        # Extract the necessary information from the file_key
        parts = file_key.split('/')
        curve = parts[1]
        iso = parts[2]
        node = parts[3]
        year = parts[4]
        file_name = parts[-1]
        
        # Create the monthly folder path
        monthly_folder = f"actuals/{curve}/{iso}/{node}/{year}"
        
        # Create the full S3 key with the monthly folder and file name
        s3_key = f"{monthly_folder}/{file_name}"
        
        # Create the monthly folder if it doesn't exist
        self.client.put_object(Body='', Bucket=bucket_name, Key=f"{monthly_folder}/")
        
        # Upload the file to S3 with the modified file_key
        self.client.upload_fileobj(
            fileobj,
            bucket_name,
            s3_key,
            ExtraArgs={"ContentEncoding": "gzip", "ContentType": "text/csv"},
        )
        return True

    def parallel_s3_upload(self, data, bucket_name):
        futures = []
        with ThreadPoolExecutor() as executor:
            for path, df in data.items():
                futures.append(
                    executor.submit(self.upload_df_to_s3_in_gzip, df, bucket_name, path)
                )
                print(f"Submitted task for {path}")
            # Wait for all uploads to complete
            for future in futures:
                future.result()

    def parallel_df_download(self, bucket_name, file_keys):
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.download_df, bucket_name, file_key)
                for file_key in file_keys
            ]
            df_list = [future.result() for future in futures]

        return pd.concat(df_list) if len(df_list) > 0 else pd.DataFrame()

    def zip_to_df(self, buffer):
        result = []
        zip_files = zipfile.ZipFile(buffer)
        zip_list = zip_files.infolist()
        for file in zip_list:
            result.append(zip_files.open(file))

        return pd.concat([pd.read_csv(f) for f in result])
