import io
import csv
import boto3
import datetime
import pandas as pd
from typing import Dict, List
from botocore.client import Config
import datadotworld as dw


class Dataset:

    def __init__(self, name: str, dataframe: pd.DataFrame) -> None:
        
        self.name = self._clean_name(name)
        self.dataframe = dataframe

    def _clean_name(self, name: str) -> str:
        """Clean name"""
        
        return name.replace('original/', '')

    def _datestamp_dataframe(self, dataframe):

        today = datetime.datetime.date(
            datetime.datetime.now()
        )

        # modify in place
        dataframe['extract_date'] = today

class Extract:

    def __init__(self) -> None:
        pass

    def get_s3_client(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        config: Config = Config(signature_version='s3v4'),
        region_name: str = "us-east-1"
    ) -> boto3.client:
        """
        Connects to a MinIO instance using boto3, an AWS SDK for Python.

        This function configures a boto3 client to interact with a MinIO server,
        which is S3-compatible. It lists all buckets in the MinIO instance.

        Configuration:
            - `endpoint_url`: Set to your MinIO server URL (e.g., "http://localhost:9000").
            - `aws_access_key_id` and `aws_secret_access_key`: Replace with your MinIO credentials.
            - `config`: Uses S3v4 signature for compatibility with MinIO.
        """

        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config,
            region_name=region_name  # Optional for MinIO
        )

        return s3_client

    def extract_dataworld_data(
        self,
        url: str
    ) -> List[Dataset]:
        """
        Extracts data from DataWorld.io and loads to an s3 bucket

        return a list strings
        """
        dataset = dw.load_dataset(url, force_update=True)

        datasets = []

        dataframe_keys = []
        # get all standard datasets
        for key in dataset.dataframes.keys():
            datasets.append(
                Dataset(
                    key,
                    dataset.dataframes.get(
                        key
                    )
                )
            )
            # add this to compare to later
            dataframe_keys.append(key)

        # get all other datasets
        for key in dataset.raw_data.keys():
            if key.replace('original/', '') not in dataframe_keys:
                # process this else skip
                print(f"processing: {key}")
                datasets.append(
                    self.process_raw_data(
                        key,
                        dataset.raw_data.get(key)
                    )
                )

        return datasets

    def process_raw_data(
        self,
        key: str,
        data: bytes
    ) -> Dataset:
        """Process raw data and return a dataframe"""

        buffer = io.StringIO(data.decode())
        df = pd.read_csv(buffer)

        return Dataset(key, df)        


    def load_to_bucket(
        self,
        client: boto3.client,
        bucket_name: str,
        dataset: Dataset
    ):
        """Upload data to bucket"""

        # stripe the upload name with date
        today = datetime.datetime.date(
            datetime.datetime.now()
        )

        buffer = io.BytesIO()

        dataset.dataframe.to_csv(buffer, index=False)

        # reset the cursor in the buffer
        buffer.seek(0)

        # Upload the CSV data to the specified bucket
        client.put_object(
            Bucket=bucket_name, 
            Key=f"{today}/{dataset.name}.csv",
            Body=buffer, 
            ContentType='text/csv'
        )


if __name__ == '__main__':
    extract = Extract()
    url = 'dcereijo/player-scores'
    minio_url = 'http://localhost:9000'
    user = 'user'
    password = 'password'

    s3_client = extract.get_s3_client(
        minio_url,
        user,
        password
    )
    datasets = extract.extract_dataworld_data(url)
    for dataset in datasets:
        extract.load_to_bucket(
            s3_client,
            'football-data',
            dataset
        )

