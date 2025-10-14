import os
from typing import Any, Optional

from dotenv import load_dotenv
import boto3
from botocore.client import BaseClient
from botocore.paginate import PageIterator
from io import BytesIO
import polars as pl
import json

class S3ClientModule:
    _s3_client = None
    _access_key_id = None
    _secret_access_key = None

    @classmethod
    def get_client(cls,
                   aws_access_key_id: Optional[str] = None,
                   aws_secret_access_key: Optional[str] = None) -> BaseClient:
        if access_key_id and secret_access_key:
            session = boto3.session.Session()
            cls._s3_client = session.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            cls._access_key_id = aws_access_key_id
            cls._secret_access_key = aws_secret_access_key

        return cls._s3_client

    @classmethod
    def get_storage_options(cls) -> dict[str, str | None | Any]:
        return {
            "aws_access_key_id": cls._access_key_id,
            "aws_secret_access_key": cls._secret_access_key,
            "aws_region": cls._s3_client.meta.region_name
        }


class S3Handler:
    _bucket = None
    _path = None

    def __init__(self, bucket: str, path: str) -> None:
        self._bucket = bucket
        self._path = path

    def _get_objects(self) -> PageIterator:
        s3_client = S3ClientModule.get_client()
        paginator = s3_client.get_paginator('list_objects_v2')
        return paginator.paginate(Bucket=self._bucket, Prefix=self._path)

    def _get_object_content(self) -> BytesIO:
        s3_client = S3ClientModule.get_client()
        response = s3_client.get_object(Bucket=self._bucket, Key=self._path)
        content = response["Body"].read()
        return BytesIO(content)

    def handle_in_bucket_path(self) -> str:
        pages = self._get_objects()

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_key = obj['Key']
                    file_key_csv = file_key.replace('.parquet', '.csv')

                    print(file_key)

                    # content: BytesIO = self._get_object_content()
                    # df: pl.DataFrame = pl.read_parquet(content, use_pyarrow=True)
                    # df.write_csv("s3://{0}/{1}".format(self._bucket, file_key_csv),
                    #              storage_options=S3ClientModule.get_storage_options())

if __name__ == "__main__":

    load_dotenv()

    schema = json.load(open("schema.json", 'r'))

    buckets = schema["buckets"]

    for bucket in buckets:

        name = bucket["name"]
        access_key_id = bucket["access_key_id"]
        secret_access_key = bucket["secret_access_key"]
        paths = bucket["paths"]

        # inicializando client
        S3ClientModule.get_client(aws_access_key_id=access_key_id,
                                  aws_secret_access_key=secret_access_key)

        for path in paths:
            handler = S3Handler(name, path)
            handler.handle_in_bucket_path()