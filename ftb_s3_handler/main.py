import os
from dotenv import load_dotenv
import boto3
from botocore.client import BaseClient
from botocore.paginate import PageIterator
from io import BytesIO
import polars as pl


class S3ClientModule:
    _s3_client = None

    @classmethod
    def get_client(cls) -> BaseClient:
        if cls._s3_client is None:
            session = boto3.session.Session()
            cls._s3_client = session.client(
                "s3",
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            )
        return cls._s3_client

    @classmethod
    def get_storage_options(cls) -> str:
        client = cls.get_client()

        return {
            "aws_access_key_id": os.environ.get('AWS_ACCESS_KEY_ID'),
            "aws_secret_access_key": os.environ.get('AWS_SECRET_ACCESS_KEY'),
            "aws_region": client.meta.region_name
        }


def get_objects(bucket: str, prefix: str) -> PageIterator:
    s3_client = S3ClientModule.get_client()
    paginator = s3_client.get_paginator('list_objects_v2')
    return paginator.paginate(Bucket=nome_bucket, Prefix=prefix)


def get_object_content(bucket: str, path: str) -> BytesIO:
    s3_client = S3ClientModule.get_client()
    response = s3_client.get_object(Bucket=bucket, Key=path)
    content = response["Body"].read()
    return BytesIO(content)


if __name__ == "__main__":

    load_dotenv()

    nome_bucket = os.environ.get('S3_BUCKET')
    destino = "appcues/2024/12/18/2024-12-18_07-00-19.parquet"

    pages = get_objects(nome_bucket, destino)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                file_key = obj['Key']
                file_key_csv = file_key.replace('.parquet', '.csv')

                content: BytesIO = get_object_content(nome_bucket, file_key)
                df: pl.DataFrame = pl.read_parquet(content, use_pyarrow=True)
                df.write_csv("s3://{0}/{1}".format(nome_bucket, file_key_csv),
                             storage_options=S3ClientModule.get_storage_options())
