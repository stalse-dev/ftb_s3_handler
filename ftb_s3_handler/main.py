import concurrent
import polars as pl
import json
import logging
import boto3
from typing import Any, Optional
from dotenv import load_dotenv
from botocore.client import BaseClient
from botocore.paginate import PageIterator
from botocore.exceptions import ClientError
from io import BytesIO
from concurrent.futures.thread import ThreadPoolExecutor

from ftb_s3_handler.utils import handle_nested_data

logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class S3ClientModule:
    _s3_client = None
    _access_key_id = None
    _secret_access_key = None

    @classmethod
    def get_client(cls,
                   aws_access_key_id: Optional[str] = None,
                   aws_secret_access_key: Optional[str] = None) -> BaseClient:
        if aws_access_key_id and aws_secret_access_key:
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

    def object_already_exists(self, file_key: str) -> bool:
        s3_client = S3ClientModule.get_client()
        try:
            s3_client.get_object(Bucket=self._bucket, Key=file_key)
        # se nenhum ãrquivo foi encontrado ele levanta o erro NoSuchKey
        # que pode ser capturado usando isso
        except ClientError:
            return False

        return True

    def _get_objects(self) -> PageIterator:
        s3_client = S3ClientModule.get_client()
        paginator = s3_client.get_paginator('list_objects_v2')
        return paginator.paginate(Bucket=self._bucket, Prefix=self._path)

    def _get_object_content(self, file_key: str) -> BytesIO:
        s3_client = S3ClientModule.get_client()
        response = s3_client.get_object(Bucket=self._bucket, Key=file_key)
        content = response["Body"].read()
        return BytesIO(content)

    def _save_in_path(self, df: pl.DataFrame, file_key_csv: str) -> None:
        df = handle_nested_data(df)

        df.write_csv(
            f"s3://{self._bucket}/{file_key_csv}",
            storage_options=S3ClientModule.get_storage_options()
        )

    def execute(self):
        pages = self._get_objects()
        logger.info(f"Retrieved objects in bucket {self._bucket} path {self._path}")

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_key = obj['Key']

                    if file_key.endswith('/'):
                        logger.warning(
                            "{0} é um diretório, pulando".format(file_key))
                        continue

                    if not file_key.endswith('.parquet'):
                        logger.warning(
                            "{0} não é um parquet, pulando".format(file_key))
                        continue

                    file_extension = file_key.split('.')[-1].lower()
                    if file_extension == 'csv':
                        logger.warning(
                            "{0} já um csv, pulando".format(file_key))
                        continue

                    file_key_csv = file_key.replace('.parquet', '.csv')

                    # if self.object_already_exists(file_key_csv):
                    #     logger.warning(
                    #         "{0} conversão para csv já existe, pulando".format(file_key_csv))
                    #     continue

                    logger.info(f"Processando: {file_key} -> {file_key_csv}")

                    try:
                        content: BytesIO = self._get_object_content(file_key)
                        df: pl.DataFrame = pl.read_parquet(
                            content, use_pyarrow=True)

                        self._save_in_path(df=df, file_key_csv=file_key_csv)

                        logger.info(
                            f"Convertido com sucesso: {file_key} -> {file_key_csv}")

                    except Exception as e:
                        logger.error(
                            f"Ocorreu um erro ao processar {file_key}: {str(e)}")
                        continue


def process_bucket_path(bucket_name: str,
                        path: str,
                        aws_access_key_id: str,
                        aws_secret_access_key: str):
    S3ClientModule.get_client(aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

    handler = S3Handler(bucket_name, path)
    handler.execute()


if __name__ == "__main__":

    load_dotenv()

    schema = json.load(open("schema.json", 'r'))

    buckets = schema["buckets"]

    for bucket in buckets:

        name = bucket["name"]
        access_key_id = bucket["access_key_id"]
        secret_access_key = bucket["secret_access_key"]
        paths = bucket["paths"]

        logger.info(f"Processing bucket {name}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            # enviando todas as tasks e salvando em um dict
            future_to_path = {
                executor.submit(process_bucket_path, name, path, access_key_id, secret_access_key): path
                for path in paths
            }

            # esperando tasks completarem para mostrar o feedback [path por path]
            for future in concurrent.futures.as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    result = future.result()
                    logger.info(f"Finalizado {path}")
                except Exception as exc:
                    logger.error(f"Ocorreu um erro ao processar path: {exc}")
