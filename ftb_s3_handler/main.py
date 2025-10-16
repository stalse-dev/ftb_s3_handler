import concurrent
import os
import sys
from typing import Optional, Literal

import polars as pl
import logging
from dotenv import load_dotenv
from botocore.paginate import PageIterator
from botocore.exceptions import ClientError
from io import BytesIO
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock, Event
from ftb_s3_handler.s3_client import S3Client
from ftb_s3_handler.utils import handle_nested_data

logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class S3Handler:
    _bucket: str = None
    _path: str = None
    _file_count: int = None
    _file_count_lock: Lock = None
    _file_limit: int = None
    _shutdown_event = None
    _s3_client: S3Client = None
    _s3_client_session = None

    def __init__(self, bucket: str, path: str, file_limit: int = 0) -> None:
        self._bucket = bucket
        self._path = path
        self._file_count = 0
        # lock count para thread safe
        # evitar race condition na contagem
        # de arquivos
        self._file_count_lock = Lock()
        self._file_limit = file_limit
        self._shutdown_event = Event()
        self._s3_client = S3Client()
        self._s3_client_session = self._s3_client.get_s3_client()

    def object_already_exists(self, file_key: str) -> bool:
        try:
            self._s3_client_session.get_object(Bucket=self._bucket, Key=file_key)
        # se nenhum ãrquivo foi encontrado ele levanta o erro NoSuchKey
        # que pode ser capturado usando isso
        except ClientError:
            return False

        return True

    def _increment_file_count(self) -> int:
        with self._file_count_lock:
            self._file_count += 1
            return self._file_count

    def _get_objects(self) -> PageIterator:
        paginator = self._s3_client_session.get_paginator('list_objects_v2')
        return paginator.paginate(Bucket=self._bucket, Prefix=self._path)

    def _get_object_content(self, file_key: str) -> BytesIO:
        response = self._s3_client_session.get_object(Bucket=self._bucket, Key=file_key)
        content = response["Body"].read()
        return BytesIO(content)

    def _save_in_path(self, df: pl.DataFrame, file_key_csv: str) -> None:
        df = handle_nested_data(df)

        df.write_csv(
            f"s3://{self._bucket}/{file_key_csv}",
            storage_options=self._s3_client.get_storage_options()
        )

    def execute(self):
        pages = self._get_objects()
        max_workers_process_file = int(os.environ.get('MAX_WORKERS', '2'))

        logger.info(
            f"Retrieved objects in bucket {self._bucket} path {self._path}")

        def _process_file(obj) -> Optional[str]:
            file_key = obj['Key']

            if file_key.endswith('/'):
                logger.warning(
                    "{0} é um diretório, pulando".format(file_key))
                return None

            file_key_csv = file_key.replace('.parquet', '.csv')

            if self.object_already_exists(file_key_csv):
                logger.warning(
                    "{0} conversão para csv já existe, pulando".format(file_key_csv))
                return None

            logger.info(f"Processando: {file_key} -> {file_key_csv}")

            try:
                content: BytesIO = self._get_object_content(file_key)
                df: pl.DataFrame = pl.read_parquet(
                    content, use_pyarrow=True)

                self._save_in_path(df=df, file_key_csv=file_key_csv)

                file_count = self._increment_file_count()

                logger.info(
                    f"Convertido com sucesso: {file_key} -> {file_key_csv}")

                # Check file limit with thread-safe approach
                # if file_count >= self._file_limit != 0:
                #     logger.warning(
                #         "Limite de arquivos alcançado")
                #     sys.exit(0)

            except Exception as e:
                logger.error(
                    f"Ocorreu um erro ao processar {file_key}: {str(e)}")
                return None

        for page in pages:
            if 'Contents' in page:
                with ThreadPoolExecutor(max_workers=max_workers_process_file) as _process_file_executor:
                    futures = [_process_file_executor.submit(_process_file, obj) for obj in page['Contents']]

                    for _process_file_future in concurrent.futures.as_completed(futures):
                        _process_file_result = future.result()


def process_bucket_path(bucket: str, path: str):
    file_limit = int(os.environ.get('FILE_LIMIT', '0'))

    handler = S3Handler(bucket, path, file_limit=file_limit)
    handler.execute()


if __name__ == "__main__":

    load_dotenv()

    bucket_name = os.environ.get("S3_BUCKET")
    paths = os.environ.get("S3_PATHS").split(";")
    max_workers = int(os.environ.get('MAX_WORKERS', '2'))

    logger.info(f"Processing bucket {bucket_name}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # enviando todas as tasks e salvando em um dict
        future_to_path = {
            executor.submit(process_bucket_path, bucket_name, path): path
            for path in paths
        }

        # esperando tasks completarem para mostrar o feedback [path por path]
        for future in concurrent.futures.as_completed(future_to_path):
            path = future_to_path[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error(f"Ocorreu um erro ao processar path: {exc}")
