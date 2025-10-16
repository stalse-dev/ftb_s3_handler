import os
from threading import Lock
import boto3
from botocore.client import BaseClient


class SingletonMeta(type):
    _instances = {}
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class S3Client(metaclass=SingletonMeta):
    def __init__(self):
        self._access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self._secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        session = boto3.session.Session()
        self._s3_client = session.client(
            "s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
        )

    def get_s3_client(self) -> BaseClient:
        return self._s3_client

    def get_storage_options(self):
        return {
            "aws_access_key_id": self._access_key_id,
            "aws_secret_access_key": self._secret_access_key,
            "aws_region": self._s3_client.meta.region_name
        }