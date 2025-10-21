import json
import os
from threading import Lock
from google.cloud import storage
from google.oauth2 import service_account


class SingletonMeta(type):
    _instances = {}
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class GCSClient(metaclass=SingletonMeta):
    def __init__(self):
        service_account_json = json.loads(os.environ.get('GOOGLE_SERVICE_ACCOUNT'))
        credentials = service_account.Credentials.from_service_account_info(
            service_account_json
        )

        self._gcs_client = storage.Client(
            "api-project-1033684201634",
            credentials=credentials,
        )

    def get_gcs_client(self) -> storage.Client:
        return self._gcs_client