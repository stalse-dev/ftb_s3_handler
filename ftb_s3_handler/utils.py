import json
from threading import Lock, Event
import polars as pl

# classes para forçar o desligamento geral no contexto de thread
class ShutdownManager(object):
    _instance = None
    _shutdown_event = None

    # precisa ser thread safe
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ShutdownManager, cls).__new__(cls)
                cls._instance._shutdown_event = Event()
        return cls._instance

    @property
    def shutdown_event(self):
        return self._shutdown_event

    def request_shutdown(self):
        self._shutdown_event.set()

    def is_shutdown_requested(self):
        return self._shutdown_event.is_set()

    def reset(self):
        self._shutdown_event.clear()

def handle_nested_data(df: pl.DataFrame) -> pl.DataFrame:
    processed_df = df.clone()

    for column in df.columns:
        dtype = df[column].dtype

        if dtype in (pl.List, pl.Array):
            processed_df = processed_df.with_columns(
                pl.col(column).map_elements(
                    lambda x: json.dumps(x, default=str) if x is not None else "",
                    return_dtype=pl.Utf8
                ).alias(column)
            )

        if dtype == pl.Struct:
            processed_df = processed_df.with_columns(
                pl.col(column).map_elements(
                    lambda x: json.dumps(x, default=str) if x is not None else "",
                    return_dtype=pl.Utf8
                ).alias(column)
            )

    return processed_df