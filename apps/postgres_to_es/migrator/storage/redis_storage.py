import json
from typing import Any

from storage.base_storage import BaseStorage
from utils.datetime_converter import DateTimeConverter

ETL_STATE_KEY = 'etl_state'


class RedisStorage(BaseStorage):

    def __init__(self, client):
        self.client = client

    def save_state(self, state: dict[str, Any]) -> None:
        self.client.set(ETL_STATE_KEY, json.dumps(DateTimeConverter.convert_datetime_to_str(state)))

    def retrieve_state(self) -> dict[str, Any]:
        if state := self.client.get(ETL_STATE_KEY):
            return DateTimeConverter.convert_str_to_datetime(json.loads(state))
        return {}
