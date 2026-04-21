import json
import os
from typing import Any

from storage.base_storage import BaseStorage
from utils.datetime_converter import DateTimeConverter


class JsonFileStorage(BaseStorage):

    def __init__(self, file_path: str = 'etl_state.json'):
        self.file_path = file_path
        self._create_temp_file()

    def save_state(self, state: dict[str, Any]) -> None:
        with open(self.file_path, 'w') as f:
            json.dump(DateTimeConverter.convert_datetime_to_str(state), f, indent=2)

    def retrieve_state(self) -> dict[str, Any]:
        with open(self.file_path, 'r') as f:
            return DateTimeConverter.convert_str_to_datetime(json.load(f))

    def _create_temp_file(self):
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                json.dump({}, f)
