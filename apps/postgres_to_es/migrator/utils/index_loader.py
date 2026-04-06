import json


def load_index_from_json(index_json_path: str) -> dict:
    with open(index_json_path) as index_file:
        return json.load(index_file)
