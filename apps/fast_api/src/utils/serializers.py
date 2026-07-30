from typing import Any, TypeVar

import orjson

T = TypeVar("T")


def orjson_dumps(data: Any) -> bytes:
    return orjson.dumps(
        data, default=lambda x: x.dict() if hasattr(x, "dict") else None
    )


def orjson_loads(data: bytes) -> Any:
    return orjson.loads(data)


def deserialize_list(data: bytes, model_class: type[T]) -> list[T]:
    return [model_class(**item) for item in orjson_loads(data)]
