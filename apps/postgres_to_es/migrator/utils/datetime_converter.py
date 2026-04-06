from datetime import datetime, date
from typing import Any


class DateTimeConverter(object):
    @staticmethod
    def convert_datetime_to_str(obj: Any) -> Any:
        """Рекурсивно преобразует datetime объекты в строки"""
        if isinstance(obj, dict):
            return {key: DateTimeConverter.convert_datetime_to_str(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [DateTimeConverter.convert_datetime_to_str(item) for item in obj]
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        else:
            return obj

    @staticmethod
    def convert_str_to_datetime(obj: Any) -> Any:
        """Рекурсивно преобразует строки с датами обратно в datetime"""
        if isinstance(obj, dict):
            return {key: DateTimeConverter.convert_str_to_datetime(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [DateTimeConverter.convert_str_to_datetime(item) for item in obj]
        elif isinstance(obj, str):
            try:
                return datetime.fromisoformat(obj)
            except (ValueError, TypeError):
                return obj
        else:
            return obj
