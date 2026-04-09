from pathlib import Path

from common import get_logger

logger = get_logger(__name__)


class QueryLoader:

    def __init__(self, sql_dir: str = 'data/sql'):
        self.sql_dir = Path(__file__).resolve().parent.parent / Path(sql_dir)
        self._cache = {}

        if not self.sql_dir.exists():
            raise FileNotFoundError(
                f'SQL directory not found: {self.sql_dir}\n'
                f'Current file: {__file__}\n'
                f'Looking for: {self.sql_dir}'
            )

        logger.info(f'QueryLoader initialized with SQL directory: {self.sql_dir}')

    def load(self, filename: str):
        if filename in self._cache:
            return self._cache[filename]

        filepath = self.sql_dir / f'{filename}.sql'

        with open(filepath, 'r') as f:
            query = f.read()
            self._cache[filename] = query
            return query
