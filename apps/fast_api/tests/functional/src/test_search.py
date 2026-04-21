import aiohttp
import pytest

from common.settings import get_settings


#  Название теста должно начинаться со слова `test_`
#  Любой тест с асинхронными вызовами нужно оборачивать декоратором `pytest.mark.asyncio`, который следит за запуском и работой цикла событий.

settings = get_settings()

@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'The Star', 'page_number': 1, 'page_size': 50},
                {'status': 200, 'length': 50}
        ),
        (
                {'query': 'query', 'page_number': 1, 'page_size': 50},
                {'status': 200, 'length': 0}
        )
    ]
)
@pytest.mark.asyncio
async def test_search(es_write_data, query_data, expected_answer):
    session = aiohttp.ClientSession()
    url = settings.api.url + '/api/v1/films/search'

    async with session.get(url, params=query_data) as response:
        body = await response.json()
        status = response.status
    await session.close()

    # 4. Проверяем ответ

    assert status == expected_answer['status']
    assert len(body) == expected_answer['length']
