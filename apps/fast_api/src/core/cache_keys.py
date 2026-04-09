class CacheKeyBuilder:

    @staticmethod
    def film_detail(film_id: str) -> str:
        return f'film:{film_id}'

    @staticmethod
    def films_list(
            sort: str | None = None,
            offset: int = 0,
            limit: int = 50,
            genre: str | None = None,
            query: str | None = None,
    ) -> str:
        key_parts = ['films', f'offset:{offset}', f'limit:{limit}']

        if sort:
            key_parts.append(f'sort:{sort}')
        if genre:
            key_parts.append(f'genre:{genre}')
        if query:
            key_parts.append(f'query:{query}')

        return ':'.join(key_parts)