from typing import Optional, Dict, Any, List


class ElasticQueryBuilder:

    @staticmethod
    def build_films_query(
            sort: Optional[str] = None,
            offset: int = 0,
            limit: int = 50,
            genre: Optional[str] = None,
            query: Optional[str] = None,
    ) -> Dict[str, Any]:

        es_query = {'match_all': {}}

        if query:
            es_query = {
                'multi_match': {
                    'query': query,
                    'fields': ['title', 'description'],
                    'fuzziness': 'AUTO'
                }
            }

        filters = []
        if genre:
            filters.append({
                'nested': {
                    'path': 'genres',
                    'query': {
                        'term': {'genres.uuid': genre}
                    }
                }
            })

        body = {
            'from': offset,
            'size': limit,
            'query': es_query,
        }

        if filters:
            body['query'] = {
                'bool': {
                    'must': es_query,
                    'filter': filters
                }
            }

        if sort:
            body['sort'] = ElasticQueryBuilder._build_sort(sort)

        return body

    @staticmethod
    def _build_sort(sort: str) -> List[Dict]:
        sort_field = sort
        sort_order = 'asc'

        if sort.startswith('-'):
            sort_field = sort[1:]
            sort_order = 'desc'

        sort_config = {sort_field: {'order': sort_order}}

        if sort_field == 'imdb_rating':
            sort_config[sort_field]['missing'] = '_last'

        return [sort_config]