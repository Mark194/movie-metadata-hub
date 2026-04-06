from typing import Dict, Any, List


class DataConverter:

    @staticmethod
    def transform_movie(postgres_movie: Dict[str, Any]) -> Dict[str, Any]:
        persons = postgres_movie.get('persons', [])
        genres = postgres_movie.get('genres', [])

        actors = []
        writers = []
        directors = []
        actors_names = []
        writers_names = []
        directors_names = []

        for person in persons:
            role = person.get('role', '').lower()
            person_data = {
                'id': person['id'],
                'name': person['full_name']
            }
            person_name = person.get('full_name', '')

            if role == 'actor':
                actors.append(person_data)
                if person_name:
                    actors_names.append(person_name)
            elif role == 'writer':
                writers.append(person_data)
                if person_name:
                    writers_names.append(person_name)
            elif role == 'director':
                directors.append(person_data)
                if person_name:
                    directors_names.append(person_name)

        genre_names = [genre.get('name') for genre in genres if genre.get('name')]

        return {
            'id': postgres_movie['id'],
            'title': postgres_movie['title'],
            'description': postgres_movie.get('description'),
            'genres': genre_names,
            'actors': actors,
            'writers': writers,
            'directors': directors,
            'actors_names': actors_names,
            'writers_names': writers_names,
            'directors_names': directors_names
        }

    @staticmethod
    def transform_movies(postgres_movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [DataConverter.transform_movie(movie) for movie in postgres_movies]