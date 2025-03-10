import datetime as dt
import json
import logging
from typing import Any, AnyStr, Dict, Generator, List, Tuple

from psycopg import connection as _connection

from .state import State

logging.basicConfig(
    level=logging.INFO,
    filename="logs.log",
    format=(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
)

BATCH_SIZE = 100


class ETL:
    def __init__(
        self, pg_conn: _connection, es_conn, index_name: str, state: State
    ):
        """
        Инициализирует подключение к бд.
        Запросы будут отправляться в схему content.
        """
        self.conn = pg_conn
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET search_path TO content, public;")
        self.es = es_conn
        self.index_name = index_name
        self.state = state
        self.columns_cache = {}

    def get_columns(self, table_name: str) -> List[str]:
        """Получает список столбцов для указанной таблицы."""
        if table_name in self.columns_cache:
            return self.columns_cache[table_name]
        try:
            self.cursor.execute(
                """
                    SELECT column_name FROM information_schema.columns
                    WHERE 'table_name' = %s AND table_schema = 'content'
                """,
                (table_name,)
            )
            columns = [row[0] for row in self.cursor.fetchall()]
            self.columns_cache[table_name] = columns
            return columns
        except Exception as e:
            logging.error(
                f"Ошибка при получении колонок таблицы {table_name}: {e}")
            return []

    def get_data(self) -> Generator[Any, Any, Any]:
        """Генератор извлекающий данные из Postgres пачками по batch."""
        last_modified = self.state.get_state(
            'last_modified') or dt.datetime.min
        logging.info(f"Получаем данные из film_work начиная с {last_modified}")
        try:
            query = '''
                SELECT * FROM content.film_work
                WHERE modified > %s
                ORDER BY modified
                LIMIT %s
            '''
            self.cursor.execute(
                query, (last_modified, BATCH_SIZE)
            )
            while results := self.cursor.fetchmany(BATCH_SIZE):
                yield results
        except Exception:
            return

    def transform(self, row: List) -> Dict[str, Any]:
        """Преобразование данных для таблицы film_work.

        Args:
            transformed_data (List): список данных о фильме.
        """
        columns = self.get_columns('film_work')
        data = dict(zip(columns, row))
        filmwork_id = data.get('id')

        genres_query = """
            SELECT g.name FROM content.genre_film_work gfw
            JOIN content.genre g ON gfw.genre_id = g.id
            WHERE gfw.film_work_id = %s
        """

        genres_data = self.cursor.execute(genres_query, filmwork_id).fetchall()
        genres = [genre[0] for genre in genres_data]

        persons_query = """
            SELECT p.id, p.full_name, pfw.role
            FROM content.person_film_work pfw
            JOIN content.person p ON pfw.person_id = p.id
            WHERE pfm.film_work_id = %s
        """
        persons = self.cursor.execute(persons_query, (filmwork_id,)).fetchall()
        directors = []
        actors = []
        writers = []

        for person_id, full_name, role in persons:
            person_data = {"id": str(person_id), "name": full_name}
            if role == 'director':
                directors.append(person_data)
            elif role == 'actor':
                actors.append(person_data)
            elif role == 'writer':
                writers.append(person_data)

            return {
                "id": str(data.get('id')),
                "imdb_rating": data.get('rating'),
                "genres": genres,
                "title": data.get('title'),
                "description": data.get('description'),
                "directors_names": [dir.get('name') for dir in directors],
                "actors_names": [actor.get('name') for actor in actors],
                "writers_names": [writer.get('name') for writer in writers],
                "directors": directors,
                "actors": actors,
                "writers": writers,
            }

    def load_data(self, transformed_data: List[dict]) -> None:
        """Загружает отформатированные данные в Elastic Search.

        Args:
            transformed_data (List[dict]): список словарей с
            информацией о фильме.
        """
        bulk_data = []
        for document in transformed_data:
            if not document:
                continue
            bulk_data.append({
                "index": {
                    "_index": self.index_name,
                    "_id": document['id'],
                }
            })
            bulk_data.append(document)
        if bulk_data:
            try:
                response = self.es.bulk(
                    index=self.index_name,
                    body='\n'.join(json.dumps(doc) for doc in bulk_data) + '\n'
                )
                if response.get('errors'):
                    logging.error(f'В bulk произошла ошибка {response}')
                else:
                    logging.info(
                        f'Успешно перенесено {len(transformed_data)} фильмов.')
            except Exception as err:
                logging.debug(
                    f'Ошибка при попытке загрузить данные в ES: {err}')
        else:
            logging.info('Нет данных для загрузки в ES')

    def etl(self):
        """
        Процесс объединяющий все функции ETL.

        Extract: получает данные пачками из таблицы film_work.
        Transform: преобразует данные в понятный для Elastic Search формат.
        Load: загружает данные в Elastic Search.
        """
        logging.info('Начат процесс ETL...')
        for results in self.get_data():
            if not results:
                logging.info('Больше нет данных для обработки.')
                continue

            transformed_data = []
            last_modified = self.state.get_state()

            for row in results:
                doc = self.transform(row)
                if doc:
                    transformed_data.append(doc)
                    columns = self.get_columns('film_work')
                    modified_index = columns.index('modified')
                    last_modified = row.get(modified_index)

            self.load_data(transformed_data)

            if last_modified is not None:
                self.state.set_state({'last_modified': last_modified})
                logging.info(
                    f'Обновили значение last_modified на {last_modified}')
            logging.info('ETL процесс завершён успешно.')
