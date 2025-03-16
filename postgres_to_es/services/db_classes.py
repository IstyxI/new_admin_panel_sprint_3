import datetime as dt
import json
import logging
from typing import Any, Dict, Generator, List

from psycopg import connection as _connection

from .queries import main_query
from .state import State
from db.backoff import backoff

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BATCH_SIZE = 100


class ETL:
    def __init__(
        self, pg_conn: _connection, es_conn, index_name: str, state: State
    ):
        """Инициализирует курсор."""
        self.conn = pg_conn
        self.cursor = self.conn.cursor()
        self.es = es_conn
        self.index_name = index_name
        self.state = state

    def etl(self):
        """
        Процесс объединяющий все функции ETL.

        Extract: получает данные пачками из таблицы film_work.
        Transform: преобразует данные в понятный для Elastic Search формат.
        Load: загружает данные в Elastic Search.
        """
        logger.info('Начат процесс ETL...')

        last_modified = self._get_last_modified()

        if not isinstance(last_modified, dt.datetime):
            logger.error("Некорректный тип last_modified")
            raise TypeError("last_modified должен быть datetime")

        total_transformed = 0
        max_modified = last_modified
        self.execute_query(last_modified)
        while True:
            batch = self.get_data()
            if not batch:
                break
            transformed_data = []
            for row in batch:
                try:
                    doc = self.transform(list(row))
                    if doc:
                        transformed_data.append(doc)
                        curr_mod = row[6].replace(tzinfo=dt.timezone.utc)
                        if (curr_mod and curr_mod > last_modified):
                            last_modified = curr_mod
                    else:
                        logger.warning("Transform вернул None.")
                except Exception as err:
                    logger.error(
                        f"Ошибка при трансформации строки:{err}",
                        exc_info=True
                    )

            if transformed_data:
                try:
                    self.load_data(transformed_data)
                    total_transformed += len(transformed_data)
                except Exception as e:
                    logger.error(
                        f"Ошибка при загрузке данных в ElasticSearch: {e}",
                        exc_info=True
                    )

            logger.info(f'Загружено {len(transformed_data)} документов')

        if max_modified > last_modified:
            self.state.set_state(
                'last_modified', max_modified.isoformat())
            logger.info(f"Обновлено last_modified: {max_modified}")

        logger.info('ETL процесс завершён успешно.')

    @backoff()
    def get_data(self) -> Generator[Any, Any, Any]:
        """Генератор извлекающий данные из Postgres пачками по batch."""
        results = self.cursor.fetchmany(BATCH_SIZE)
        logger.info(f"Получено {len(results)} записей из PostgreSQL")
        return results

    @backoff()
    def execute_query(self, last_modified) -> None:
        self.cursor.execute(
            main_query, (last_modified,)
        )

    def transform(self, row: List) -> Dict[str, Any]:
        """Преобразование данных для таблицы film_work.

        Args:
            transformed_data (List): список данных о фильме.
        """
        try:
            (
                fw_id,
                title,
                description,
                rating,
                type_,
                created,
                modified,
                genres,
                raw_persons
            ) = row

            persons = raw_persons if raw_persons is not None else []

            directors = []
            actors = []
            writers = []
            directors_names = []
            actors_names = []
            writers_names = []

            for person in persons:
                try:
                    if not isinstance(person, dict):
                        logger.warning(f"Некорректный формат person: {person}")
                        continue

                    person_id = person.get('id')
                    role = person.get('role')
                    name = person.get('name')

                    if None in [person_id, role, name]:
                        logger.warning(f"Неполные данные person: {person}")
                        continue

                    person_data = {
                        "id": person_id,
                        "name": name
                    }

                    if role == 'director':
                        directors.append(person_data)
                        directors_names.append(name)
                    elif role == 'actor':
                        actors.append(person_data)
                        actors_names.append(name)
                    elif role == 'writer':
                        writers.append(person_data)
                        writers_names.append(name)
                    else:
                        logger.warning(f"Неизвестная роль: {role}")

                except Exception as e:
                    logger.error(f"Ошибка обработки person: {e}")

            return {
                "id": str(fw_id),
                "imdb_rating": float(rating) if rating else 0.0,
                "genres": genres or [],
                "title": title or "Untitled",
                "description": description or "",
                "directors": directors,
                "actors": actors,
                "writers": writers,
                "directors_names": directors_names,
                "actors_names": actors_names,
                "writers_names": writers_names,
            }

        except Exception as e:
            logger.error(f"Ошибка трансформации строки: {e}")
            raise

    @backoff()
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
                    logger.error(f'В bulk произошла ошибка {response}')
                else:
                    logger.info(
                        f'Успешно перенесено {len(transformed_data)} фильмов.')
            except Exception as err:
                logger.error(
                    f'Ошибка при попытке загрузить данные в ES: {err}')
        else:
            logger.info('Нет данных для загрузки в ES')

    def _get_last_modified(self) -> dt.datetime:
        last_modified_str = self.state.get_state('last_modified')
        if last_modified_str:
            try:
                return dt.datetime.fromisoformat(
                    last_modified_str).replace(tzinfo=dt.timezone.utc)
            except ValueError:
                return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
