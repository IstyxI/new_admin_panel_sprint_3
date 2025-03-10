import logging
from contextlib import closing
import time

import redis
from elasticsearch.exceptions import RequestError

from db.connect_to_dbs import (connect_to_elastic, connect_to_pg,
                               connect_to_redis)
from db.es_schema import MAPPINGS, SETTINGS
from services.db_classes import ETL
from services.state import RedisStorage, State

logging.basicConfig(
    level=logging.INFO,
    filename="logs.log",
    format=(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
)
logging.info("Логи работают")

ES_INDEX_NAME = 'movies'
POLL_INTERVAL = 60


def create_index(es, index_name: str, mappings: dict = None, settings: dict = None):
    """Создает индекс в Elasticsearch."""
    try:
        if not es.indices.exists(index=index_name):
            body = {}
            if settings:
                body['settings'] = settings
            if mappings:
                body['mappings'] = mappings

            es.indices.create(index=index_name, body=body)
            logging.info(f"Индекс '{index_name}' успешно создан.")
        else:
            logging.warning(f"Индекс '{index_name}' уже существует.")
    except RequestError as e:
        logging.error(f"Ошибка при создании индекса '{index_name}': {e}")
    except Exception as e:
        logging.error(
            f"Неожиданная ошибка при создании индекса '{index_name}': {e}")


def main():
    try:
        mappings = MAPPINGS
        settings = SETTINGS
        es = connect_to_elastic()
        re = connect_to_redis()
        state = State(RedisStorage(re))
        create_index(es, ES_INDEX_NAME, mappings, settings)
        while True:
            try:
                with closing(connect_to_pg()) as pg_conn:
                    etl = ETL(pg_conn, es, ES_INDEX_NAME, state)
                    etl.etl()
                    pg_conn.commit()
                logging.info('PostgreSQL подключение закрыто.')
            except Exception as err:
                logging.error(f'Ошибка в ETL цикле: {err}')
            time.sleep(POLL_INTERVAL)
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Ошибка подключения к Redis: {e}")
    except Exception as err:
        logging.error(f'Возникла не предвиденная ошибка: {err}')


if __name__ == "__main__":
    main()
