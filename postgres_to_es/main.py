import logging
import time
from contextlib import closing

import redis
from db.backoff import backoff
from db.connect_to_dbs import (connect_to_elastic, connect_to_pg,
                               connect_to_redis)
from db.es_schema import MAPPINGS, SETTINGS
from elasticsearch.exceptions import RequestError
from services.db_classes import ETL
from services.state import RedisStorage, State

logging.basicConfig(
    level=logging.INFO,
    filename="logs.log",
    format=(
        '%(asctime)s - %(name)s - %(levelname)s '
        '- %(filename)s:%(lineno)d - %(message)s'
    )
)
logging.info("Логи работают")

ES_INDEX_NAME = 'movies'
POLL_INTERVAL = 60


@backoff()
def create_index(es, index_name: str, mappings: dict = None,
                 settings: dict = None):
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
    except RequestError as err:
        logging.error(f"Ошибка при создании индекса '{index_name}': {err}")
    except Exception as err:
        logging.error(
            f"Неожиданная ошибка при создании индекса '{index_name}': {err}",
            exc_info=True
        )


def main():
    try:
        @backoff()
        def init_index():
            with closing(connect_to_elastic()) as es:
                create_index(es, ES_INDEX_NAME, MAPPINGS, SETTINGS)
        init_index()
        while True:
            try:
                with closing(
                    connect_to_pg()
                ) as pg_conn, closing(
                    connect_to_elastic()
                ) as es_conn, closing(
                    connect_to_redis()
                ) as re_conn:

                    state = State(RedisStorage(re_conn))
                    pg_conn.autocommit = False
                    etl = ETL(pg_conn, es_conn, ES_INDEX_NAME, state)
                    etl.etl()
                    pg_conn.commit()
                logging.info('PostgreSQL подключение закрыто.')
            except Exception as err:
                logging.error(
                    f'Ошибка в ETL цикле: {err}',
                    exc_info=True
                )
            logging.info(
                'Ждём POLL INTERVAL перед перезапуском цикла: '
                f'{POLL_INTERVAL} секунд.'
            )
            time.sleep(POLL_INTERVAL)
    except redis.exceptions.ConnectionError as err:
        logging.error(f"Ошибка подключения к Redis: {err}")
    except Exception as err:
        logging.error(f'Возникла непредвиденная ошибка: {err}')


if __name__ == "__main__":
    main()
