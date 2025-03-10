import logging
import os

import psycopg
import redis
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from db.backoff import backoff, pg_backoff

load_dotenv()

DSL = {
    "dbname": os.environ.get("POSTGRES_DB", 'movies_database'),
    "user": os.environ.get("POSTGRES_USER", 'postgres'),
    "password": os.environ.get("POSTGRES_PASSWORD", '123qwe'),
    "host": os.environ.get("POSTGRES_HOST", 'movies-db'),
    "port": os.environ.get("DB_PORT", 5432),
}

ES_DSL = {
    'host': os.environ.get("ES_HOST", "localhost"),
    'port': int(os.environ.get("ES_PORT", 9200)),
    'scheme': os.environ.get("ES_SCHEME", "http"),
}

logging.basicConfig(
    level=logging.INFO,
    filename="logs.log",
    format=(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
)


@pg_backoff()
def connect_to_pg():
    return psycopg.connect(**DSL)


@backoff()
def connect_to_redis():
    return redis.Redis()


@backoff()
def connect_to_elastic():
    return Elasticsearch(
        f'http://{ES_DSL.get('host')}:{ES_DSL.get('port')}'
    )
