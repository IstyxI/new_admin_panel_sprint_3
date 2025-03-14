import os

import psycopg
import redis
from db.backoff import backoff, pg_backoff
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

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
REDIS_DSL = {
    'host': os.environ.get('REDIS_HOST', 'rediska'),
    'port': os.environ.get('REDIS_PORT', 6379)
}


@pg_backoff(40, 20, 1)
def connect_to_pg():
    return psycopg.connect(**DSL)


@backoff()
def connect_to_redis():
    return redis.Redis(host=REDIS_DSL.get('host'), port=REDIS_DSL.get('port'))


@backoff()
def connect_to_elastic():
    return Elasticsearch(
        f'http://{ES_DSL.get('host')}:{ES_DSL.get('port')}'
    )
