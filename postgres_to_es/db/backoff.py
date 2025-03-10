import logging
import time
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    filename="logs.log",
    format=(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Args:
        start_sleep_time: начальное время ожидания
        factor: во сколько раз нужно увеличивать время ожидания
        на каждой итерации
        border_sleep_time: максимальное время ожидания

    Формула:
        t = start_sleep_time * (factor ^ attempt), если t < border_sleep_time
        t = border_sleep_time, иначе
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            delay = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    logging.error(
                        f'Попытка {attempt} выполнить функцию {func.__name__},'
                        f' вызавала ошибку: {err}')
                    if delay < border_sleep_time:
                        delay = start_sleep_time * (factor ** attempt)
                    else:
                        delay = border_sleep_time
                    time.sleep(delay)
                    attempt += 1
        return inner
    return func_wrapper


def pg_backoff(start_sleep_time=25, factor=5, border_sleep_time=1):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            delay = start_sleep_time
            while True:
                while delay > border_sleep_time:
                    delay -= factor
                    try:
                        return func(*args, **kwargs)
                    except Exception as err:
                        logging.error(
                            f'Попытка {attempt + 1} выполнить функцию {func.__name__},'
                            f' вызвала ошибку: {err}. Delay = {delay}')
                        time.sleep(delay)
                        attempt += 1
                else:
                    while delay < start_sleep_time:
                        delay += factor
                        try:
                            return func(*args, **kwargs)
                        except Exception as err:
                            logging.error(
                                f'Попытка {attempt} выполнить '
                                f'функцию {func.__name__},'
                                f' вызавала ошибку: {err}. Delay = {delay}'
                            )
                            time.sleep(delay)
                            attempt += 1
        return inner
    return func_wrapper
