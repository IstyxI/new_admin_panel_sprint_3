FROM python:3.12-slim

RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev postgresql-client --no-install-recommends

WORKDIR /app

COPY requirements/requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir

COPY . /app/

CMD ["python", "main.py"]