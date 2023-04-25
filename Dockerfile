FROM python:3.9

WORKDIR /app

RUN apt-get update && \
    apt-get install -y libpq-dev python3-dev

COPY . /app

RUN pip install --trusted-host pypi.python.org python-dotenv pyModbusTCP numpy psycopg2

CMD ["python", "main.py"]
