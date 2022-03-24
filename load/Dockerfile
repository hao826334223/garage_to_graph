FROM python:3.8-slim-buster

WORKDIR /app

COPY ./load ./
COPY ./util/ ./util
COPY ./gremlin/gremlin.py ./
COPY ./mysql/mysql.py ./


RUN pip install -r requirements.txt


# ENTRYPOINT python main.py

