FROM python:3.11-slim

WORKDIR /opt

ENV PYTHONPATH=/opt:$PYTHONPATH

COPY data_handler/requirements.txt ./
RUN  pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

COPY data_handler ./app