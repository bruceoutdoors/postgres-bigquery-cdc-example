FROM python:3.8-slim

WORKDIR /kafpubsub/

COPY requirements.txt /kafpubsub/
RUN pip install -r requirements.txt

COPY kafpubsub.py /kafpubsub/
