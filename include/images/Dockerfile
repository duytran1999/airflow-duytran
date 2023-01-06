FROM python:3.10-slim-buster

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY ./stock.py stock.py

# docker build -t stock_image .
# docker tag stock_image:latest stock_image:v1.0.0