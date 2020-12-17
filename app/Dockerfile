ARG BASE_CONTAINER=jupyter/pyspark-notebook:latest
FROM $BASE_CONTAINER
USER root
RUN apt update
RUN apt install python3-dev --yes
RUN apt install libpq-dev python3-dev --yes
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
