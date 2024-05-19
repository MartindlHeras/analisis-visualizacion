FROM python:3.11-buster
WORKDIR /app
COPY . .
RUN apt update && apt upgrade -y
RUN apt install default-jdk -y
RUN pip install pyspark
ENTRYPOINT python etl_apache_spark.py