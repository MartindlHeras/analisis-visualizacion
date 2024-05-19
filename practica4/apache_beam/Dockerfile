FROM python:3.11-buster
WORKDIR /app
COPY . .
RUN pip install apache_beam
ENTRYPOINT python etl_apache_beam.py