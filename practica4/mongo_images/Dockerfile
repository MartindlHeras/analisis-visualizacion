FROM python:3.11-buster
WORKDIR /app
COPY . .
RUN pip install pymongo
ENTRYPOINT python ingest_image.py