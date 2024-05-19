FROM python:3.11-buster
WORKDIR /app
COPY . .
RUN pip install pymysql sqlalchemy pandas cryptography
ENTRYPOINT python insert_mysql_sqlalchemy.py