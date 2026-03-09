FROM apache/airflow:2.8.0
RUN pip install pymongo pandas psycopg2-binary
