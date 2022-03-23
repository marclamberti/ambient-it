FROM apache/airflow:2.2.4

COPY requirements.txt /tmp/

RUN pip install --requirement /tmp/requirements.txt