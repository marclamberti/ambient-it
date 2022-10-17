FROM apache/airflow:2.4.1

COPY requirements.txt /tmp/

RUN pip install --requirement /tmp/requirements.txt