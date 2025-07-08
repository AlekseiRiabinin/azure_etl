FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN mkdir -p /home/jovyan/.cache/pip /home/jovyan/.local

RUN pip install --user --cache-dir /home/jovyan/.cache/pip \
    apache-airflow==2.9.1 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-amazon \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt

RUN chown -R jovyan:users /home/jovyan

USER jovyan
