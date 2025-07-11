FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    chmod 644 /opt/spark/jars/*.jar

RUN mkdir -p /home/jovyan/.cache/pip /home/jovyan/.local/share/jupyter/runtime && \
    chown -R jovyan:users /home/jovyan && \
    chmod 755 /home/jovyan && \
    chmod 700 /home/jovyan/.local/share/jupyter/runtime

USER jovyan

RUN pip install --no-cache-dir \
    apache-airflow==2.9.1 \
    psycopg2-binary \
    s3fs \
    pandas \
    requests \
    jupyterlab-git \
    jupyterlab-dash \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt

RUN jupyter lab build
