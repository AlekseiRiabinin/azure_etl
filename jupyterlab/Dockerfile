FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Create directory for Spark jars and download all required dependencies
RUN mkdir -p /opt/spark/jars && \
    # Clean any existing Kafka/S3 jars to prevent conflicts
    find /opt/spark/jars -maxdepth 1 -type f \( -name "*kafka*" -o -name "*aws*" -o -name "*hadoop-aws*" -o -name "*postgresql*" \) -delete && \
    echo "Starting dependency downloads..." && \
    # 1. Kafka Connector and Core Dependencies
    echo "Downloading Kafka dependencies..." && \
    { \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar \
        -o /opt/spark/jars/kafka-clients-3.5.0.jar && \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/3.5.0/kafka_2.12-3.5.0.jar \
        -o /opt/spark/jars/kafka_2.12-3.5.0.jar && \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
        -o /opt/spark/jars/commons-pool2-2.11.1.jar; \
    } && \
    # 2. Kafka Compression Libraries
    echo "Downloading compression libraries..." && \
    { \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar \
        -o /opt/spark/jars/lz4-java-1.8.0.jar && \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.1/snappy-java-1.1.10.1.jar \
        -o /opt/spark/jars/snappy-java-1.1.10.1.jar; \
    } && \
    # 3. S3 and Cloud Storage Support
    echo "Downloading S3 support libraries..." && \
    { \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
      curl -f -L --retry 3 --retry-delay 5 \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar; \
    } && \
    # 4. Database Drivers
    echo "Downloading database drivers..." && \
    curl -f -L --retry 3 --retry-delay 5 \
      https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
      -o /opt/spark/jars/postgresql-42.7.3.jar && \
    # Verification
    echo "Verifying downloads..." && \
    for jar in \
      spark-sql-kafka-0-10_2.12-3.5.0.jar \
      kafka-clients-3.5.0.jar \
      kafka_2.12-3.5.0.jar \
      commons-pool2-2.11.1.jar \
      hadoop-aws-3.3.4.jar \
      aws-java-sdk-bundle-1.12.262.jar \
      postgresql-42.7.3.jar; do \
      if [ ! -f "/opt/spark/jars/${jar}" ]; then \
        echo "ERROR: Missing required JAR: ${jar}"; \
        exit 1; \
      fi; \
    done && \
    # Final setup
    echo "Setting permissions..." && \
    chmod 644 /opt/spark/jars/*.jar && \
    echo "Creating checksums..." && \
    sha256sum /opt/spark/jars/*.jar > /opt/spark/jars/jars.sha256 && \
    echo "✅ All dependencies successfully installed"

# Configure Ivy cache
RUN mkdir -p /home/jovyan/.ivy2 && \
    chown jovyan:users /home/jovyan/.ivy2
ENV IVY_CACHE_DIR=/home/jovyan/.ivy2

# Configure Spark to use pre-downloaded jars
ENV SPARK_JARS_DIR=/opt/spark/jars
ENV SPARK_OPTS="--driver-class-path=${SPARK_JARS_DIR}/* --jars=${SPARK_JARS_DIR}/* -Divy.home=/home/jovyan/.ivy2 --conf spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true"

# Add health check to verify Spark+Kafka dependencies
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD java -cp "/opt/spark/jars/*" org.apache.spark.sql.kafka010.KafkaSourceProvider || exit 1

# Set up directories and permissions for Jupyter user
RUN mkdir -p /home/jovyan/.cache/pip /home/jovyan/.local/share/jupyter/runtime && \
    chown -R jovyan:users /home/jovyan && \
    chmod 755 /home/jovyan && \
    chmod 700 /home/jovyan/.local/share/jupyter/runtime

USER jovyan

# Install Python packages with pip
RUN pip install --no-cache-dir \
    apache-airflow==2.9.1 \
    psycopg2-binary \
    s3fs \
    pandas \
    requests \
    jupyterlab-git \
    jupyterlab-dash \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt

RUN pip install --no-cache-dir \
    ipython \
    matplotlib \
    seaborn

# Clean and rebuild JupyterLab properly
RUN jupyter lab clean && \
    jupyter lab build --minimize=False && \
    mkdir -p /home/jovyan/.jupyter/lab/{user-settings,workspaces} && \
    chmod -R 700 /home/jovyan/.jupyter && \
    rm -rf /home/jovyan/.cache/yarn
