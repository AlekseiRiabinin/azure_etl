FROM apache/hive:3.1.3

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    postgresql-client \
    curl \
    netcat-openbsd \
  && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HIVE_HOME=/opt/hive \
    HIVE_LIB_DIR=/opt/hive/lib \
    HADOOP_VERSION=3.3.6 \
    HADOOP_HOME=/opt/hadoop \
    PATH=/opt/hive/bin:/opt/hadoop/bin:$PATH

# Download and install Hadoop 3.3.6 (3.3.x+ required for IOStatisticsSource)
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
  && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
  && ln -s /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
  && rm hadoop-${HADOOP_VERSION}.tar.gz

# Clean out any old Hadoop JARs that might conflict in Hive lib
RUN find $HIVE_LIB_DIR -name 'hadoop-common-*.jar' -exec rm -f {} \; \
  && find $HIVE_LIB_DIR -name 'hadoop-aws-*.jar' -exec rm -f {} \; \
  && find $HIVE_LIB_DIR -name 'aws-java-sdk-bundle-*.jar' -exec rm -f {} \;

# DEBUG: List Hadoop JAR files to verify filenames before copying
RUN ls -l $HADOOP_HOME/share/hadoop/common/
RUN ls -l $HADOOP_HOME/share/hadoop/tools/lib/

# Copy required Hadoop JARs from Hadoop 3.3.6 to Hive lib (using wildcards)
RUN cp $HADOOP_HOME/share/hadoop/common/hadoop-common-*.jar $HIVE_LIB_DIR/ \
  && cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-*.jar $HIVE_LIB_DIR/ \
  && cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar $HIVE_LIB_DIR/ \
  && wget -P $HIVE_LIB_DIR https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Configure logging
RUN echo "log4j.rootLogger=INFO, console" > $HIVE_HOME/conf/log4j.properties \
  && echo "log4j.appender.console=org.apache.log4j.ConsoleAppender" >> $HIVE_HOME/conf/log4j.properties \
  && echo "log4j.appender.console.layout=org.apache.log4j.PatternLayout" >> $HIVE_HOME/conf/log4j.properties \
  && echo "log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n" >> $HIVE_HOME/conf/log4j.properties

# Create writable directory for init marker
RUN mkdir -p /var/hive && chown hive:hive /var/hive

# Copy entrypoint and make executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER hive

ENTRYPOINT ["/entrypoint.sh"]
