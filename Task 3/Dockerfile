FROM apache/airflow:2.7.1
USER root
# Install Java
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;
# Create Spark directory
RUN mkdir -p /opt/spark
RUN chown 50000:50000 /opt/spark
# Set JAVA_HOME for arm architecture
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/
# Set JAVA_HOME for amd64 architecture
# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
USER airflow
# Set Spark binaries
RUN pip install --no-cache-dir pyspark
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar xf spark-3.3.0-bin-hadoop3.tgz -C /opt/spark
ENV SPARK_HOME="/opt/spark/spark-3.3.0-bin-hadoop3"
ENV PATH="/opt/spark/spark-3.3.0-bin-hadoop3/bin:${PATH}"

