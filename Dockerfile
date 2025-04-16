FROM ubuntu:latest as base

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless wget build-essential libncursesw5-dev libssl-dev curl\
     libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Intall Python
RUN wget https://www.python.org/ftp/python/3.10.13/Python-3.10.13.tgz && \
    tar xzf Python-3.10.13.tgz  && \
    cd Python-3.10.13 && \
    ./configure --enable-optimizations && \
    make install  && \
    ln -s /usr/local/bin/pip3 /usr/local/bin/pip && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    ln -s /usr/local/bin/python3 /usr/local/bin/py

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3.3.4
ENV SPARK_HOME /usr/local/spark

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar zxvf spark-3.5.0-bin-hadoop3.tgz \
    && rm -rf spark-3.5.0-bin-hadoop3.tgz \
    && mv spark-3.5.0-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && rm -rf /usr/local/spark-3.4.0-bin-hadoop3 \
    && ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark

RUN ln -s $SPARK_HOME/bin/spark-submit /usr/local/bin/spark-submit && \
    ln -s $SPARK_HOME/bin/spark-class /usr/local/bin/spark-class


## instalação dos jars para o minIO, Delta e Unity Catalog
RUN curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    && curl -O https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-spark_2.12/0.2.0/unitycatalog-spark_2.12-0.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-client/0.2.0/unitycatalog-client-0.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && mv aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars \
    && mv delta-spark_2.12-3.2.1.jar /usr/local/spark/jars \
    && mv delta-storage-3.2.1.jar /usr/local/spark/jars \
    && mv unitycatalog-spark_2.12-0.2.0.jar /usr/local/spark/jars \
    && mv unitycatalog-client-0.2.0.jar /usr/local/spark/jars \
    && mv hadoop-aws-3.3.4.jar /usr/local/spark/jars

RUN export SPARK_HOME
ENV PATH $PATH:$SPARK_HOME/bin

COPY requirements.txt .
RUN pip install -r requirements.txt

