FROM apache/airflow:2.8.1-python3.10

# PySpark needs a JVM; the base image ships none. default-jdk-headless pulls
# in whatever JDK is current for the base OS and symlinks it at
# /usr/lib/jvm/default-java, so we don't have to hardcode a Java version here.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
