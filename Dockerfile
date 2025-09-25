# Start from the official Airflow image
FROM apache/airflow:2.7.3

# Switch to root to install system packages
USER root

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
# Switch back to airflow user
USER airflow
# Install Airflow Spark provider + OpenLineage provider
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    "apache-airflow-providers-openlineage>=1.8.0"



