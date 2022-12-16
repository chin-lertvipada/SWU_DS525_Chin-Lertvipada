# https://medium.com/@jaroensakyodkantha_654/%E0%B9%82%E0%B8%9B%E0%B8%A3%E0%B9%80%E0%B8%88%E0%B8%84-de-%E0%B8%87%E0%B9%88%E0%B8%B2%E0%B8%A2%E0%B9%86-%E0%B8%9C%E0%B9%88%E0%B8%B2%E0%B8%99-docker-apache-airflow-%E0%B9%81%E0%B8%A5%E0%B8%B0-kubernetes-81296aff01c4
# docker build -t apache/airflow:2.4.1 .
# docker-compose up -d --build

FROM apache/airflow:2.4.1

USER root
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
WORKDIR /app
RUN pip install --no-cache-dir lxml
COPY /Prerequisite/requirements_af.txt /tmp
# RUN pip install --trusted-host pypi.python.org -r requirements_af.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements_af.txt
RUN pip install --upgrade pip ipython ipykernel
RUN ipython kernel install --name "python3" --user
RUN ipython kernel install --name "pyspark" --user
RUN pip install apache-airflow-providers-apache-spark

COPY --chown=airflow:root ./dags /opt/airflow/dags
