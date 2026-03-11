FROM apache/spark:4.1.1-scala2.13-java21-ubuntu AS spark

FROM eclipse-temurin:21-jre AS jre

FROM apache/airflow:3.1.7

USER root

COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-openlineage
