FROM openjdk:11.0.11-jre-slim-buster as spark-base

ENV SPARK_VERSION=3.2.1 \
SPARK_HOME=/opt/spark

RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get install -y procps \
  && rm -rf /var/lib/apt/lists/*


# Download and uncompress spark from the apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2-scala2.13.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz



WORKDIR /opt/spark

# RUN mvn -DskipTests clean package

# Apache spark master
FROM spark-base as spark-master

WORKDIR /opt/spark

EXPOSE 7077
EXPOSE 8080

ENTRYPOINT ["/bin/sh", "-c", "/opt/spark/sbin/start-master.sh && tail -f /dev/null"]

# Apache spark worker
FROM spark-base as spark-worker

WORKDIR /opt/spark

EXPOSE 8081

ENTRYPOINT ["/bin/sh", "-c", "/opt/spark/sbin/start-worker.sh spark://spark-master:7077 && tail -f /dev/null"]