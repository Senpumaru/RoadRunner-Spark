FROM bitnami/spark:3.3.2

USER root

# Install wget
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
COPY requirements.txt .
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt

# Download Iceberg Spark Runtime for Spark 3.3
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.0/iceberg-spark-runtime-3.3_2.12-1.4.0.jar -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.3_2.12-1.4.0.jar

# Download AWS bundle
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.0/iceberg-aws-bundle-1.4.0.jar -O /opt/bitnami/spark/jars/iceberg-aws-bundle-1.4.0.jar

# Download Hadoop AWS and AWS Java SDK bundle
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -O /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Download PostgreSQL JDBC Driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -O /opt/bitnami/spark/jars/postgresql-42.5.0.jar

USER 1001
