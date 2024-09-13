FROM bitnami/spark:3.5.2

USER root

# Install wget
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Download Iceberg Spark Runtime
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

# Download AWS bundle
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.2/iceberg-aws-bundle-1.4.2.jar -O /opt/bitnami/spark/jars/iceberg-aws-bundle-1.4.2.jar

# Download Hadoop AWS and AWS Java SDK bundle
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER 1001