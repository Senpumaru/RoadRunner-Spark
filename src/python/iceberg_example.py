from pyspark.sql import SparkSession
from pyspark import SparkConf

# Create SparkConf
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.iceberg.type", "rest")
conf.set("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
conf.set("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse")
conf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
conf.set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2")

# Create SparkSession with Iceberg configuration
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .master("spark://spark-master:7077") \
    .config(conf=conf) \
    .getOrCreate()


# Create a database in the Iceberg catalog
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.mydb")

# Create an Iceberg table
spark.sql("CREATE TABLE IF NOT EXISTS iceberg.mydb.my_table (id INT, data STRING) USING iceberg")

# Write data to the Iceberg table
data = [(1, "a"), (2, "b"), (3, "c")]
df = spark.createDataFrame(data, ["id", "data"])
df.writeTo("iceberg.mydb.my_table").append()

# Read data from the Iceberg table
result = spark.table("iceberg.mydb.my_table")
result.show()

# Don't forget to stop the SparkSession
spark.stop()