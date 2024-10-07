from pyspark.sql import SparkSession
from pyspark import SparkConf

# Create SparkConf
conf = SparkConf()
conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.iceberg.type", "rest")
conf.set("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
conf.set("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse")
conf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
conf.set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2")

# Create SparkSession
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config(conf=conf) \
    .getOrCreate()

# Print out configuration values
print("Iceberg REST Catalog URL:", spark.conf.get("spark.sql.catalog.iceberg.uri"))
print("S3 Endpoint:", spark.conf.get("spark.sql.catalog.iceberg.s3.endpoint"))
print("Warehouse Location:", spark.conf.get("spark.sql.catalog.iceberg.warehouse"))

# Rest of your code...

# Create a test DataFrame
test_data = spark.createDataFrame([(1, "test")], ["id", "value"])

# Try to write directly to S3
try:
    test_data.write.mode("overwrite").parquet("s3a://warehouse/test_write")
    print("Successfully wrote to S3")
except Exception as e:
    print("Failed to write to S3:", str(e))

# Try to read from S3
try:
    spark.read.parquet("s3a://warehouse/test_write").show()
    print("Successfully read from S3")
except Exception as e:
    print("Failed to read from S3:", str(e))

# Create a database in the Iceberg catalog
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.mydb")

# Create an Iceberg table
spark.sql("CREATE TABLE IF NOT EXISTS iceberg.mydb.guru (id INT, data STRING) USING iceberg")
print("Successfully created a table")