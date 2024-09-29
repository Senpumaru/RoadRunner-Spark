from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Define the schema for the Kafka messages
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("battery_level", DoubleType(), True)
])

# Create SparkSession
spark = SparkSession.builder \
    .appName("Kafka to Iceberg") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "processed_iot_data") \
    .load()

# Parse the JSON data
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp to timestamp type and partition columns
final_df = parsed_df \
    .withColumn("timestamp", col("timestamp").cast(LongType())) \
    .withColumn("year", col("timestamp").cast("timestamp").cast("date").cast("string")) \
    .withColumn("month", col("timestamp").cast("timestamp").cast("date").cast("string"))

# Write to Iceberg table
query = final_df \
    .writeStream \
    .outputMode("append") \
    .format("iceberg") \
    .option("path", "iceberg.iot_data") \
    .option("checkpointLocation", "/tmp/checkpoint/iot_data") \
    .partitionBy("device_id", "year", "month") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()