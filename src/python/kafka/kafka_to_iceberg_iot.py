from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time

# Define schema for IoT data
iot_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("battery_level", DoubleType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToIcebergIoT V1.2") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://test/crit") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

# Create Iceberg table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.default.iot_data (
    device_id STRING,
    timestamp TIMESTAMP,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    battery_level DOUBLE,
    date DATE
)
USING iceberg
PARTITIONED BY (device_id, date)
""")

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "processed_iot_data") \
    .option("startingOffsets", "earliest") \
    .option("kafkaConsumer.pollTimeoutMs", 1000) \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()

# Parse JSON and select fields
parsed_df = df.select(
    from_json(col("value").cast("string"), iot_schema).alias("data")
).select("data.*")

# Convert timestamp to proper format and add date column for partitioning
processed_df = parsed_df \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withColumn("date", to_date("timestamp"))  # Changed this line

# Basic data quality checks
# quality_df = processed_df \
#     .filter(col("device_id").isNotNull()) \
#     .filter(col("timestamp").isNotNull()) \
#     .filter(col("temperature").between(-50, 50)) \
#     .filter(col("humidity").between(0, 100)) \
#     .filter(col("pressure").between(900, 1100)) \
#     .filter(col("battery_level").between(0, 100))

# Write to Iceberg table
query = processed_df.writeStream \
    .outputMode("append") \
    .format("iceberg") \
    .option("path", "iceberg.default.iot_data") \
    .option("checkpointLocation", "s3a://test/crit/checkpoints/iot_data") \
    .trigger(processingTime='1 minute') \
    .start()

# Wait for the streaming query to terminate
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping the streaming query...")
    query.stop()
    time.sleep(10)  # Give some time for the query to stop gracefully
finally:
    print("Streaming query stopped.")
    spark.stop()