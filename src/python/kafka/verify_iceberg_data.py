from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IcebergDataVerification V1") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://test/crit") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .getOrCreate()

# Query the Iceberg table
df = spark.sql("SELECT * FROM iceberg.default.iot_data LIMIT 10")

# Show the results
print("Sample data from iceberg.default.iot_data:")
df.show()

# Get the count of records
count = spark.sql("SELECT COUNT(*) FROM iceberg.default.iot_data").collect()[0][0]
print(f"Total number of records: {count}")

# Get some basic statistics
print("Basic statistics:")
spark.sql("""
    SELECT 
        COUNT(DISTINCT device_id) as unique_devices,
        MIN(timestamp) as min_timestamp,
        MAX(timestamp) as max_timestamp,
        AVG(temperature) as avg_temperature,
        AVG(humidity) as avg_humidity,
        AVG(pressure) as avg_pressure,
        AVG(battery_level) as avg_battery_level
    FROM iceberg.default.iot_data
""").show()

# Stop the Spark session
spark.stop()