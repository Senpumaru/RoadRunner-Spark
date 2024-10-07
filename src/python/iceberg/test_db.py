from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IcebergDatabaseCreation") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://test/crit") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

# Create a new database in Iceberg
database_name = "customers_db"  # Replace with your desired database name
spark.sql(f"CREATE DATABASE IF NOT EXISTS iceberg.{database_name}")

print(f"Database '{database_name}' created successfully!")

# Stop the Spark session
spark.stop()