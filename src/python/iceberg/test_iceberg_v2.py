from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IcebergSparkConnectivityTest V2.2") \
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

# Test connectivity by creating a simple table
try:
    spark.sql("CREATE TABLE IF NOT EXISTS iceberg.default.test_connectivity (id INT, name STRING) USING iceberg")
    print("Successfully created test table. Connectivity test passed!")
    
    # Verify the table was created
    tables = spark.sql("SHOW TABLES IN iceberg.default").collect()
    for table in tables:
        print(f"Found table: {table['tableName']}")
    
    # Insert some test data
    spark.sql("INSERT INTO iceberg.default.test_connectivity VALUES (1, 'Test Name')")
    print("Inserted test data successfully.")
    
    # Read the data back
    result = spark.sql("SELECT * FROM iceberg.default.test_connectivity").collect()
    for row in result:
        print(f"Read data: id={row['id']}, name={row['name']}")
    
    # Clean up
    spark.sql("DROP TABLE iceberg.default.test_connectivity")
    print("Test table dropped successfully.")
except Exception as e:
    print(f"Connectivity test failed. Error: {str(e)}")
    import traceback
    traceback.print_exc()

# Stop the Spark session
spark.stop()