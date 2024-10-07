from pyspark.sql import SparkSession

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("PostgreSQL Test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Define PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/roadrunner"
db_properties = {
    "user": "roadrunner",
    "password": "roadrunner_password",
    "driver": "org.postgresql.Driver"
}

# Load the 'customer' table into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table="customers", properties=db_properties)

# Show the first 5 rows of the DataFrame to verify the connection and data load
df.show(5)

# Stop the Spark session after the job
spark.stop()
