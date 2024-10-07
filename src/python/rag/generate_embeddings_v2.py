from pyspark.sql import SparkSession
from transformers import pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("GenerateEmbeddingsAndStoreInIceberg") \
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
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

# Load data from PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/roadrunner"
db_table = "customers"
properties = {
    "user": "roadrunner",
    "password": "roadrunner_password",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table=db_table, properties=properties)

# Load Huggingface Transformer model for embeddings
embedder = pipeline('feature-extraction', model='sentence-transformers/all-MiniLM-L6-v2')

# Define a function to generate embeddings using Huggingface pipeline
def generate_embedding(text):
    embeddings = embedder(text)
    return embeddings[0][0]  # Get first embedding layer output

# Register UDF to generate embeddings
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType

generate_embedding_udf = udf(lambda text: generate_embedding(text), ArrayType(FloatType()))

# Apply UDF to generate embeddings for name and email
df_with_embeddings = df.withColumn("name_embedding", generate_embedding_udf(df["name"])) \
                       .withColumn("email_embedding", generate_embedding_udf(df["email"]))

# Define Iceberg schema for embedding table
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("name_embedding", ArrayType(FloatType()), nullable=True),
    StructField("email_embedding", ArrayType(FloatType()), nullable=True),
])

# Write the DataFrame to Iceberg
df_with_embeddings.writeTo("iceberg.customers_db.customers").tableProperty("format-version", "2").createOrReplace()

print("Embeddings generated and stored in Iceberg successfully!")

# Stop the Spark session
spark.stop()
