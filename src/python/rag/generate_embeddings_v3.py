from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sentence_transformers import SentenceTransformer
import torch

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OptimizedEmbeddingsGeneration") \
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

# Load data from PostgreSQL (with parallelism)
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/roadrunner") \
    .option("dbtable", "customers") \
    .option("user", "roadrunner") \
    .option("password", "roadrunner_password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Define batch size and repartition for parallelism
batch_size = 1000
num_partitions = 10  # Adjust based on the cluster capacity
df = df.repartition(num_partitions)

# Load the embedding model
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

# UDF to generate embeddings in batches
def generate_embeddings(text_list):
    embeddings = model.encode(text_list, convert_to_tensor=True, show_progress_bar=False)
    return embeddings.cpu().numpy().tolist()

# Register UDF with parallelism
generate_embeddings_udf = spark.udf.register("generate_embeddings", generate_embeddings, returnType="array<float>")

# Apply the UDF in parallel using batches and processing each partition separately
# Fix: Avoid selecting 'name' twice
df_with_embeddings = df.select("id", "name", "email") \
    .withColumn("embeddings", generate_embeddings_udf(col("name")))

# Write to Iceberg in batches, preserving scalability
df_with_embeddings.write \
    .format("iceberg") \
    .mode("append") \
    .option("write-distribution-mode", "hash") \
    .option("hash-partition-by", "id") \
    .option("batch-size", batch_size) \
    .save("iceberg.customers_db.customers")

# Stop Spark session
spark.stop()
