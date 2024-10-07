from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from transformers import AutoTokenizer, AutoModel
import torch

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Generate Customer Embeddings") \
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

# Load Hugging Face model and tokenizer
model_name = "sentence-transformers/all-MiniLM-L6-v2"  # You can choose another model
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

# Define a function to generate embeddings for a batch of texts
def generate_batch_embeddings(texts):
    inputs = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    with torch.no_grad():
        embeddings = model(**inputs).last_hidden_state.mean(dim=1)
    return embeddings.numpy()

# Collect 'name' and 'email' into a list for batch processing
text_batch = df.select("name", "email").rdd.map(lambda row: f"{row.name} {row.email}").collect()

# Generate embeddings for the entire batch
embeddings_batch = generate_batch_embeddings(text_batch)

# Add the embeddings back to a DataFrame
embeddings_df = spark.createDataFrame(zip(df.select("id").rdd.collect(), embeddings_batch.tolist()), ["id", "embeddings"])


# Show the first 5 rows of the embeddings DataFrame
embeddings_df.show(5)

# Stop the Spark session after the job
spark.stop()
