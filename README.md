# RoadRunner-Spark
Apache Spark Microservice (Addon) for Road Runner project.

docker compose up --build -d

## Java
mvn clean package

spark-submit --class com.example.nyctaxi.NYCTaxiAnalysis --master spark://spark-master:7077 target/nyc-taxi-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --class com.example.nyctaxi.NYCTaxiAnalysis --master spark://spark-master:7077 target/nyc-taxi-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar


Optimizations and Best Practices for Large-Scale Spark Processing:

Partitioning:

Ensure proper partitioning of your data. Use repartition() or coalesce() to optimize the number of partitions.
Consider partitioning by a column that's frequently used in joins or filters.


Caching and Persistence:

Use cache() or persist() for DataFrames that are reused multiple times.
Choose appropriate storage levels (e.g., MEMORY_AND_DISK) based on your needs and available resources.


Broadcast Joins:

For joining a large DataFrame with a small one, use broadcast joins to reduce shuffle.
Example: df1.join(broadcast(df2), "joinKey")


Predicate Pushdown:

Apply filters early in your query to reduce the amount of data processed.


Column Pruning:

Select only the columns you need to reduce memory usage and processing time.


Avoid UDFs when possible:

Use built-in functions instead of UDFs when possible, as they're optimized and vectorized.


Use Appropriate Data Formats:

Use columnar formats like Parquet for better performance.


Tune Spark Configurations:

Adjust spark.sql.shuffle.partitions, spark.driver.memory, spark.executor.memory, etc., based on your cluster and data size.


Monitor and Optimize Queries:

Use the Spark UI to identify bottlenecks and optimize slow stages.


Avoid collect() on Large Datasets:

Use take(), sample(), or write results to files instead of collecting large datasets to the driver.