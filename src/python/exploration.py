import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HVFHV Trip Records Analysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Java version: {spark.sparkContext.getConf().get('spark.java.version')}")

# Load the Parquet file
df = spark.read.option("mergeSchema", "true").parquet("./datasets/fhvhv_tripdata_2024-01.parquet")

# Basic info
print("\nSample Data:")
df.show(5, truncate=False)

total_records = df.count()
print(f"\nTotal number of records: {total_records}")

print("\nColumn names:")
print(df.columns)

# Trip duration analysis
df = df.withColumn("trip_duration_minutes", 
    (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60)

print("\nTrip Duration Statistics (in minutes):")
df.select(F.mean("trip_duration_minutes"), F.stddev("trip_duration_minutes"), 
          F.min("trip_duration_minutes"), F.max("trip_duration_minutes")).show()

# Busiest hours
df = df.withColumn("pickup_hour", F.hour("pickup_datetime"))
busiest_hours = df.groupBy("pickup_hour").count().orderBy(F.desc("count"))

print("\nTop 5 Busiest Hours:")
busiest_hours.show(5)

# Most common pickup locations
top_pickup_locations = df.groupBy("PULocationID").count().orderBy(F.desc("count"))

print("\nTop 5 Pickup Locations:")
top_pickup_locations.show(5)

# Average fare by hour
avg_fare_by_hour = df.groupBy("pickup_hour").agg(F.mean("base_passenger_fare").alias("avg_fare"))

print("\nAverage Fare by Hour:")
avg_fare_by_hour.orderBy("pickup_hour").show(24)

# Trip distance distribution
print("\nTrip Distance Distribution:")
df.select(F.mean("trip_miles"), F.stddev("trip_miles"), 
          F.min("trip_miles"), F.max("trip_miles"), 
          F.expr("percentile(trip_miles, array(0.25, 0.5, 0.75))").alias("percentiles")).show()

# Distribution of shared rides
print("\nDistribution of Shared Rides:")
df.groupBy("shared_request_flag").count().orderBy(F.desc("count")).show()

# Average tips by hour
avg_tips_by_hour = df.groupBy("pickup_hour").agg(F.mean("tips").alias("avg_tips"))

print("\nAverage Tips by Hour:")
avg_tips_by_hour.orderBy("pickup_hour").show(24)

# Analysis of WAV (Wheelchair Accessible Vehicle) requests
print("\nWAV Request Analysis:")
df.groupBy("wav_request_flag", "wav_match_flag").count().orderBy(F.desc("count")).show()

# Analysis of driver pay vs. base fare
df = df.withColumn("driver_pay_ratio", F.col("driver_pay") / F.col("base_passenger_fare"))
print("\nDriver Pay Ratio Statistics:")
df.select(F.mean("driver_pay_ratio"), F.stddev("driver_pay_ratio"), 
          F.min("driver_pay_ratio"), F.max("driver_pay_ratio")).show()

# Distribution of trips by license
print("\nDistribution of Trips by License:")
df.groupBy("hvfhs_license_num").count().orderBy(F.desc("count")).show()

# Stop the Spark session
spark.stop()