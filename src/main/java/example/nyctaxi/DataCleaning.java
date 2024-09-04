package com.example.nyctaxi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class DataCleaning {
    public static Dataset<Row> cleanData(Dataset<Row> df) {
        return df.withColumn("trip_distance", when(col("trip_distance").lt(0), 0).otherwise(col("trip_distance")))
                 .withColumn("fare_amount", when(col("fare_amount").lt(0), 0).otherwise(col("fare_amount")))
                 .withColumn("trip_duration", unix_timestamp(col("tpep_dropoff_datetime"))
                                               .minus(unix_timestamp(col("tpep_pickup_datetime"))))
                 .filter(col("trip_duration").gt(0))
                 .filter(col("passenger_count").gt(0))
                 .dropDuplicates("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID");
    }
}