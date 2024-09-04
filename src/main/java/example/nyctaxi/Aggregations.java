package com.example.nyctaxi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Aggregations {
    public static void performAggregations(Dataset<Row> df) {
        // Average fare amount by passenger count
        Dataset<Row> avgFareByPassengers = df.groupBy("passenger_count")
            .agg(avg("fare_amount").alias("avg_fare"),
                 count("*").alias("trip_count"))
            .orderBy(col("passenger_count"));
        
        System.out.println("Average fare amount by passenger count:");
        avgFareByPassengers.show();

        // Daily revenue
        Dataset<Row> dailyRevenue = df.groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("date"))
            .agg(sum("total_amount").alias("daily_revenue"))
            .orderBy(col("date"));
        
        System.out.println("Daily revenue:");
        dailyRevenue.show();

        // Top 10 pickup locations by trip count
        Dataset<Row> topPickupLocations = df.groupBy("PULocationID")
            .agg(count("*").alias("trip_count"))
            .orderBy(col("trip_count").desc())
            .limit(10);
        
        System.out.println("Top 10 pickup locations:");
        topPickupLocations.show();
    }
}