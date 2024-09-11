package com.example.nyctaxi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class ExternalDataJoin {
    public static void joinWithLocationData(SparkSession spark, Dataset<Row> df) {
        // Create a sample location dataset
        List<Row> locationData = Arrays.asList(
            Row.apply(1, "Times Square"),
            Row.apply(2, "Central Park"),
            Row.apply(3, "Empire State Building"),
            Row.apply(4, "Statue of Liberty"),
            Row.apply(5, "Broadway Theatre District")
        );
        
        Dataset<Row> locationDF = spark.createDataFrame(locationData, 
                                  new org.apache.spark.sql.types.StructType()
                                      .add("LocationID", "int")
                                      .add("LocationName", "string"));

        // Join taxi data with location data
        Dataset<Row> joinedDF = df.join(locationDF, 
                                        df.col("PULocationID").equalTo(locationDF.col("LocationID")), 
                                        "left_outer");

        // Analyze popular pickup locations
        Dataset<Row> popularLocations = joinedDF.groupBy("LocationName")
                                                .agg(count("*").alias("trip_count"))
                                                .orderBy(col("trip_count").desc());

        System.out.println("Popular pickup locations:");
        popularLocations.show();

        // Average fare by location
        Dataset<Row> avgFareByLocation = joinedDF.groupBy("LocationName")
                                                 .agg(avg("fare_amount").alias("avg_fare"))
                                                 .orderBy(col("avg_fare").desc());

        System.out.println("Average fare by location:");
        avgFareByLocation.show();
    }
}