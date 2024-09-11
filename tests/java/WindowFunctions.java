package com.example.nyctaxi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

public class WindowFunctions {
    public static void applyWindowFunctions(Dataset<Row> df) {
        // Rank trips by fare amount within each day
        Window windowSpec = Window.partitionBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
                                  .orderBy(col("fare_amount").desc());
        
        Dataset<Row> rankedTrips = df.withColumn("fare_rank", rank().over(windowSpec));
        
        System.out.println("Top 10 highest fares per day:");
        rankedTrips.filter(col("fare_rank").leq(10))
                   .orderBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"), col("fare_rank"))
                   .show();

        // Calculate moving average of trip distance
        Window movingAvgWindow = Window.orderBy("tpep_pickup_datetime")
                                       .rowsBetween(-6, 0); // 7-day moving average
        
        Dataset<Row> movingAvgDistance = df.withColumn("avg_distance_7day", 
                                            avg("trip_distance").over(movingAvgWindow));
        
        System.out.println("7-day moving average of trip distance:");
        movingAvgDistance.select("tpep_pickup_datetime", "trip_distance", "avg_distance_7day")
                         .orderBy("tpep_pickup_datetime")
                         .show();
    }
}