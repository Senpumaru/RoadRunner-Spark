package com.example.nyctaxi;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class NYCTaxiAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("NYC Taxi Data Analysis")
            .master("spark://spark-master:7077")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate();

        String dataPath = "/app/datasets/nyc_taxi/*.parquet";
        
        Dataset<Row> taxiDF = spark.read().parquet(dataPath);
        
        System.out.println("Total number of records before cleaning: " + taxiDF.count());
        
        Dataset<Row> cleanTaxiDF = DataCleaning.cleanData(taxiDF);
        
        System.out.println("Total number of records after cleaning: " + cleanTaxiDF.count());
        
        // Cache the DataFrame for better performance in subsequent operations
        cleanTaxiDF.cache();

        // Perform aggregations
        Aggregations.performAggregations(cleanTaxiDF);

        // Apply window functions
        WindowFunctions.applyWindowFunctions(cleanTaxiDF);

        // Join with external data
        ExternalDataJoin.joinWithLocationData(spark, cleanTaxiDF);

        // Unpersist the cached DataFrame
        cleanTaxiDF.unpersist();

        spark.stop();
    }
}