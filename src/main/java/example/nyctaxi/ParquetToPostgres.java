package com.example.nyctaxi;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ParquetToPostgres {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("ParquetToPostgres")
            .master("spark://spark-master:7077")
            .getOrCreate();

        Dataset<Row> df = spark.read().parquet("/app/datasets/nyc_taxi/*.parquet");

        df.write()
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/roadrunner")
            .option("dbtable", "public.nyc_yellow_taxi_trips")
            .option("user", "roadrunner")
            .option("password", "roadrunner_password")
            .mode("append")
            .save();

        spark.stop();
    }
}