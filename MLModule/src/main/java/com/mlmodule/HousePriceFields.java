package com.mlmodule;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("House Price Analysis")
                .config("spark.sql.warehouse.dir", "tmp/")
                .master("local[*]").getOrCreate();

        Dataset<Row> csvData = spark.read().option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/kc_house_data.csv");

        // csvData.describe().show();
        csvData = csvData.drop("id", "date", "view", "waterfront", "condition", "grade", "yr_renovated"
        , "zipcode", "lat", "long");

        for(String col :  csvData.columns())
        {
            System.out.println("correlation " + col + ": " + csvData.stat().corr("price", col));
        }
    }
}
