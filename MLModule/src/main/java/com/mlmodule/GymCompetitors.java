package com.mlmodule;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Gym Competitors")
                .config("spark.sql.warehouse.dir", "tmp/")
                .master("local[*]").getOrCreate();

        Dataset<Row> csvData = spark.read().option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/GymCompetition.csv");

        csvData.printSchema();

        StringIndexer genderIndexer = new StringIndexer()
                .setInputCol("Gender")
                .setOutputCol("GenderIndex");

        csvData = genderIndexer.fit(csvData).transform(csvData);


        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[]{"Age", "Height", "Weight"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDatawithFeatures = vectorAssembler.transform(csvData);

        Dataset<Row> modelInputData = csvDatawithFeatures
                .select("NoOfReps", "features")
                .withColumnRenamed("NoOfReps", "label");

        modelInputData.show();

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(modelInputData);
        System.out.println("intercept: "+ model.intercept() + " coeff: " + model.coefficients());

        model.transform(modelInputData).show();

        spark.close();

    }
}
