package com.mlmodule;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceAnalysis {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("House Price Analysis")
                .config("spark.sql.warehouse.dir", "tmp/")
                .master("local[*]").getOrCreate();

        Dataset<Row> csvData = spark.read().option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/kc_house_data.csv");

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living"
                , "sqft_lot", "floors", "grade"})
                .setOutputCol("features");
        Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
                .select("price", "features")
                .withColumnRenamed("price", "label");

        //modelInputData.show();

        Dataset<Row>[] randomSplit = modelInputData.randomSplit(new double[]  {0.8, 0.2});
        Dataset<Row> trainTestData = randomSplit[0];
        Dataset<Row> valData = randomSplit[1];

        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder =new ParamGridBuilder();

        ParamMap[] paramMaps = paramGridBuilder
                .addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.5, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);

        TrainValidationSplitModel model= trainValidationSplit.fit(trainTestData);
        LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel();

        System.out.println("train R2: " + lrModel.summary().r2());
        System.out.println("train RMSE: " + lrModel.summary().rootMeanSquaredError());
        System.out.println("---------------------------------------------------------");
        System.out.println("test R2: " + lrModel.evaluate(valData).r2());
        System.out.println("test RMSE: " + lrModel.evaluate(valData).rootMeanSquaredError());
        System.out.println("---------------------------------------------------------");
        System.out.println("intercept: "+ lrModel.intercept() + " coeff: " + lrModel.coefficients());
        System.out.println("reg param: "+ lrModel.getRegParam() + " enet: " + lrModel.getElasticNetParam());
        //linearRegressionModel.transform(testData).show();

        //csvData.show();
    }

}
  