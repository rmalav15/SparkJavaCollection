package com.kafkamodule;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.concurrent.TimeoutException;

public class ViewingFiguresStructured {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("structureViewingReport")
                .getOrCreate();

        session.conf().set("spark.sql.shuffle.partitions", "10");

        Dataset<Row> df = session.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        df.createOrReplaceTempView("viewing_figures");

        // Key Value
        Dataset<Row> result = session
                .sql("select window, cast( value as string) as course_name, sum(5) as seconds_watch " +
                        "from viewing_figures " +
                        "group by window(timestamp, '2 minutes'), course_name");

        StreamingQuery query = result
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .option("truncate", false)
                .option("numRows", 50)
                .start();

        query.awaitTermination();

    }
}
