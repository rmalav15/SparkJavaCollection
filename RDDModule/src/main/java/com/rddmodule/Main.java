package com.rddmodule;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "")
                .toLowerCase())
                .filter(sentences -> sentences.trim().length() > 0)
                .flatMap(sentences -> Arrays.asList(sentences.split("\\s")).iterator())
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<String, Long>(word, 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(50)
                .forEach(System.out::println);


        sc.close();
    }


}
