package com.rddmodule;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MainCount {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // groupbykey version - not recommended due to performance (see later) AND the iterable
        // is awkward to work with.
		/*sc.parallelize(inputData)
		  .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0] , 1L  ))
		  .groupByKey()
		  .foreach( tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances") );*/

        sc.close();
    }

}
