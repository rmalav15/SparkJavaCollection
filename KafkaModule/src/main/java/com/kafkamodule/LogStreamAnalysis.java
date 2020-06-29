package com.kafkamodule;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreamAnalysis {

	public static void main(String[] args) throws InterruptedException 
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));
		
		JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);
		
		JavaDStream<String> results = inputData.map(item -> item);
		JavaPairDStream<String, Long> pairDStream = results.mapToPair(rawLogMessage -> new Tuple2<> (rawLogMessage.split(",")[0],1L));
		pairDStream = pairDStream.reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(2));
		
		pairDStream.print();
		
		sc.start();
		sc.awaitTermination();
	}

}
