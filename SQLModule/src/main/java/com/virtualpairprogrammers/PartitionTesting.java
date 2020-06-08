package com.virtualpairprogrammers;

import java.util.Optional;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class PartitionTesting {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");

		System.out.println("Initial RDD Partition Size: " + initialRdd.getNumPartitions());
		
		JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair( inputLine -> {
			String[] cols = inputLine.split(":");
			String level = cols[0];
			String date = cols[1];
			return new Tuple2<>(level, date);
		});
		
		System.out.println("After a narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		
		// Now we're going to do a "wide" transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();
		
		results = results.persist(StorageLevel.MEMORY_AND_DISK());
		
		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");
		
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));
		
		System.out.println(results.count());
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		sc.close();
	}

}
