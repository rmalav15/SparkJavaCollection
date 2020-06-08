package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class ExamResults {
	
	// This is just for reference, favour Java 8 lambdas
	private static UDF2<String, String,Boolean> hasPassedFunction = new UDF2<String, String, Boolean> () {

		@Override
		public Boolean call(String grade, String subject) throws Exception {
			
			if (subject.equals("Biology"))
			{
				if (grade.startsWith("A")) return true;
				return false;
			}
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}
		
	} ;
	
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		spark.udf().register("hasPassed", (String grade, String subject) -> { 
			
			if (subject.equals("Biology"))
			{
				if (grade.startsWith("A")) return true;
				return false;
			}
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
			
		}, DataTypes.BooleanType  );
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset = dataset.withColumn("pass", callUDF("hasPassed",col("grade"), col("subject") ) );
		
		dataset.show();
	}

}
