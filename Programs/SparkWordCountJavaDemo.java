package com.dbs.bootcamp;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCountJavaDemo {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("Wordcount Java Example");
		JavaSparkContext context = new JavaSparkContext(conf);
		context.setLogLevel("ERROR");
		JavaRDD<String> input = context.textFile("C:\\Users\\pavan\\Desktop\\bootcamp\\WordCountFile.txt"); // Explain 2 here
		JavaPairRDD<String, Integer> counts = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
				.reduceByKey((x, y) ->  x +  y)
				.sortByKey();
		counts.saveAsTextFile("C:\\Users\\pavan\\Desktop\\bootcampOutput\\sparkOutput1");
		
//		counts.saveAsTextFile("hdfs://localhost:50071//pavan//sparkOutput5");
		context.close();
	}

}
