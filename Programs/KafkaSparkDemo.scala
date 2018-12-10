package com.dbs.bootcamp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Calendar
import java.io.BufferedWriter
import java.io.FileWriter

/*
 * This class is used to read the kafka topic and store the data in file using spark streaming
 */

object KafkaSpark{
	def main(args: Array[String]) {
		val ssc = new StreamingContext("local[3]", "Streaming Example", Seconds(5))
		ssc.sparkContext.setLogLevel("ERROR")
				val kafkaParams = Map("bootstrap.servers" -> "localhost:9092", 
						"key.deserializer" -> "org.apache.kafka.common.serialization.LongDeserializer",
						"value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
						ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", 
						ConsumerConfig.GROUP_ID_CONFIG -> Calendar.getInstance().getTime().toString)
				val topics = List("Aug20").toSet
				val lines = KafkaUtils.createDirectStream[Long, String](
						ssc,
						LocationStrategies.PreferConsistent,
						ConsumerStrategies.Subscribe[Long, String](topics, kafkaParams));

		lines.foreachRDD(rdd => {
  			val data = rdd.map(e => {
				println("key =" + e.key() + "value  = " + e.value())
			}).foreach(println)
		})
	
		val output = lines.map(record => (record.value))
		output.saveAsTextFiles("file:///c:/Users/Pavan/Desktop/bootcampOutput/KafkaSparkOutput1")
		ssc.start()
		ssc.awaitTermination()
	}
}


