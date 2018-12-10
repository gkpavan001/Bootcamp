package com.dbs.bootcamp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object SparkStream {
  def main(args: Array[String]) {
    def aggregateFunc(values: Seq[Int] , runningCount: Option[Int]) =  {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)      
    }
		val conf = new SparkConf().setMaster("local").setAppName("Spark Stream")
//		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(conf, Seconds(10))
		ssc.sparkContext.setLogLevel("ERROR")
		ssc.checkpoint("c:\\cp")
		val lines = ssc.textFileStream("C:/Users/pavan/Desktop/SparkStreamFolder")
		val words = lines.flatMap(line => line.split(" "))
		val pairs = words.map(word =>(word, 1))
		val wordCounts = pairs.reduceByKey(_ + _)
		
//		val counts = lines.flatMap(line => line.split(" ")).map(word =>(word, 1)).reduceByKey(_+_)
//		counts.print()
		val tc = wordCounts.updateStateByKey(aggregateFunc)
		tc.print()
		ssc.start()
		ssc.awaitTermination()
  }
}