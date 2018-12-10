package com.dbs.bootcamp

import org.apache.spark.sql.SparkSession

/*
 * This class is used to read the kafka topic and process the data through Structured streaming
 */

object KafkaWordCountStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Aug20")
      .option("startingOffsets", "earliest")
      .load()
      
//      val consoleOutput = df.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//    consoleOutput.awaitTermination()
      
      df.printSchema()
    

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    
    val co = data.writeStream
      .outputMode("append")
      .format("console")
      .start()
//        co.awaitTermination()

    val results = data
                    .map(_._1)
                    .flatMap(value => value.split("\\s+"))
                    .groupByKey(_.toLowerCase)
                    .count()

    val query = results.writeStream.format("console").outputMode("complete").start()
//    query.awaitTermination()
    spark.streams.awaitAnyTermination()

  }
}