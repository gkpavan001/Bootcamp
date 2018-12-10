package com.dbs.bootcamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset

/*
 * This class is used to read the kafka topic and process the data through Structured streaming.
 * You can apply queries on the topic data which we read from Kafka topic.
 */

object KafkaSparkStreamDemo {
  case class employee(number:Int, name:String,salary:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

      val schema = new StructType()
        .add($"number".string)
        .add($"name".string)
        .add($"salary".string)

      val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "EmpTopic")
      .option("startingOffsets", "latest")
      .load()
      
    // Select the required columns only from the kafka streamed DF. In this case Value is required   
    val data = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]

    data.printSchema()
    println(" ----------------")
    // Need to split the data using delimiter because we get the line as string, and pass it to employee case class.
    val finalDS = data.map(_.split(",")).map(line => employee(line(0).toInt, line(1).toString(),line(2).toInt))
    finalDS.printSchema()

    finalDS.createOrReplaceTempView("emp")
    val sql = "select * from emp"
    val ageAverage = spark.sql(sql);
    
//    val q = finalDS.select(sum(col("salary")).alias("Total Salary DF"))
//    q.writeStream.format("console").outputMode("append").start()
            
    ageAverage.writeStream.format("console").outputMode("append").start()
    spark.streams.awaitAnyTermination() // This is good for any number of queries

  }
}