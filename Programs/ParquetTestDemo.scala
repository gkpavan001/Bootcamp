package com.dbs.bootcamp

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import scala.reflect.io.Directory

/*
 * This class is used to read a csv file from local file system and run queries in DF using Spark SQL 
 * by implementing the Parquet format  
 */

object ParquetTest {
	val sourceFile = "C:\\Users\\pavan\\Desktop\\Superstore.csv"
			val csvOutputPath = "c:\\users\\pavan\\Desktop\\Superstoreout.csv"
			val parquetOutputPath = "c:\\users\\pavan\\Desktop\\Superstoreout.parquet"   
			def main(args: Array[String]) = {
					// Two threads local[2]
					val session:SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
							session.sparkContext.setLogLevel("ERROR")
							//Before write, delete the directory if already exist  
							deleteIfAlreadyExists()
							writeParquet(session)
							readParquet(session)

	}
	def writeParquet(session: SparkSession) = {
			// Read file as DF
			val df = session.sqlContext.read.format("csv").option("header", "true").load(sourceFile).toDF()
					// Write file to parquet
					df.write.parquet(parquetOutputPath)
					df.write.csv(csvOutputPath)
					df.printSchema()
	}

	def readParquet(session: SparkSession) = {
			// read back parquet to DF
			val newDataDF = session.sqlContext.read.parquet(parquetOutputPath)
					println("count is =" +newDataDF.count())
					println("count after drop is =" +newDataDF.dropDuplicates().count())
					import session.implicits._
					newDataDF.filter($"Category"  === "Furniture").where($"Profit" > 150.00).sort($"CustomerName".asc).show()  // Only Furniture and profit > 150
//					val finalDS = newDataDF.filter($"Category"  === "Furniture").groupBy($"shipMode")  // Only Furniture and profit > 150
//					newDataDF.dropDuplicates().show()

	}

	def deleteIfAlreadyExists(): Unit = {
			val csvdirectory = new Directory(new File(csvOutputPath))
					if(csvdirectory.exists)
						csvdirectory.deleteRecursively()
						val parquetdirectory = new Directory(new File(parquetOutputPath))
						if(parquetdirectory.exists)
							parquetdirectory.deleteRecursively()
	}


}