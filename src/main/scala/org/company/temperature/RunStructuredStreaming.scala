package org.company.temperature

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.company.temperature.ParseXML.parseXML
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.{Append, Complete, Update}
import org.company.temperature.DataModels.MeasurementWithCountry
import org.company.temperature.UDFs._

object RunStructuredStreaming extends App {

  val appConfig = Config

  implicit val spark:SparkSession = SparkSession
    .builder
    .master(appConfig.master)
    .appName(appConfig.appName)
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel(appConfig.logLevel)

  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

  val consumer = spark.readStream
    .option("maxFilesPerTrigger", 2)
    .textFile(appConfig.dataPathString)

  val processor = consumer
    .filter(line=> !line.startsWith(appConfig.skipPattern))
    .withColumn("timestamp", current_timestamp)
    .withWatermark("timestamp", "1 minutes")
    .groupBy("timestamp")
    .agg(collect_list("value").as("lst"))
    .withColumn("lst_element", mkString2UDF(col("lst")))
    .select(col("timestamp"), explode(col("lst_element")).as("element"))
    .select("element")
    .flatMap(r =>  parseXML(r.getString(0)))
    .map(MeasurementWithCountry(_))
    .cache


  processor.show
  processor
    .write
    .mode(SaveMode.Append)
    .parquet(appConfig.temperaturePath)

  //    val windowSpec = Window
  //    .partitionBy(col("city"))
  //    .orderBy(col("measured_at_ts") desc_nulls_last)
//    val df = consumer
//      .withColumn("row_number", row_number over windowSpec)
//      .filter("row_number == 1")
//      .drop("row_number")
//      .as[MeasurementWithTimestamp]


  val producer = processor.writeStream
    .format("console")
    .option("truncate", value = false)
    .option("numRows", 20)
    .outputMode(Complete)   // <-- update output mode

  producer.start.awaitTermination()

  println( "TERMINATE!" )
}
