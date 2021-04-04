package org.company.temperature.StructuredStreaming

import org.apache.spark.sql.functions.{col, collect_list, current_timestamp, explode}
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.streaming.Trigger
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DStreaming.ParseXML.parseXML
import org.company.temperature.DataModels.{CityTemperature, MeasurementWithCountry}
import UDFs.mkString2UDF
import org.company.temperature.{AppConfig, Utils}

object RunStructuredStreaming extends App {

  import spark.implicits._

  val streamSources = List(AppConfig.dataStream1, AppConfig.dataStream2, AppConfig.dataStream3)
  val createStructuredStream = spark.readStream.option("maxFilesPerTrigger", 2).textFile _
  val consumer = streamSources.map(createStructuredStream).reduce(_ union _)

  val processor = consumer
    .filter(line => !line.startsWith(AppConfig.skipPattern))
    .withColumn("timestamp", current_timestamp)
    .withWatermark("timestamp", "5 seconds")
    .groupBy("timestamp")
    .agg(collect_list("value").as("lst"))
    .withColumn("lst_element", mkString2UDF(col("lst")))
    .select(col("timestamp"), explode(col("lst_element")).as("element"))
    .select("element")
    .flatMap(r => parseXML(r.getString(0)))
    .map(MeasurementWithCountry(_))
    .map(Utils.fillMissingTemperatures)
    .map(CityTemperature(_))

  //  val windowSpec = Window
  //    .partitionBy(col("city"))
  //    .orderBy(col("measured_at_ts") desc_nulls_last)
  //  val dashboardProcessor = processor
  //    .withColumn("row_number", row_number over windowSpec)
  //    .filter("row_number == 1")
  //    .drop("row_number")
  //    .as[MeasurementWithTimestamp]
  val producerParquet = processor
    .writeStream
    .format("parquet")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("path", AppConfig.temperaturePath)
    .option("checkpointLocation", AppConfig.checkpointLocation)
    .outputMode(Append)

  val producerConsole = processor.writeStream
    .format("console")
    .option("truncate", value = false)
    .option("numRows", 20)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .outputMode(Append) // <-- update output mode

  producerConsole.start.awaitTermination
  //  producerParquet.start.awaitTermination


  println("TERMINATE!")
}
