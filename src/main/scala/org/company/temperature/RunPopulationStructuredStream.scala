package org.company.temperature

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DataModels.{CityTemperature, MeasurementWithCountry}
import org.company.temperature.ParseXML.parseXML
import org.company.temperature.UDFs._

object RunPopulationStructuredStream extends App {

  import spark.implicits._

  spark.sparkContext.setLogLevel(AppConfig.logLevel)

  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

  val populationSchema = new StructType()
    .add("city", "string")
    .add("country", "string")
    .add("population_M", "double")
    .add("updated_at_ts", "timestamp")

  val streamSources = List(AppConfig.populationStream)
  val createPopulationStream = (path :String) => spark
    .readStream
    .format("json")
    .option("maxFilesPerTrigger", 10)
    .schema(populationSchema)
    .load(path)

  val consumer: DataFrame = streamSources.map(createPopulationStream).reduce(_ union _)

  val processor = consumer

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
    .option("numRows", 200)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .outputMode(Append)   // <-- update output mode

  producerConsole.start.awaitTermination
//  producerParquet.start.awaitTermination


  println( "TERMINATE!" )
}
