package org.company.temperature

import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DataModels.{CityTemperature, MeasurementWithCountry, Population}
import org.company.temperature.ParseXML.parseXML
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object RunStreaming extends App {

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(AppConfig.pollingInterval))
  ssc.checkpoint(AppConfig.checkpointLocation)

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  //TODO handle xml._COPYING_ case
  val streamSources = List(AppConfig.dataStream1, AppConfig.dataStream2, AppConfig.dataStream3)

  val consumer = streamSources.map(ssc.textFileStream).reduce(_ union _)

  implicit val formats: DefaultFormats.type = DefaultFormats
  val populationConsumer = ssc.textFileStream(AppConfig.populationStream)
  val populationProcessor: DStream[Population] = populationConsumer
    .map((jsonStr: String) => parse(jsonStr).extract[Population])
    .cache

  populationProcessor.print

  val processor: DStream[CityTemperature] = consumer
    .filter(line => !line.startsWith(AppConfig.skipPattern))
    .reduce(_ + " " + _)
    .flatMap(_.split("<data>").toList)
    .filter(_.nonEmpty)
    .map("<data>"+_)
    .flatMap(parseXML(_))
    .map(MeasurementWithCountry(_))
    .map(Utils.fillMissingTemperatures)
    .map(CityTemperature(_))
    .cache

  processor.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      println("#### NEW DATA")
      val producer = rdd.toDS().cache
      producer.show
      producer
        .write
        .mode(SaveMode.Append)
        .parquet(AppConfig.temperaturePath)
    } else {
      println("#### NO NEW DATA")
    }
  })

  LatestMeasurementStream(processor, populationProcessor)

  ssc.start()
  ssc.awaitTermination()

  println( "TERMINATE!" )
}
