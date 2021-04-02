package org.company.temperature

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.ParseXML.parseXML
import DataModels._
import org.apache.spark.streaming.dstream.DStream
import org.company.temperature.AppSparkConf.spark

object RunStreaming extends App {

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(AppConfig.pollingInterval))
  ssc.checkpoint(AppConfig.checkpointLocation)

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  //TODO handle xml._COPYING_ case
  val consumer: DStream[String] = ssc.textFileStream(AppConfig.dataPathString)

  val processor = consumer
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

  LatestMeasurementStream(processor)

  ssc.start()
  ssc.awaitTermination()

  println( "TERMINATE!" )
}