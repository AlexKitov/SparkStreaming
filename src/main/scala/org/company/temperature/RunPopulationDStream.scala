package org.company.temperature

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.AppSparkConf.spark

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.company.temperature.DataModels.Population

object RunPopulationDStream extends App {

  implicit val formats: DefaultFormats.type = DefaultFormats

  val ssc = new StreamingContext(spark.sparkContext, Seconds(AppConfig.pollingInterval))
  ssc.checkpoint(AppConfig.checkpointLocation)

  //  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  //TODO handle xml._COPYING_ case
  val streamSources = List(AppConfig.populationStream)

  val consumer = streamSources.map(ssc.textFileStream).reduce(_ union _)

  val processor = consumer
    .map((jsonStr: String) => parse(jsonStr).extract[Population])
    .cache

  processor.print()

  ssc.start()
  ssc.awaitTermination()

  println( "TERMINATE!" )
}
