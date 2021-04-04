package org.company.temperature.DStreaming

import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.{AppConfig, Utils}
import org.company.temperature.DStreaming.DStreamContext.unionSourceConsumers
import org.company.temperature.DStreaming.ParseXML.parseXML
import org.company.temperature.DataModels.{CityTemperature, MeasurementWithCountry}

object CityTemperatureStream {

  import spark.implicits._

  //  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  //TODO handle xml._COPYING_ case
  val streamSources = List(AppConfig.dataStream1, AppConfig.dataStream2, AppConfig.dataStream3)

  private val consumer = unionSourceConsumers(streamSources)

  val cityTemperatureStream: DStream[CityTemperature] = consumer
    .filter(line => !line.startsWith(AppConfig.skipPattern))
    .reduce(_ + " " + _)
    .flatMap(_.split("<data>").toList)
    .filter(_.nonEmpty)
    .map("<data>" + _)
    .flatMap(parseXML(_))
    .map(MeasurementWithCountry(_))
    .map(Utils.fillMissingTemperatures)
    .map(CityTemperature(_))
    .persist
    .cache

  cityTemperatureStream.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      println("#### NEW DATA TO APPEND TO TABLE")
      val producer = rdd.toDS()
      producer.show
      producer
        .write
        .mode(SaveMode.Append)
        .parquet(AppConfig.temperaturePath)
    } else {
      println("#### NO NEW DATA")
    }
  })
}
