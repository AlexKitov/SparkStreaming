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

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
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
    .cache

  processor.print()

  processor.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      val producer = rdd.toDS().cache
      producer.show
      producer
        .write
        .mode(SaveMode.Append)
        .parquet(AppConfig.temperaturePath)


//      TODO Fix when bug is fixed or try with mapWithState
//      https://issues.apache.org/jira/browse/SPARK-16087
//      val ds_vis: Dataset[LocationMeasurement] = spark.read
//        .parquet(appConf.dashboardPath)
//        .as[LocationMeasurement]
//        .cache
//
//      val windowSpec = Window
//        .partitionBy(col("city"))
//        .orderBy(col("measured_at_ts") desc_nulls_last)
//
//      val dashDS = (ds_vis.rdd union rdd).toDS()
//        .withColumn("row_number", row_number over windowSpec)
//        .filter("row_number == 1")
//        .drop("row_number")
//        .as[LocationMeasurement]
//        .cache
//
//      dashDS
//        .write
//        .mode(SaveMode.Append)
//        .parquet(appConf.dashboardPath)
    }
  })

  ssc.start()
  ssc.awaitTermination()

  println( "TERMINATE!" )
}
