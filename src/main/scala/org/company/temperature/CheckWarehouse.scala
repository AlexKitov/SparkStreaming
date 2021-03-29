package org.company.temperature

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.DataModels._
import org.company.temperature.ParseXML.parseXML


object CheckWarehouse extends App {

  val appConf = Config()
  println(appConf.isResolved)

  val temperaturePath = appConf.getString("hdfs.path.temperaturePath")
  println(temperaturePath)

  val spark:SparkSession = SparkSession
    .builder
    .master(appConf.getString("spark.master"))
    .appName(appConf.getString("spark.app.name"))
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")


  val storage = spark.read
    .parquet(temperaturePath)
    .as[LocationMeasurement]
    .cache()

  storage.show
  println(storage.count())

  println( "TERMINATE!" )
}
