package org.company.temperature

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.company.temperature.DataModels._


object RunCheckWarehouse extends App {

  val appConf = Config

  val spark:SparkSession = SparkSession
    .builder
    .master(appConf.master)
    .appName(appConf.appName)
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")


  val storage = spark.read
    .parquet(appConf.temperaturePath)
    .as[LocationMeasurement]
    .cache()

  storage.show
  println(storage.count())

  println( "TERMINATE!" )
}
