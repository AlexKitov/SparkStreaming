package org.company.temperature

import org.apache.spark.sql.SparkSession

object AppSparkConf {

  implicit val spark:SparkSession = SparkSession
    .builder
    .master(AppConfig.master)
    .appName(AppConfig.appName)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

}
