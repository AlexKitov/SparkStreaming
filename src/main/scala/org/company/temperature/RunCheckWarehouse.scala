package org.company.temperature

import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DataModels._


object RunCheckWarehouse extends App {

  import spark.implicits._

  val storage = spark.read
    .parquet(AppConfig.temperaturePath)
    .as[CityTemperature]
    .cache()

  storage.show
  println(storage.count())

  println( "TERMINATE!" )
}
