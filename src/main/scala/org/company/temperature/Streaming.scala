package org.company.temperature

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.ParseXML.parseXML
import DataModels._
import org.apache.spark.streaming.dstream.DStream
import org.company.temperature.CheckWarehouse.appConf


object Streaming extends App {

  val appConf = Config()
  println(appConf.isResolved)

  val dataPathString = appConf.getString("hdfs.path.dataPath")
  println(dataPathString)
  val temperaturePath = appConf.getString("hdfs.path.temperaturePath")
  println(temperaturePath)

  val skipPattern = appConf.getString("xml.skip.pattern")
  println(skipPattern)

  implicit val spark:SparkSession = SparkSession
    .builder
    .master(appConf.getString("spark.master"))
    .appName(appConf.getString("spark.app.name"))
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

  val pollInterval=appConf.getInt("spark.poll.interval")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(pollInterval))

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  val lines: DStream[String] = ssc.textFileStream(dataPathString)

  val data = lines
    .filter(line => !line.startsWith(skipPattern))
    .reduce(_ + " " + _)
    .flatMap(_.split("<data>").toList)
    .filter(_.nonEmpty)
    .map("<data>"+_)
    .flatMap(parseXML(_))
    .map(MeasurementWithCountry(_))
    .cache

  data.print()

  data.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      println("##foreachRDD")
      val storage = rdd.toDS().cache
      storage.show
      storage
        .write
        .mode(SaveMode.Append)
        .parquet(temperaturePath)

//      TODO Fix when bug is fixed or try with mapWithState
//      https://issues.apache.org/jira/browse/SPARK-16087
//      val ds_vis: Dataset[LocationMeasurement] = spark.read
//        .parquet("hdfs://localhost:9000/warehouse/dashboard/dash.parquet")
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
//        .parquet("hdfs://localhost:9000/warehouse/dashboard.parquet")
    }
  })

  ssc.start()
  ssc.awaitTermination()

  println( "TERMINATE!" )
}
