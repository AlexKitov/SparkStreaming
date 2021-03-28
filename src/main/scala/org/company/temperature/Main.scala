package org.company.temperature

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.ParseXML.parseXML
import org.apache.spark.sql.functions._

import DataModels._

object Main extends App {
  type LocationMeasurement = MeasurementWithCountry

  println( "Hello World!" )

  val conf = Config()
  println(conf.isResolved)

  val dataPathString = conf.getString("hdfs.path.dataPath")
  println(dataPathString)

  val skipPattern = Config().getString("xml.skip.pattern")
  println(skipPattern)

  implicit val spark:SparkSession = SparkSession
    .builder
    .master(Config().getString("spark.master"))
    .appName(Config().getString("spark.app.name"))
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

  var ds_vis: Dataset[LocationMeasurement] = spark.createDataset(Seq.empty[LocationMeasurement])

  val pollInterval=Config().getInt("spark.poll.interval")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(pollInterval))

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  val lines = ssc.textFileStream(dataPathString)

  val data = lines
    .filter(line=> !line.startsWith(skipPattern))
    .reduce(_ + " " + _)
    .flatMap(_.split("<data>").toList)
    .filter(_.nonEmpty)
    .map("<data>"+_)
    .flatMap(parseXML(_))
    .map(MeasurementWithCountry(_))

  data.print()

  data.foreachRDD(rdd => {
    val windowSpec = Window
                      .partitionBy(col("city"))
                      .orderBy(col("measured_at_ts") desc_nulls_last)

    ds_vis = (ds_vis union rdd.toDS())
                .withColumn("row_number",row_number over windowSpec)
                .filter("row_number == 1")
                .drop("row_number")
                .as[LocationMeasurement]

    ds_vis.cache().show()
  })

  ssc.start()
  ssc.awaitTermination()

  println( "TERMINATE!" )
}
