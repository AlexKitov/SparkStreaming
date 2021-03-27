package org.company.temperature

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.ParseXML.parseXML
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.{Append, Complete, Update}


object Main extends App {
  println( "Hello World!" )
  val conf = Config()
  println(conf.isResolved)
  val dataPathString = conf.getString("hdfs.path.dataPath")

  println(dataPathString)
//  val pathString="file:///home/alkit/code_excercise/Troels/SparkStreaming/src/main/resources/test"
//  val path = new org.apache.hadoop.fs.Path(pathString)


  implicit val spark:SparkSession = SparkSession
    .builder
    .master(Config().getString("spark.master"))
    .appName(Config().getString("spark.app.name"))
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

  var ds_vis: Dataset[Measurement] = spark.createDataset(Seq.empty[Measurement])
  val pollInterval=Config().getInt("spark.poll.interval")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(pollInterval))

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  val lines = ssc.textFileStream(dataPathString.toString)

  val skipPattern = Config().getString("xml.skip.pattern")

//  val data = lines
//    .filter(line=> !line.startsWith(skipPattern))
//    .reduce(_ + " " + _)
//    .flatMap(_.split("<data>").toList)
//    .filter(_.nonEmpty)
//    .map("<data>"+_)
//    .flatMap(parseXML(_))
//
//  data.print()

//  data.foreachRDD(rdd => {
//    val windowSpec = Window.partitionBy(col("city")).orderBy(col("measured_at_ts") desc_nulls_last)
//    ds_vis = (ds_vis union rdd.toDS())
//              .withColumn("row_number",row_number over windowSpec)
//              .filter("row_number == 1")
//              .drop("row_number")
//              .as[Measurement]
//    ds_vis.cache().show()
//  })

//  ssc.start()
//  ssc.awaitTermination()

  println("Another solution below")

//val spark:SparkSession = SparkSession.builder.master("local[3]").appName("SparkByExamples").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
//  import spark.implicits._
//  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

//  val schema = StructType(List(StructField("line", StringType, true)))



  def mkString2(xml:Seq[String]): Seq[String] = {
      xml
        .mkString(" ")
        .split("<data>")
        .filter(_.nonEmpty)
        .map("<data>"+_)

  }

  val mkString2UDF=udf(mkString2 _)

  val consumer = spark.readStream
    .option("maxFilesPerTrigger", 2)
    .textFile(dataPathString)
    .filter(line=> !line.startsWith(skipPattern))
    .withColumn("rank",  lit(1))
    .groupBy("rank")
    .agg(collect_list("value").as("lst"))
    .withColumn("hop", mkString2UDF(col("lst")))
    .select(col("rank"), explode(col("hop")).as("tap"))
    .select("tap")
    .flatMap(r => parseXML(r.getString(0)))

  val windowSpec = Window.partitionBy(col("city")).orderBy(col("measured_at_ts") desc_nulls_last)

//    val df = consumer
//    .withColumn("row_number", row_number over windowSpec)
//    .filter("row_number == 1")
//    .drop("row_number")
//    .as[Measurement]


  val producer = consumer.writeStream
    .format("console")
    .option("truncate", value = false)
    .option("numRows", 10)
    .outputMode(Update) // <-- update output mode


  producer.start.awaitTermination()



  println( "TERMINATE!" )
}
