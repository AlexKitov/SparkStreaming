package org.company.temperature


import org.apache.spark.sql.{ Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.ParseXML.parseXML
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.{Append, Complete, Update}


object StructuredStreaming extends App {

  val appConfig = Config()
  println(appConfig.isResolved)
  val dataPathString = appConfig.getString("hdfs.path.dataPath")

  println(dataPathString)

  implicit val spark:SparkSession = SparkSession
    .builder
    .master(appConfig.getString("spark.master"))
    .appName(appConfig.getString("spark.app.name"))
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

  var ds_vis: Dataset[Measurement] = spark.createDataset(Seq.empty[Measurement])
  val pollInterval=Config().getInt("spark.poll.interval")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(pollInterval))

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  val lines = ssc.textFileStream(dataPathString.toString)

  val skipPattern = Config().getString("xml.skip.pattern")

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
    .withColumn("timestamp", current_timestamp)
    .withWatermark("timestamp", "1 minutes")
    .groupBy("timestamp")
    .agg(collect_list("value").as("lst"))
    .withColumn("lst_element", mkString2UDF(col("lst")))
    .select(col("timestamp"), explode(col("lst_element")).as("element"))
    .select("element")
    .flatMap(r =>  parseXML(r.getString(0)))
    .persist()

//    val windowSpec = Window
  //    .partitionBy(col("city"))
  //    .orderBy(col("measured_at_ts") desc_nulls_last)
//    val df = consumer
//      .withColumn("row_number", row_number over windowSpec)
//      .filter("row_number == 1")
//      .drop("row_number")
//      .as[MeasurementWithTimestamp]


  val producer = consumer.writeStream
    .format("console")
    .option("truncate", value = false)
    .option("numRows", 10)
    .outputMode(Complete)   // <-- update output mode

  producer.start.awaitTermination()

  println( "TERMINATE!" )
}
