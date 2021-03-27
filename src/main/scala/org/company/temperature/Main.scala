package org.company.temperature

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.ParseXML.parseXML

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

  val ds_vis: Dataset[Measurement] = spark.createDataset(Seq.empty[Measurement])
  val pollInterval=Config().getInt("spark.poll.interval")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(pollInterval))

//  val lines = ssc.socketStream("localhost", 9999) # producer @nc -lk 9999
  val lines = ssc.textFileStream(dataPathString.toString)

  val skipPattern = Config().getString("xml.skip.pattern")

  val data = lines
    .filter(line=> !line.startsWith(skipPattern))
    .reduce(_ + " " + _)
    .flatMap(_.split("<data>").toList)
    .filter(_.nonEmpty)
    .map("<data>"+_)
    .flatMap(parseXML(_))


  data.print()

  ssc.start()
  ssc.awaitTermination()

  println("Another solution below")

//  line.foreachRDD {
//    RDD =>
//      val spark: SparkSession = SparkSession.builder.master("local[3]").appName("SparkByExamples").getOrCreate()
//      spark.sparkContext.setLogLevel("ERROR")
//      import spark.implicits._
//  }

//val spark:SparkSession = SparkSession.builder.master("local[3]").appName("SparkByExamples").getOrCreate()
  //  spark.sparkContext.setLogLevel("ERROR")
//  import spark.implicits._
//  spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation","True")

//  val schema = StructType(List(StructField("line", StringType, true)))



//  val consumer = spark.readStream
//    .option("maxFilesPerTrigger", 1)
//    .textFile(path.toString)
//    .agg(collect_list("value"))
//
//  val producer = consumer.writeStream
//    .format("console")
//    .option("truncate", value = false)
//    .option("numRows", 1000)
//    .outputMode(Complete) // <-- update output mode
//
//
////  val producer = consumer.writeStream.format("console").option("truncate", value = false).option("numRows", 1000).outputMode(Complete) // <-- update output mode
//
//
//  producer.start.awaitTermination()



  println( "TERMINATE!" )
}
