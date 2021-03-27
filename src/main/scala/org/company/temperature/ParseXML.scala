package org.company.temperature

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.company.temperature.Main.conf
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import java.io.BufferedOutputStream
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, NodeSeq}

object ParseXML {

  val failedPath = conf.getString("hdfs.path.failPath")
  private val xmlStr = """<?xml version="1.0" encoding="UTF-8"?>
                 |<data>
                 |    <city>London</city>
                 |    <temperature>
                 |        <value unit="celsius">25</value>
                 |        <value unit="fahrenheit">77.5</value>
                 |    </temperature>
                 |    <measured_at_ts>2020-08-02T18:02:00</measured_at_ts>
                 |</data>""".stripMargin

  def strToDate(dateTime: String): DateTime = {
    val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
    val jodatime: DateTime = dtf.parseDateTime(dateTime);
    jodatime
  }

  def parseNode(node: scala.xml.Node): Try[Measurement] =
    Try({
      val city = (node \\ "city").head.text
      val tempAndUnit = (node \\ "temperature")
      val measured_at_ts = strToDate((node \\ "measured_at_ts").head.text)

      val tempMap: Map[String, Option[Double]] = (tempAndUnit \\ "value")
        .map(
          valueTag =>
            (valueTag \@ "unit") ->
              Option(valueTag)
                .map(_.text)
                .map(_.toDouble)
        )
        .toMap
        .withDefaultValue(None)


      Measurement(city, tempMap("celsius"), tempMap("fahrenheit"), measured_at_ts)
    })

  // TODO split into multiple functions
  def handleError(node: scala.xml.Node, e: Throwable, ss: SparkSession): Unit = {
    val printer = new scala.xml.PrettyPrinter(80, 2)
    val path = new Path(s"${failedPath}/failed.txt")
    val conf: Configuration = new Configuration(ss.sparkContext.hadoopConfiguration)
    conf.setInt("dfs.blocksize", 16 * 1024 * 1024) // 16MB HDFS Block Size
    val fs = path.getFileSystem(conf)
    if (fs.exists(path))
      fs.delete(path, true)
    val out = new BufferedOutputStream(fs.create(path))
    out.write(s"${printer.format(node)}\n\nERROR>$e\n".toString.getBytes("UTF-8"))
    out.flush()
    out.close()
    fs.close()
  }

  def writeError(text: String, e: Throwable, ss: SparkSession): Unit = {
    val path = new Path(s"${failedPath}/failed.txt")
    val conf: Configuration = new Configuration(ss.sparkContext.hadoopConfiguration)
    conf.setInt("dfs.blocksize", 16 * 1024 * 1024) // 16MB HDFS Block Size
    val fs = path.getFileSystem(conf)
    if (fs.exists(path))
      fs.delete(path, true)
    val out = new BufferedOutputStream(fs.create(path))
    out.write(s"$text\nERROR > $e".getBytes("UTF-8"))
    out.flush()
    out.close()
    fs.close()
  }

  def parseData(node: scala.xml.Node, ss: SparkSession): Try[Measurement] = {
    val maybeMeasurement = parseNode(node)
    maybeMeasurement match  {
      case Success(measurement) => Success(measurement)
      case Failure(e) => {
        handleError(node, e, ss)
        println("### Some error: ")
        println(e)
        Failure(e)
      }
    }
  }
  def parseXML (xmlStr: String, ss: SparkSession): Seq[Measurement] = {
    val xml = Try(scala.xml.XML.loadString(xmlStr))

    xml match {
      case Failure(e) => {
        writeError(xmlStr, e, ss)
        println("### BIG error: ")
        println(e)
        Seq[Measurement]()
      }
      case Success(xml) => {
        val data = for {
          node <- xml \\ "data"
        } yield parseData(node, ss)

        (xml \\ "data")
          .map(parseData(_, ss))
          .filter(_.isSuccess)
          .map{case Success(value) => value}
      }
    }

  }

}
