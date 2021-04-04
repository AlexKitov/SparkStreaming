package org.company.temperature.DStreaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.company.temperature.AppConfig
import org.company.temperature.DataModels.Measurement
import org.company.temperature.Utils.strToTimestamp
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import java.io.BufferedOutputStream
import scala.util.{Failure, Success, Try}

object ParseXML {

  private val xmlStrExample =
    """<data>
      |    <city>London</city>
      |    <temperature>
      |        <value unit="celsius">25</value>
      |        <value unit="fahrenheit">77.5</value>
      |    </temperature>
      |    <measured_at_ts>2020-08-02T18:02:00</measured_at_ts>
      |</data>""".stripMargin

  def parseXML(xmlStr: String)(implicit ss:SparkSession): Option[Measurement] = {
    val maybeMeasurement = parseNode(xmlStr)
    maybeMeasurement match  {
      case Success(measurement) => Some(measurement)
      case Failure(e) => {
        handleError(xmlStr, e)
        println(s"### ERROR: '$e'")
        None
      }
    }
  }

  private def parseNode(xmlStr: String): Try[Measurement] =
    Try({
      val node = scala.xml.XML.loadString(xmlStr)
      val city = (node \\ "city").head.text
      val tempAndUnit = (node \\ "temperature")
      val measured_at_ts = strToTimestamp((node \\ "measured_at_ts").head.text, AppConfig.xmlDateFormat)

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

  // TODO Simplify this config
  private def handleError(text: String, e: Throwable)(implicit ss:SparkSession): Unit = {
//    val printer = new scala.xml.PrettyPrinter(80, 2)
    val fsConf: Configuration = new Configuration(ss.sparkContext.hadoopConfiguration)
    fsConf.setInt("dfs.blocksize", 16 * 1024 * 1024) // 16MB HDFS Block Size

    val path = new Path(s"${AppConfig.failedPath}/${newDateFileNameString()}.txt")
    val fs = path.getFileSystem(fsConf)
    if (fs.exists(path))
      fs.delete(path, true)
    val out = new BufferedOutputStream(fs.create(path))
//    out.write(s"${printer.format(text)}\nERROR > $e".getBytes("UTF-8"))
    out.write(s"$text\nERROR > $e".getBytes("UTF-8"))

    out.flush()
    out.close()
    fs.close()
  }

  private def newDateFileNameString(): String = {
    val dtf: DateTimeFormatter = DateTimeFormat.forPattern(AppConfig.fileNameDateFormat);
    new DateTime().toString(dtf)
  }
}
