package org.company.temperature

import org.apache.spark.sql.functions.udf

object UDFs {

  def mkString2(xml:Seq[String]): Seq[String] = {
    xml
      .mkString(" ")
      .split("<data>")
      .filter(_.nonEmpty)
      .map("<data>"+_)
  }

  val mkString2UDF=udf(mkString2 _)

}
