package org.company.temperature.DStreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.company.temperature.AppConfig
import org.company.temperature.AppSparkConf.spark

object DStreamContext {

  val ssc = new StreamingContext(spark.sparkContext, Seconds(AppConfig.pollingInterval))
  ssc.checkpoint(AppConfig.checkpointLocation)

  def unionSourceConsumers(streamingSource: List[String])
  : DStream[String] = {
    streamingSource
      .map(ssc.textFileStream)
      .reduce(_ union _)
  }
}