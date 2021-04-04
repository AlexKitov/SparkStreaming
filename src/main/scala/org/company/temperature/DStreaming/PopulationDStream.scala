package org.company.temperature.DStreaming

import org.apache.spark.streaming.dstream.DStream
import org.company.temperature.AppConfig
import org.company.temperature.DStreaming.DStreamContext. unionSourceConsumers
import org.company.temperature.DataModels.Population
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object PopulationDStream{

  implicit val formats: DefaultFormats.type = DefaultFormats

  private val populationConsumer = unionSourceConsumers(List(AppConfig.populationStream))

  val populationStream: DStream[Population] = populationConsumer
    .map(jsonStr => parse(jsonStr).extract[Population])
    .cache
}
