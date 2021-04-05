package org.company.temperature.DStreaming

import DStreamContext.ssc

object RunApp extends App {
  val populationStream =
    PopulationDStream
      .populationStream
  //  populationStream.print(5)

  val cityTemperatureStream =
    CityTemperatureStream
      .cityTemperatureStream
  //  cityTemperatureStream.print(5)

  val cityTemperatureJoinPopulationStream =
    LatestMeasurementStream
    .latestMeasurement(cityTemperatureStream, populationStream)

  SCDT2.process(cityTemperatureJoinPopulationStream)

  ssc.start()
  ssc.awaitTermination()

  println("TERMINATE!")
}
