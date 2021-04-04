package org.company.temperature.DStreaming

import DStreamContext.ssc

object RunApp extends App {
  val populationStream = PopulationDStream.populationStream
  //  populationStream.print(5)

  val cityTemperatureStream = CityTemperatureStream.cityTemperatureStream
  //  cityTemperatureStream.print(5)

  LatestMeasurementStream(cityTemperatureStream, populationStream)

  ssc.start()
  ssc.awaitTermination()

  println("TERMINATE!")
}
