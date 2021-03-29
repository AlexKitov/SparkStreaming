package org.company.temperature

import java.sql.Timestamp

object DataModels {
  type LocationMeasurement = MeasurementWithCountry

  val cityCountryMap = CityCountryMap.apply()

  case class Measurement(city: String, celsius: Option[Double], fahrenheit: Option[Double], measured_at_ts: Timestamp)


  case class MeasurementWithCountry(country: Option[String], city: String, celsius: Option[Double], fahrenheit: Option[Double], measured_at_ts: Timestamp)
  object MeasurementWithCountry {
    def apply(measurement: Measurement): MeasurementWithCountry = {
      val city = measurement.city.toLowerCase.capitalize
      val country: Option[String] = cityCountryMap.get(city)
      MeasurementWithCountry(country, city, measurement.fahrenheit, measurement.celsius, measurement.measured_at_ts)
    }
  }

  object CityCountryMap {
    def apply(): Map[String, String] = {
      Map(
        "Copenhagen" ->  "Denmark",
        "London" ->  "United Kingdom"
      )
    }
  }

}