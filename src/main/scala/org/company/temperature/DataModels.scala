package org.company.temperature

import java.sql.Timestamp
import java.text.SimpleDateFormat

object DataModels {
  type LocationMeasurement = CityTemperature

  val cityCountryMap = CityCountryMap.apply()

  case class Measurement(city: String, celsius: Option[Double], fahrenheit: Option[Double], measured_at_ts: Timestamp)


  case class MeasurementWithCountry(country: Option[String], city: String
                                    , celsius: Option[Double], fahrenheit: Option[Double]
                                    , measured_at_ts: Timestamp)
  object MeasurementWithCountry {
    def apply(measurement: Measurement): MeasurementWithCountry = {
      val city = measurement.city.toLowerCase.capitalize
      val country: Option[String] = cityCountryMap.get(city)
      MeasurementWithCountry(country, city, measurement.fahrenheit, measurement.celsius, measurement.measured_at_ts)
    }
  }

  case class CityTemperature(date:String
                             , country: Option[String], city: String
                             , celsius: Option[Double], fahrenheit: Option[Double]
                             , measured_at_ts: Timestamp)
  object CityTemperature {
    def apply(measurement: MeasurementWithCountry): CityTemperature = {
      val date: String = new SimpleDateFormat(AppConfig.outDateFormat).format(measurement.measured_at_ts)
      CityTemperature(date, measurement.country, measurement.city
        , measurement.fahrenheit, measurement.celsius
        , measurement.measured_at_ts)
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