package org.company.temperature

import org.company.temperature.DataModels.MeasurementWithCountry
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import java.sql.Timestamp

object Utils {

  def celsiusToFahrenheit(celsius: Option[Double]): Option[Double] = {
    celsius.map {
        c => {
          val fahrenheit = (c * 1.8) + 32
          BigDecimal(fahrenheit)
            .setScale(2, BigDecimal.RoundingMode.HALF_UP)
            .toDouble
        }
      }
  }

  def fahrenheitToCelsius(fahrenheit: Option[Double]): Option[Double] = {
    fahrenheit.map {
      f => {
        val celsius = (f - 32) / 1.8
        BigDecimal(celsius)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)
          .toDouble
      }
    }
  }
  def calculateNATemp(celsius: Option[Double], fahrenheit: Option[Double])
  : (Option[Double], Option[Double]) = {
    (celsius, fahrenheit) match {
      case (None, None) => (None, None)
      case (None, fahrenheit) => (fahrenheitToCelsius(fahrenheit), fahrenheit)
      case (celsius, None) => (celsius, celsiusToFahrenheit(celsius))
      case _ => (celsius, fahrenheit)
    }
  }

  def fillMissingTemperatures(measurement: MeasurementWithCountry)
  : MeasurementWithCountry = {
    val (celsius, fahrenheit) =  calculateNATemp(measurement.celsius, measurement.fahrenheit)
    MeasurementWithCountry(
      measurement.country,
      measurement.city,
      celsius,
      fahrenheit,
      measurement.measured_at_ts
    )
  }

  def strToTimestamp(dateTime: String, dateFormat: String)
  : Timestamp = {
    val dtf: DateTimeFormatter = DateTimeFormat.forPattern(dateFormat)
    val jodaTime: DateTime = dtf.parseDateTime(dateTime);
    new Timestamp(jodaTime.getMillis)
  }
}
