package org.company.temperature

import org.company.temperature.Utils.strToTimestamp

import java.sql.Timestamp
import java.text.SimpleDateFormat

object DataModels {
  type LocationMeasurement = MeasurementWithCountryAndPopulation
  type PopulationData = Population

  val cityCountryMap = CityCountryMap.apply()

  case class Measurement(city: String, celsius: Option[Double], fahrenheit: Option[Double], measured_at_ts: Timestamp)
  case class Population(city:String,country:String,population_M:Double,updated_at_ts:String)

  case class SCDT2Temperature(city: String,
                              measured_at_ts: Timestamp,
                              celsius: Double,
                              fahrenheit: Double,
                              population_M: Option[Double],
                              active_from: Timestamp,
                              active_to: Option[Timestamp],
                              active_flag: Boolean)
  object SCDT2Temperature {
    def apply(o: MeasurementWithCountryAndPopulation): SCDT2Temperature = {
      SCDT2Temperature(
        o.city,
        o.measured_at_ts,
        o.celsius.get, // TODO handle unsafe get
        o.fahrenheit.get, // TODO handle unsafe get
        o.population_M,
        o.measured_at_ts,
        None,
        true
      )
    }
    def apply(o: MeasurementWithCountryAndPopulation, expired_at: Timestamp): SCDT2Temperature = {
      SCDT2Temperature(
        o.city,
        o.measured_at_ts,
        o.celsius.get, // TODO handle unsafe get
        o.fahrenheit.get, // TODO handle unsafe get
        o.population_M,
        o.measured_at_ts,
        Some(expired_at),
        false
      )
    }
    def apply(o: SCDT2Temperature, expired_at: Timestamp): SCDT2Temperature = {
      SCDT2Temperature(
        o.city,
        o.measured_at_ts,
        o.celsius,
        o.fahrenheit,
        o.population_M,
        o.measured_at_ts,
        Some(expired_at),
        false
      )
    }
  }

  case class PopulationMeasurementKey(city: String, date: Timestamp)
  object PopulationMeasurementKey{
    def apply(city: String, date: String): PopulationMeasurementKey = {
      val timestamp = strToTimestamp(date, AppConfig.jsonDateFormat)
      PopulationMeasurementKey(city.toLowerCase.capitalize, timestamp)
    }
  }

  case class MeasurementWithCountry(country: Option[String], city: String
                                    , celsius: Option[Double], fahrenheit: Option[Double]
                                    , measured_at_ts: Timestamp)
  object MeasurementWithCountry {
    def apply(measurement: Measurement): MeasurementWithCountry = {
      val city = measurement.city.toLowerCase.capitalize
      val country: Option[String] = cityCountryMap.get(city)
      MeasurementWithCountry(country, city, measurement.celsius, measurement.fahrenheit, measurement.measured_at_ts)
    }
  }

  case class MeasurementWithCountryAndPopulation(country: Option[String], city: String
                                    , celsius: Option[Double]
                                    , fahrenheit: Option[Double]
                                    , population_M: Option[Double]

//                                    , population_M: Double
                                    , measured_at_ts: Timestamp)
  object MeasurementWithCountryAndPopulation {
    def apply(measurement: CityTemperature, population: Option[Population]): MeasurementWithCountryAndPopulation = {
      MeasurementWithCountryAndPopulation(
        measurement.country,
        measurement.city,
        measurement.celsius,
        measurement.fahrenheit,
        population.map(_.population_M),
//        population.population_M,
        measurement.measured_at_ts)
    }
  }

  case class CityTemperature(date:String
                             , country: Option[String], city: String
                             , celsius: Option[Double], fahrenheit: Option[Double]
                             , measured_at_ts: Timestamp)
  object CityTemperature {
    def apply(measurement: MeasurementWithCountry): CityTemperature = {
      val date: String = new SimpleDateFormat(AppConfig.outDateFormat).format(measurement.measured_at_ts)
      CityTemperature(date
        , measurement.country
        , measurement.city
        , measurement.celsius
        , measurement.fahrenheit
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