package org.company.temperature.DStreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, State, StateSpec}
import org.company.temperature.AppConfig
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DataModels._

object LatestMeasurementStream {
  import spark.implicits._

  def latestMeasurement(temperatureStream: DStream[CityTemperature], populationStream: DStream[PopulationData])
  : DStream[(String, LocationMeasurement)] = {
    val popStream = populationStream
      .map(population => {
        val key = PopulationMeasurementKey(population.city.toLowerCase.capitalize, population.updated_at_ts)
        (key, population)
      })
      .cache

    val tempJoinPopulationStream =
      temperatureStream
        .map(ct => {
          val key = PopulationMeasurementKey(ct.city, ct.measured_at_ts)
          (key, ct)
        })
        .cache
        .leftOuterJoin(popStream)

    val tempWithCountryAndPopulation =
      tempJoinPopulationStream
        .map {
          case (_, valuePair) => {
            val (temperature, population) = valuePair
            MeasurementWithCountryAndPopulation(temperature, population)
          }
        }
        .map(measurement => (measurement.city, measurement))
        .cache

    val latestMeasurement =
      tempWithCountryAndPopulation
        .mapWithState(stateSpec)
        .stateSnapshots()
        .map{case (location, locationMeasurement) => locationMeasurement}
        .cache
    //      .updateStateByKey(updateStateFunction)

    latestMeasurement
      .foreachRDD(rdd => {
        println("#### LATEST DATA TABLE")
        rdd
          .toDS()
          .show
      })

    tempWithCountryAndPopulation
  }

  private val stateSpec =
    StateSpec
      .function(handleMeasurements _)
      .timeout(Milliseconds(AppConfig.expireAfterMillis))

  private def handleMeasurements(keyLocation: String,
                                 temperatureMeasurement: Option[LocationMeasurement],
                                 state: State[LocationMeasurement]): Option[LocationMeasurement]
  = {
    //    println(s"Handle measurement at '$keyLocation': '$temperatureMeasurement'")
    (temperatureMeasurement, state.getOption()) match {
      case (Some(newState), None) => {
        // the 1st visit
        state.update(newState)
        Some(newState)
      }
      case (Some(newTemperatureMeasurement), Some(latestState)) => {
        val newState = chooseLatestMeasurement(latestState, newTemperatureMeasurement)
        state.update(newState)
        Some(newState)
      }
      case _ if state.isTimingOut() => {
        //        println("######## timeout")
        None
      }
      case _ => None
    }
  }

  private def chooseLatestMeasurement(first: LocationMeasurement,
                                      second: LocationMeasurement): LocationMeasurement
  = {
    if (first.measured_at_ts before second.measured_at_ts) second
    else first
  }

  // ###############################################
  // ##### Old style updateStateByKey solution #####
  // ###############################################
  //
  //  private def updateStateFunction(newData: Seq[LocationMeasurement], state: Option[LocationMeasurement])
  //  : Option[LocationMeasurement] = {
  //
  //    //TODO Assumes there will be no 3 days old data arriving. Correct if not the case
  //    (newData, state) match {
  //      case (Seq(), state) => expireState(state)
  //      case (newData, None)  => Some(newData.reduce((t1, t2) => chooseLatestMeasurement(t1,t2)))
  //      case (newData, Some(state))  => Some((newData :+ state).reduce((t1, t2) => chooseLatestMeasurement(t1,t2)))
  //    }
  //  }
  //
  //  private def expireState(measurement: Option[LocationMeasurement]): Option[LocationMeasurement] = {
  //    println("Expire")
  //    val now = new DateTime()
  //    measurement.flatMap{temp =>
  //      if ((now.getMillis - temp.measured_at_ts.getTime) < AppConfig.expireAfterMillis)
  //        Some(temp)
  //      else
  //        None
  //    }
  //  }
  //  private def chooseLatestMeasurement(first: LocationMeasurement, second: LocationMeasurement) = {
  //    if (first.measured_at_ts before second.measured_at_ts)
  //      second
  //    else
  //      first
  //  }

}
