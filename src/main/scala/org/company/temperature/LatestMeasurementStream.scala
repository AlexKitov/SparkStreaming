package org.company.temperature

import org.joda.time.DateTime
import org.apache.spark.streaming.dstream.DStream
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DataModels.LocationMeasurement

object LatestMeasurementStream {
  import spark.implicits._

  def apply(temperatureStream: DStream[LocationMeasurement]): Unit = {
    temperatureStream
      .map(measurement => (measurement.city, measurement))
      //      .mapWithState(StateSpec.function(handleMeasurements _).timeout(Durations.minutes(1)))
      .updateStateByKey(updateStateFunction)
      .foreachRDD(rdd => {
        println("##### LATEST")
        rdd
          .map{case (location, locationMeasurement) => locationMeasurement}
          .toDS()
          .persist
          .show
      })
  }

  private def updateStateFunction(newData: Seq[LocationMeasurement], state: Option[LocationMeasurement])
  : Option[LocationMeasurement] = {

    //TODO Assumes there will be no 3 days old data arriving. Correct if not the case
    (newData, state) match {
      case (Seq(), state) => expireState(state)
      case (newData, None)  => Some(newData.reduce((t1, t2) => chooseLatestMeasurement(t1,t2)))
      case (newData, Some(state))  => Some((newData :+ state).reduce((t1, t2) => chooseLatestMeasurement(t1,t2)))
    }
  }

  private def chooseLatestMeasurement(first: LocationMeasurement, second: LocationMeasurement) = {
    if (first.measured_at_ts before second.measured_at_ts)
      second
    else
      first
  }

  private def expireState(measurement: Option[LocationMeasurement]): Option[LocationMeasurement] = {
    println("Expire")
    val now = new DateTime()
    measurement.flatMap{temp =>
      if ((now.getMillis - temp.measured_at_ts.getTime) < AppConfig.expireAfterMillis)
        Some(temp)
      else
        None
    }
  }

  //  private def handleMeasurements(keyLocation: String,
  //                                 temperatureMeasurement: Option[LocationMeasurement],
  //                                 state: State[LocationMeasurement]): Option[LocationMeasurement]
  //  = {
  //    println(s"Handle measurement at '$keyLocation': '$temperatureMeasurement'")
  //
  //    (temperatureMeasurement, state.getOption()) match {
  //      case (Some(newTemperatureMeasurement), None) => {
  //        println(s"??? first state ")
  //        // the 1st visit
  //        state.update(newTemperatureMeasurement)
  //        Some(newTemperatureMeasurement)
  //        //        None
  //      }
  //      case (Some(newTemperatureMeasurement), Some(lastMeasurement)) => {
  //
  //        val updatedTemperatureMeasurement =
  //          if (lastMeasurement.measured_at_ts before newTemperatureMeasurement.measured_at_ts) {
  //            state.update(newTemperatureMeasurement)
  //            newTemperatureMeasurement
  //          } else {
  //            state.update(lastMeasurement)
  //            lastMeasurement
  //          }
  //        //TODO Should I update state if lastMeasurement is latest just to keep the timeout????
  //        println(s"UpdatedTemperaterue measurement: $updatedTemperatureMeasurement")
  //        Some(updatedTemperatureMeasurement)
  //        //        None
  //      }
  //      //      case (None, Some(lastMeasurement)) => {
  //      //        None
  //      //      }
  //      case _ => None
  //    }
  //  }

}
