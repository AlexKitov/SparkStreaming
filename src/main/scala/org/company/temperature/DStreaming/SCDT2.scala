package org.company.temperature.DStreaming

import org.company.temperature.AppSparkConf.spark
import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.DStream
import org.company.temperature.DataModels.{LocationMeasurement, SCDT2Temperature}

object SCDT2 {

  import spark.implicits._

  def process(stream: DStream[(String, LocationMeasurement)]): Unit = {
    val SCDT2 = stream
      .mapWithState(stateSpec)


    val expired = SCDT2
      .filter(filterOptionRows)
      .map(_.get)
      .cache

//    expired.print(100)

    val active = SCDT2
      .stateSnapshots()
      .map{case (location, locationMeasurement) => locationMeasurement}
      .cache

//    active.print(100)

    // TODO Think on the order of expire
    // e.g. if Copenhagen 23th, Copenhagen 24th, Copenhagen 25th come in the same batch
    // Copenhagen 23th, Copenhagen 24th could be both expired at 25th
    // Ideally 23th should be expired at 24th and 24th at 25th
    expired.union(active)
      .foreachRDD(
        rdd => {
          println("#### SCDT2 DATA TO UPDATE IN TABLE")
          println(rdd.isEmpty())
          rdd
            .toDS()
            .show
          // TODO Write to a table in update mode
          // This solution will constantly update active records
          // This is unnecessary in case no new data is coming
          // One solution is to setup timeout as for the LatestMEasurement
        }
      )
  }

  private def filterOptionRows(row: Option[SCDT2Temperature]): Boolean = {
    row match {
      case Some(scdt2) => !scdt2.active_flag
      case None => false
    }
  }

  private val stateSpec =
    StateSpec
      .function(handleMeasurements _)
      // TODO Set timeout to avoid unnecesary updates of active records in lack of new data
//      .timeout(Milliseconds(AppConfig.expireAfterMillis))

  private def handleMeasurements(keyLocation: String,
                                 temperatureMeasurement: Option[LocationMeasurement],
                                 state: State[SCDT2Temperature]): Option[SCDT2Temperature]
  = {
    // basically try to leave all expired in the stream
    // and all active in the state
    // by swapping them
    (temperatureMeasurement, state.getOption()) match {
      case (Some(newState), None) => {
        // the 1st visit
        val scdt2 = SCDT2Temperature(newState)
        state.update(scdt2)
        Some(scdt2)
      }
      case (Some(newTemperatureMeasurement), Some(latestState)) => {
        val (earlier, later) = orderEarlierLater(newTemperatureMeasurement, latestState)
        state.update(later)
        Some(earlier)
      }
      case _ => None
    }
  }

  private def orderEarlierLater(left: LocationMeasurement,
                                right: SCDT2Temperature): (SCDT2Temperature, SCDT2Temperature)
  = {
    if (left.measured_at_ts before right.measured_at_ts){
      val earlierExpired = SCDT2Temperature(left, right.measured_at_ts)
      val later = right
      (earlierExpired, later)
    }
    else {
      val earlierExpired = SCDT2Temperature(right, left.measured_at_ts)
      val later = SCDT2Temperature(left)
      (earlierExpired, later)
    }
  }
}
