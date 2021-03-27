package org.company.temperature
import java.sql.Timestamp

case class Measurement(city: String, celsius: Option[Double], fahrenheit: Option[Double], measured_at_ts: Timestamp)
