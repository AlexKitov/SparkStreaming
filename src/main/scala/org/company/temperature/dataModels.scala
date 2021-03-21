package org.company.temperature

import org.joda.time.DateTime

case class Measurement(city: String, celsius: Option[Double], fahrenheit: Option[Double], measured_at_ts: DateTime)

