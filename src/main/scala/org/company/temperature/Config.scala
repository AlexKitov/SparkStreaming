package org.company.temperature

import com.typesafe.config._

object Config {
  val scalaEnv = System.getenv("SCALA_ENV")

  val env = if (scalaEnv == null) "dev"
            else scalaEnv

  val config = ConfigFactory.load()

  def apply() = config.getConfig(env)
                .withFallback(config.getConfig("default"))
}
