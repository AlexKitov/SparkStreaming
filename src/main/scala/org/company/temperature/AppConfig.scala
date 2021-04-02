package org.company.temperature

import com.typesafe.config._

object AppConfig {
  val scalaEnv = System.getenv("SCALA_ENV")

  val appEnv = if (scalaEnv == null) "dev"
            else scalaEnv

  private val config = ConfigFactory.load()

  val configProvider = config.getConfig(appEnv)
                .withFallback(config.getConfig("default"))
  println(configProvider.isResolved)

  val logLevel = configProvider.getString("spark.log.level")
  println(logLevel)

  val master = configProvider.getString("spark.master")
  println(master)

  val appName = configProvider.getString("spark.app.name")
  println(appName)

  val pollingInterval = configProvider.getInt("spark.poll.interval")
  println(pollingInterval)

  val skipPattern = configProvider.getString("xml.skip.pattern")
  println(skipPattern)

  val dataPathString = configProvider.getString("hdfs.path.dataPath")
  println(dataPathString)

  val failedPath = configProvider.getString("hdfs.path.failPath")
  println(failedPath)

  val temperaturePath = configProvider.getString("hdfs.path.temperaturePath")
  println(temperaturePath)

  val dashboardPath = configProvider.getString("hdfs.path.dashboardPath")
  println(dashboardPath)

  val xmlDateFormat = configProvider.getString("xml.in.date.format")
  println(xmlDateFormat)

  val fileNameDateFormat = configProvider.getString("hdfs.path.fileNameDateFormat")
  println(fileNameDateFormat)

  val checkpointLocation = configProvider.getString("hdfs.path.checkpointLocation")
  println(checkpointLocation)

  val outDateFormat = configProvider.getString("out.date.format")
  println(outDateFormat)

  val expireAfterMillis = configProvider.getLong("out.expireAfterMillis ")
  println(expireAfterMillis)
}
