package org.company.temperature.StructuredStreaming

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.company.temperature.AppConfig
import org.company.temperature.AppSparkConf.spark
import org.company.temperature.DataModels.{Population, PopulationData}

object RunPopulationStructuredStream extends App {

  import spark.implicits._

  val populationSchema = new StructType()
    .add("city", "string")
    .add("country", "string")
    .add("population_M", "double")
    .add("updated_at_ts", "timestamp")

  val streamSources = List(AppConfig.populationStream)
  val createPopulationStream = (path: String) => spark
    .readStream
    .format("json")
    .option("maxFilesPerTrigger", 10)
    .schema(populationSchema)
    .load(path)

  val consumer: DataFrame = streamSources.map(createPopulationStream).reduce(_ union _)

  val cleanUpPopulation = (population: Population) => {
    Population(
      population.city.toLowerCase.capitalize,
      population.country.toLowerCase.capitalize,
      population.population_M ,
      population.updated_at_ts
    )
  }
  val processor: Dataset[PopulationData] = consumer.as[PopulationData].map(cleanUpPopulation)

  val producerParquet = processor
    .writeStream
    .format("parquet")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("path", AppConfig.temperaturePath)
    .option("checkpointLocation", AppConfig.checkpointLocation)
    .outputMode(Append)

  val producerConsole = processor.writeStream
    .format("console")
    .option("truncate", value = false)
    .option("numRows", 200)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .outputMode(Append) // <-- update output mode

  producerConsole.start.awaitTermination
  //  producerParquet.start.awaitTermination


  println("TERMINATE!")
}
