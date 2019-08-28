package com.gilcu2

import com.gilcu2.cars.{CarEvent, Position, Queries}
import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, SparkMainTrait}
import com.typesafe.config.Config
import io.circe.generic.auto._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf

object IoTProcessingKafkaStreamingMain extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cars")
      .load()
    val lines = df.selectExpr("CAST(value AS STRING)")
      .as[String]

    val carEvents = lines
      .map(line =>
        io.circe.parser.decode[CarEvent](line)
          .right.getOrElse(CarEvent(0, Position(0, 0), 0, 0))
      )
      .filter(_.id == 0)

    val alarmHighTemperature = Queries.lastStatusHighTemperature(carEvents, alertTemperature = 50)

    val query = alarmHighTemperature.writeStream
      //      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

  def getConfigValues(conf: Config): ConfigValuesTrait = {
    //    val dataDir = conf.getString("DataDir")
    val dataDir = "kk"
    ConfigValues(dataDir)
  }

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    //    val inputName = parsedArgs.inputName()

    CommandParameterValues(logCountsAndTimes)
  }

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    //    val inputName = trailArg[String]()

  }

  case class CommandParameterValues(logCountsAndTimes: Boolean) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String) extends ConfigValuesTrait


}
