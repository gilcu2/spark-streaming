package com.gilcu2

import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, SparkMainTrait, Time}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.rogach.scallop.ScallopConf
import com.gilcu2.cars.CarEvent

object IoTSimulatorStreamingMain extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val rates = spark
      .readStream
      .format("rate") // <-- use RateStreamSource
      .option("rowsPerSecond", 1)
      .load

    val carEvents = rates
      .flatMap(row => Array(CarEvent(1), CarEvent(2)))

    val preparedKafka = carEvents
      .select(carEvents.col("id").cast(DataTypes.StringType).as("value"))
    //
    //    preparedKafka.show

    val kafka = preparedKafka
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "cars")
      .start()

    //    val console = iot.writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()

    kafka.awaitTermination()

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
