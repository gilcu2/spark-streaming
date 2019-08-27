package com.gilcu2

import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, SparkMainTrait}
import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SparkSession}
import org.rogach.scallop.ScallopConf

object IoTSimulatorStreamingMain extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val rates = spark
      .readStream
      .format("rate") // <-- use RateStreamSource
      .option("rowsPerSecond", 1)
      .load

    val lot = rates.flatMap(row => Array("msg dev1", "msg dev2"))

    //    val df = spark
    //      .readStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("subscribe", "test")
    //      .load()

    val query = lot.writeStream
      .outputMode("append")
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
