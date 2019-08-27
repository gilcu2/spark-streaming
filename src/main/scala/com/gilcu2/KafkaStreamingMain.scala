package com.gilcu2

import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait, SparkMainTrait}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object KafkaStreamingMain extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
    val lines = df.selectExpr("CAST(value AS STRING)")
      .as[String]

    // Split the lines into words
    val words = lines.flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
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
