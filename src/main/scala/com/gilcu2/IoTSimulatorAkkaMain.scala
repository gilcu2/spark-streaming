package com.gilcu2

import akka.actor.{ActorSystem, Props}

import scala.concurrent.duration._
import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait, Time}
import com.gilcu2.simulator.IoTSimulator
import com.typesafe.config.Config
import org.rogach.scallop.ScallopConf

import scala.concurrent.Await


object IoTSimulatorAkkaMain extends MainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val system = ActorSystem("IoTSimulator")

    val simulator = system.actorOf(Props(new IoTSimulator(nDevices = 3, topic = "cars")),
      name = "simulator")

    system.scheduler.schedule(0 seconds, 10 seconds)(
      simulator ! Time.getCurrentTime
    )

    Await.ready(system.whenTerminated, 365.days)
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
