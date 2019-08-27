//package com.gilcu2
//
//import akka.actor.{Actor, ActorSystem, Props}
//import akka.event.Logging
//
//import scala.concurrent.duration._
//import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait, Time}
//import com.typesafe.config.Config
//import org.apache.spark.sql.SparkSession
//import org.joda.time.DateTime
//import org.rogach.scallop.ScallopConf
//
//import scala.concurrent.Await
//import scala.util.Random
//
//import java.util.Properties
//import org.apache.kafka.clients.producer._
//
//class LOTSimulator(val nDevices:Int,val topic:String) extends Actor {
//  val log = Logging(context.system, this)
//
//  val props = new Properties()
//  props.put("bootstrap.servers", "localhost:9092")
//  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//  val producer = new KafkaProducer[String, String](props)
//
//  def receive = {
//    case time:DateTime => (1 to nDevices).foreach(id=>log.info(s"Event to $id ${time.getMillis}"))
//    case msg      => log.info(s"received unknown message $msg")
//  }
//
//  def sendAkkaMsg(device:Int,time:Long): Unit = {
//    val record = new ProducerRecord[String, String]("lot",  value=s"msg from $device")
//    producer.send(record)
//
//
//  }
//}
//
//object LOTSimulatorMain extends MainTrait {
//
//  def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait): Unit = {
//    import scala.concurrent.ExecutionContext.Implicits.global
//
//    val system = ActorSystem("LOTSimulator")
//
//    val simulator = system.actorOf(Props(new LOTSimulator(nDevices = 3,topic="lot")),
//      name="simulator")
//
//    system.scheduler.schedule(0 seconds, 10 seconds)(
//      simulator ! Time.getCurrentTime
//    )
//
//    Await.ready(system.whenTerminated, 365.days)
//  }
//
//  def getConfigValues(conf: Config): ConfigValuesTrait = {
//    //    val dataDir = conf.getString("DataDir")
//    val dataDir = "kk"
//    ConfigValues(dataDir)
//  }
//
//  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait = {
//
//    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
//    parsedArgs.verify
//
//    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
//    //    val inputName = parsedArgs.inputName()
//
//    CommandParameterValues(logCountsAndTimes)
//  }
//
//  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
//    val logCountsAndTimes = opt[Boolean]()
//    //    val inputName = trailArg[String]()
//
//  }
//
//  case class CommandParameterValues(logCountsAndTimes: Boolean) extends LineArgumentValuesTrait
//
//  case class ConfigValues(dataDir: String) extends ConfigValuesTrait
//
//
//}
