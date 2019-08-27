package com.gilcu2.simulator

import akka.actor.Actor
import akka.event.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime
import com.gilcu2.cars.CarEvent
import com.gilcu2.cars.Position

import java.util.Properties
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

class IoTSimulator(val nDevices: Int, val topic: String) extends Actor {
  val log = Logging(context.system, this)

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  val lastPositions = scala.collection.mutable.HashMap[Int, Position]()

  def receive = {
    case time: DateTime =>
      log.info(s"Event  $time")
      (1 to nDevices).foreach(id => sendAkkaMsg(id, time.getMillis))
    case msg => log.info(s"received unknown message $msg")
  }

  def sendAkkaMsg(device: Int, time: Long): Unit = {
    val newPositions = lastPositions.getOrElse(device, Position(0, 0)) + Position.random(50)

    val carEvent = CarEvent(device, newPositions)
    val record = new ProducerRecord[String, String](topic, carEvent.asJson.noSpaces)
    producer.send(record)
  }
}
