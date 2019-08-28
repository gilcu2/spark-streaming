package com.gilcu2.cars

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Queries {

  def lastStatusHighTemperature(carEvents: Dataset[CarEvent], alertTemperature: Int)(
    implicit spark: SparkSession): Dataset[CarEvent] = {
    import spark.implicits._

    val carEventsStreaming = carEvents.map(CarEventStreaming(_))

    val windowedEvents = carEventsStreaming.groupBy(
      window($"timeStamp", windowDuration = "10 seconds", slideDuration = "10 seconds"),
      $"id", $"temperature", $"position", $"time"
    ).max("time")

    val higherTemperature = windowedEvents.where($"temperature" > alertTemperature)
    higherTemperature.select("id", "position", "temperature", "time").as[CarEvent]
  }

}
