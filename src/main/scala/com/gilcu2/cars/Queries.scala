package com.gilcu2.cars

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Queries {

  def lastStatusHighTemperature(carEvents: Dataset[CarEvent], alertTemperature: Int)(
    implicit spark: SparkSession): Dataset[CarEvent] = {
    import spark.implicits._

    val windowedEvents = carEvents.groupBy(
      window($"time", windowDuration = "10 seconds", slideDuration = "10 seconds"),
      $"id"
    ).max("time")
    windowedEvents.show()

    val higherTemperature = windowedEvents.where($"temperature" > alertTemperature)
    higherTemperature.select("id", "position", "temperature", "time").as[CarEvent]
  }

}
