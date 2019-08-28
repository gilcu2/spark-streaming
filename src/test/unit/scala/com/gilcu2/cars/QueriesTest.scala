package com.gilcu2.cars

import com.gilcu2.testUtil.SparkSessionTestWrapper
import org.scalatest.{FlatSpec, FunSuite, GivenWhenThen, Matchers}

class QueriesTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Queries"

  import spark.implicits._

  it should "return data frame with car event with high temperature in the last event" in {

    Given("Dataset[CarEvent] and alert temperature")
    val position = Position(0, 0)
    val carEvents = spark.createDataset(Seq(
      CarEvent(1, position, temperature = 60, time = 1),
      CarEvent(2, position, temperature = 40, time = 1),
      CarEvent(1, position, temperature = 40, time = 1002),
      CarEvent(2, position, temperature = 60, time = 10002)
    ))

    val alertTemperature = 50

    When("the alert temperature is computed")
    val carHighTemperature = Queries.lastStatusHighTemperature(carEvents, alertTemperature).collect()

    Then("it must return the CarEvent that that have high temperature in the last event")
    carHighTemperature.size shouldBe 2
    carHighTemperature.toSet shouldBe Array(
      CarEvent(1, position, temperature = 60, time = 1),
      CarEvent(2, position, temperature = 60, time = 10002)
    ).toSet
  }

}
