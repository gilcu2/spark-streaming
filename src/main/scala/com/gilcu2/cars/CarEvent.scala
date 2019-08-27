package com.gilcu2.cars

import com.gilcu2.interfaces.Time

import scala.util.Random.nextInt

case class CarEvent(id: Int, position: Position = Position.random(50), temperature: Int = nextInt(100),
                    time: Long = Time.getCurrentTime.getMillis)