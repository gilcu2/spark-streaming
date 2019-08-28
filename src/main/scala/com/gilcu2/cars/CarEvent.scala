package com.gilcu2.cars

import com.gilcu2.interfaces.Time
import java.sql.Timestamp
import scala.util.Random.nextInt


case class CarEvent(id: Int, position: Position = Position.random(50), temperature: Int = nextInt(100),
                    time: Long = Time.getCurrentTime.getMillis)


case class CarEventStreaming(id: Int, position: Position, temperature: Int,
                             time: Long,
                             timeStamp: Timestamp)

object CarEventStreaming {

  def apply(carEvent: CarEvent): CarEventStreaming = new
      CarEventStreaming(carEvent.id, carEvent.position, carEvent.temperature, carEvent.time, new Timestamp(carEvent.time))
}