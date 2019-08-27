package com.gilcu2.cars

import scala.util.Random

object Position {

  def random(size: Int): Position = Position(Random.nextInt(size), Random.nextInt(size))

}

case class Position(x: Int, y: Int) {

  def +(p: Position): Position = Position(this.x + p.x, this.y + p.y)

}
