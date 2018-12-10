package com.Jerevon.randomUtils

import java.util.Date
import scala.util.Random

/**
  * Created by Jerevon on 2018/12/7.
  */
object RandomDate {

  def apply(startDate: Date, endDate: Date, step: Int): RandomDate = {
    val randomDate = new RandomDate()
    val avgStepTime = (endDate.getTime - startDate.getTime) / step
    randomDate.maxTimeStep = avgStepTime * 2
    randomDate.lastDateTime = startDate.getTime
    randomDate
  }


  class RandomDate {
    var lastDateTime = 0L
    var maxTimeStep = 0L

    def getRandomDate() = {
      val timeStep = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime + timeStep

      new Date(lastDateTime)
    }
  }

}

