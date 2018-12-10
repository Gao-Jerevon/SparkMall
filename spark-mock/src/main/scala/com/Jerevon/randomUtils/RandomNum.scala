package com.Jerevon.randomUtils

import scala.collection.mutable
import scala.util.Random

/**
  * Created by Jerevon on 2018/12/7.
  */
object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割 canRepeat为false则不允许重复
    if (canRepeat) {
      val valueList = new mutable.ListBuffer[Int]()
      for (i <- 0 until amount) {
        val randomNum = apply(fromNum, toNum)
        valueList += randomNum
      }
      valueList.mkString(",")
    } else {
      val valueSet = new mutable.HashSet[Int]()
      while (valueSet.size < amount) {
        val randomNum = apply(fromNum, toNum)
        valueSet += randomNum
      }
      valueSet.mkString(",")
    }
  }
}

