package com.Jerevon.offline.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by Jerevon on 2018/12/10.
  */
class CategoryActionCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var categoryActionCountMap = new mutable.HashMap[String, Long]

  //判断是否是初始值
  override def isZero: Boolean = {
    categoryActionCountMap.isEmpty
  }

  //制作拷贝
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryActionCountAccumulator()
    accumulator.categoryActionCountMap ++= categoryActionCountMap
    accumulator
  }

  //清空
  override def reset(): Unit = {
    categoryActionCountMap = new mutable.HashMap[String, Long]()
  }

  //累加
  override def add(key: String): Unit = {
    categoryActionCountMap(key) = categoryActionCountMap.getOrElse(key, 0L) + 1L
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    categoryActionCountMap = categoryActionCountMap.foldLeft(otherMap) { case (_, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count
      otherMap
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    categoryActionCountMap
  }
}
