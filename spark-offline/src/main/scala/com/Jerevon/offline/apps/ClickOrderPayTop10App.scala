package com.Jerevon.offline.apps

import com.Jerevon.datamodel.UserVisitAction
import com.Jerevon.offline.bean.CategoryTopN
import com.Jerevon.offline.utils.CategoryActionCountAccumulator
import com.Jerevon.util.JdbcUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Jerevon on 2018/12/10.
  */
object ClickOrderPayTop10App {

  def sortByCOPTop10(sparkSession: SparkSession, taskId: String, userActionRDD: RDD[UserVisitAction]): List[CategoryTopN] = {

    //1.遍历所有的访问日志 map
    //2.按照cid + 操作类型进行分别累加  累加器 hashMap[String, Long]
    val categoryActionCountAccumulator = new CategoryActionCountAccumulator()
    sparkSession.sparkContext.register(categoryActionCountAccumulator)
    userActionRDD.foreach { userAction =>
      if (userAction.click_category_id != -1L) {
        //给当前品类的点击(商品浏览)项进行加1
        categoryActionCountAccumulator.add(userAction.click_category_id + "_click")
      } else if (userAction.order_category_ids != null && userAction.order_category_ids.nonEmpty) {
        //由于订单涉及多个品类，所以用逗号进行分割，循环进行累加
        val orderCidArr: Array[String] = userAction.order_category_ids.split(",")
        for (orderCid <- orderCidArr) {
          categoryActionCountAccumulator.add(orderCid + "_order")
        }
      } else if (userAction.pay_category_ids != null && userAction.pay_category_ids.nonEmpty) {
        //由于支付涉及多个品类，所以用逗号进行分割，循环进行累加
        val payCidArr: Array[String] = userAction.pay_category_ids.split(",")
        for (payCid <- payCidArr) {
          categoryActionCountAccumulator.add(payCid + "_pay")
        }
      }
    }

    //3.得到累加的结果 map[cid_actionType,count]
//    println(categoryActionCountAccumulator.value.mkString("\n"))
    val actionCountByCidMap: Map[String, mutable.HashMap[String, Long]] = categoryActionCountAccumulator.value
      .groupBy { case (cidAction, _) =>
        val cid: String = cidAction.split("_")(0)
        cid //用cid分组
      }

    //4.把结果转成 cid, clickCount, orderCount, payCount
    //CategoryTopN(click, hashMap.get(cid+"_click"), hashMap.get(cid+"_order"), hashMap.get(cid+"_pay"))
    val categoryTopNList: List[CategoryTopN] = actionCountByCidMap.map { case (cid, actionMap) =>
      CategoryTopN(taskId, cid, actionMap.getOrElse(cid + "_click", 0L),
        actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
    }.toList

    val categoryTop10: List[CategoryTopN] = categoryTopNList.sortWith((ctn1, ctn2) =>
      if (ctn1.click_count > ctn2.click_count) {
        true
      } else if (ctn1.click_count == ctn2.click_count) {
        if (ctn1.order_count > ctn2.order_count) {
          true
        } else if (ctn1.order_count == ctn2.order_count) {
          if (ctn1.pay_count > ctn1.pay_count) {
            true
          } else {
            false
          }
        } else {
          false
        }
      } else {
        false
      }
    ).take(10)

    //组合成jdbcUtil插入需要的结构
    val categoryList = new ListBuffer[Array[Any]]()
    for (categoryTopN <- categoryTop10) {
      val paramArray = Array(categoryTopN.taskId, categoryTopN.category_id, categoryTopN.click_count, categoryTopN
        .order_count, categoryTopN.pay_count)
      categoryList.append(paramArray)
    }
    //保存到数据库
    JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?) ", categoryList)

    categoryTop10
  }
}
