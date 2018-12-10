package com.Jerevon.offline

import java.text.SimpleDateFormat
import java.util.UUID

import com.Jerevon.datamodel.UserVisitAction
import com.Jerevon.offline.apps.{CategorySessionTopApp, ClickOrderPayTop10App, SessionExtractApp}
import com.Jerevon.offline.bean.{CategoryTopN, SessionInfo}
import com.Jerevon.offline.utils.SessionAccumulator
import com.Jerevon.util.{ConfigurationUtil, JdbcUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by Jerevon on 2018/12/9.
  */
object OfflineApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val accumulator = new SessionAccumulator()
    sparkSession.sparkContext.register(accumulator)
    val taskId: String = UUID.randomUUID().toString
    val conditionConfig: FileBasedConfiguration = ConfigurationUtil("conditions.properties").config
    val conditionJsonString: String = conditionConfig.getString("condition.params.json")

    ///////////////////////////////////////////////////////////////
    ////////////////////////////需求1//////////////////////////////
    ///////////////////////////////////////////////////////////////
    // 统计出符合筛选条件的session中，访问时长在小于10s含、10s以上各个范围内的session数量占比。
    // 访问步长在小于等于5，和大于5次的占比

    //1.筛选 要关联的用户 sql join user_info where condition => DF => RDD[UserVisitAction]
    val userActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession, conditionJsonString)
    //2.rdd => RDD[(sessionId, UserVisitAction)] => groupByKey => RDD[(sessionId, iterable[UserVisitAction])]
    val userSessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map(userAction => (userAction
      .session_id, userAction)).groupByKey()
    userSessionRDD.cache()
    //3.求 session总数量：
    val userSessionCount: Long = userSessionRDD.count()
    //遍历一下全部session，对每个session的类型进行判断，来进行分类的累加(累加器)
    //4.分类： 时长，把session里面的每个action进行遍历，取出最大时间和最小时间，求差得到时长，再判断时长是否大于10秒
    //步长： 计算下session中有多少个action，判断个数是否大于5
    userSessionRDD.foreach { case (_, actions) =>
      var maxActionTime: Long = -1L
      var minActionTime: Long = Long.MaxValue
      for (action <- actions) {
        val format = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")
        val actionTimeMillSec: Long = format.parse(action.action_time).getTime //获取时间的毫秒值
        maxActionTime = Math.max(maxActionTime, actionTimeMillSec)
        minActionTime = Math.min(minActionTime, actionTimeMillSec)
      }
      val visitTime: Long = maxActionTime - minActionTime
      if (visitTime > 10000) {
        accumulator.add("session_visitLength_gt_10_count")
      } else {
        accumulator.add("session_visitLength_le_10_count")
      }
      if (actions.size > 5) {
        accumulator.add("session_stepLength_gt_5_count")
      } else {
        accumulator.add("session_stepLength_le_5_count")
      }
    }
    //5.提取累加器中的值
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value
    //6.把累计值计算为比例
    val session_visitLength_gt_10_ratio: Double = Math.round(1000.0 * sessionCountMap
    ("session_visitLength_gt_10_count") /
      userSessionCount) / 10.0
    val session_visitLength_le_10_ratio: Double = Math.round(1000.0 * sessionCountMap
    ("session_visitLength_le_10_count") /
      userSessionCount) / 10.0
    val session_stepLength_gt_5_ratio: Double = Math.round(1000.0 * sessionCountMap("session_stepLength_gt_5_count") /
      userSessionCount) / 10.0
    val session_stepLength_le_5_ratio: Double = Math.round(1000.0 * sessionCountMap("session_stepLength_le_5_count") /
      userSessionCount) / 10.0
    val resultArray = Array(taskId, conditionJsonString, userSessionCount, session_visitLength_gt_10_ratio,
      session_visitLength_le_10_ratio, session_stepLength_gt_5_ratio, session_stepLength_le_5_ratio)
    //7.保存到MySQL中
    JdbcUtil.executeUpdate("insert into session_stat_info value (?,?,?,?,?,?,?)", resultArray)
    println("需求一执行完毕！")

    ///////////////////////////////////////////////////////////////
    ////////////////////////////需求2//////////////////////////////
    ///////////////////////////////////////////////////////////////
    // 按比例抽取session

    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExtract(userSessionCount, taskId, userSessionRDD)

    import sparkSession.implicits._
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    sessionExtractRDD.toDF.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", "random_session_info")
      .mode(SaveMode.Append).save()

    println("需求二执行完毕！")

    ///////////////////////////////////////////////////////////////
    ////////////////////////////需求3//////////////////////////////
    ///////////////////////////////////////////////////////////////
    //根据点击、下单、支付进行排序，取前十名

    val categoryTop10: List[CategoryTopN] = ClickOrderPayTop10App.sortByCOPTop10(sparkSession, taskId, userActionRDD)
    println("需求三执行完毕！")

    ///////////////////////////////////////////////////////////////
    ////////////////////////////需求4//////////////////////////////
    ///////////////////////////////////////////////////////////////
    //Top10 热门品类中 Top10 活跃 Session 统计(按照点击、下单、支付的次序进行排序获取点击热点Top10)

    CategorySessionTopApp.statCategorySession(config, sparkSession, taskId, userActionRDD, categoryTop10)
    println("需求四执行完毕！")

  }

  def readUserVisitActionRDD(sparkSession: SparkSession, conditionJsonString: String): RDD[UserVisitAction] = {
    //1.查库 2.条件
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")
    sparkSession.sql("use " + databaseName)

    val jSONObject: JSONObject = JSON.parseObject(conditionJsonString)
    jSONObject.getString("startDate")

    //sql  //left join => 翻译  inner join => 过滤
    val sql = new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id = u.user_id where " +
      "1=1")

    if (jSONObject.getString("startDate") != null) {
      sql.append(" and date >= '" + jSONObject.getString("startDate") + "'")
    }
    if (jSONObject.getString("endDate") != null) {
      sql.append(" and date <= '" + jSONObject.getString("endDate") + "'")
    }
    if (jSONObject.getString("startAge") != null) {
      sql.append(" and u.age >=" + jSONObject.getString("startAge"))
    }
    if (jSONObject.getString("endAge") != null) {
      sql.append(" and  u.age<=" + jSONObject.getString("endAge"))
    }
    if (!jSONObject.getString("professionals").isEmpty) {
      sql.append(" and  u.professional in (" + jSONObject.getString("professionals") + ")")
    }

    //println(sql)

    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

    rdd
  }
}
