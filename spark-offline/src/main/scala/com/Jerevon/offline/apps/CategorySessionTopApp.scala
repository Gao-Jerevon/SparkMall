package com.Jerevon.offline.apps

import com.Jerevon.datamodel.UserVisitAction
import com.Jerevon.offline.bean.{CategorySessionTop, CategoryTopN}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Jerevon on 2018/12/10.
  */
object CategorySessionTopApp {

  def statCategorySession(config: FileBasedConfiguration, sparkSession: SparkSession,
                          taskId: String, userActionRDD: RDD[UserVisitAction],
                          categoryTop10List: List[CategoryTopN]): Unit = {
    val categoryTop10BC: Broadcast[List[CategoryTopN]] = sparkSession.sparkContext.broadcast(categoryTop10List)

    //1.过滤：过滤出所有排名前十品类的action=>RDD[UserVisitAction]
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter { userAction =>
      var matchflag = false
      for (categoryTop <- categoryTop10BC.value) {
        if (userAction.click_category_id.toString == categoryTop.category_id) matchflag = true
      }
      matchflag
    }

    //2.相同的cid + sessionId进行累加计数
    //RDD[userAction.clickCid+userAction.sessionId,1L]
    //reduceByKey(_+_)  =>RDD[cid_sessionId,count]
    val cidSessionIdCountRDD: RDD[(String, Long)] = filteredUserActionRDD.map { userAction =>
      (userAction.click_category_id + "_" + userAction.session_id, 1L)
    }.reduceByKey(_ + _)

    //3.根据cid进行聚合
    //RDD[cid_sessionId, count] => RDD[cid, (sessionId, count)]
    // => groupByKey => RDD[cid, Iterable[(sessionId, clickCount)]]
    val sessionCountByCidRDD: RDD[(String, Iterable[(String, Long)])] = cidSessionIdCountRDD.map { case
      (cid_sessionId, count) =>
      (cid_sessionId.split("_")(0), (cid_sessionId.split("_")(1), count))
    }.groupByKey()

    //4.聚合后进行排序、截取
    // => RDD[cid, Iterable[(sessionId, clickCount)]]
    // => 把iterable进行排序截取到前10
    // => RDD[cid, Iterable[(sessionId, clickCount)]]
    val categorySessionTopRDD: RDD[CategorySessionTop] = sessionCountByCidRDD.flatMap { case (cid, iterSessionCount) =>
      val sessionCountTop10: List[(String, Long)] = iterSessionCount.toList.sortWith { (sessionCount1, sessionCount2) =>
        sessionCount1._2 > sessionCount2._2
      }.take(10)
      val categorySessionTopList: List[CategorySessionTop] = sessionCountTop10.map { case (sessionId, count) =>
        CategorySessionTop(taskId, cid, sessionId, count)
      }
      categorySessionTopList
    }
//    println(categorySessionTopRDD.collect().mkString("\n"))

    // => RDD[CategorySessionTop] => 存数据库
    import sparkSession.implicits._
    categorySessionTopRDD.toDF.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", "category_top10_session_count")
      .mode(SaveMode.Append).save()
  }
}
