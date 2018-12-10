package com.Jerevon.offline.bean

/**
  * Created by Jerevon on 2018/12/10.
  */
case class SessionInfo(taskId: String, sessionId: String, startTime: String,
                       stepLength: Long, visitLength: Long, searchKeyWords: String,
                       clickProductIds: String, orderProductIds: String,
                       payProductIds: String) {}
