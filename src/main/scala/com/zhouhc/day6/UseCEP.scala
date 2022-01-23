package com.zhouhc.day6

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//CEP 使用处理流中复杂的数据而产生的，更高级别的抽象，通过将规则应用到流中，来匹配对应的数据
object UseCEP {
  //事件类
  case class UserLogin(userName: String, LoginStatus: String, ipAddr: String, loginTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //抽取时间，分流
    val stream = env.fromElements(UserLogin("user1", "fail", "0.0.0.1", 1000L), UserLogin("user1", "fail", "0.0.0.2", 5000L)
      , UserLogin("user1", "fail", "0.0.0.3", 8000L), UserLogin("user2", "success", "0.0.0.1", 9000L), UserLogin("user1", "success", "0.0.0.1", 10000L), UserLogin("user1", "fail", "0.0.0.4", 11000L))
      .assignAscendingTimestamps(_.loginTime).keyBy(_.userName)
    //需要应用的模式
    val pattern = Pattern.begin[UserLogin]("firstLoginFail")
      .where(_.LoginStatus.equalsIgnoreCase("fail"))
      .next("secondLoginFail")
      .where(_.LoginStatus.equalsIgnoreCase("fail"))
      .next("thirdLoginFail")
      .where(_.LoginStatus.equalsIgnoreCase("fail"))
      .within(Time.seconds(10))

    CEP.pattern(stream, pattern).select((pattern: scala.collection.Map[String, Iterable[UserLogin]]) => {
      val first = pattern.getOrElse("firstLoginFail", null).iterator.next()
      val second = pattern.getOrElse("secondLoginFail", null).iterator.next()
      val third = pattern.getOrElse("thirdLoginFail", null).iterator.next()
      (first.userName, first.ipAddr, second.ipAddr, third.ipAddr)
    }).print()

    env.execute("UseCEP")

  }
}
