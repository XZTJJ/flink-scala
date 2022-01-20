package com.zhouhc.day5

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//使用两天条，进行 intervalJoin， 两条流之间的元素差10min中
object IntervalJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //添加source
    val clickStream = env.fromElements(("1", "click", 2500 * 1000L), ("1", "click", 3000 * 1000L)
      , ("1", "click", 3300 * 1000L), ("1", "click", 3600 * 1000L)).assignAscendingTimestamps(_._3).keyBy(_._1)
    val browseStream = env.fromElements(("1", "browse", 1500 * 1000L), ("1", "browse", 2800 * 1000L)
      , ("1", "browse", 3300 * 1000L), ("1", "browse", 3400 * 1000L)).assignAscendingTimestamps(_._3).keyBy(_._1)

    //按照道理，如果是 join为10 min，应该有5条结果才对 , interval join, 然后 进行窗口定义，最后为匹配到的元素进行处理
    clickStream.intervalJoin(browseStream)
      .between(Time.seconds(-600), Time.seconds(0))
      .process(new MyIntervalJoin)
      .print()

    env.execute("IntervalJoin")
  }

  //定义匹配到的元素的处理逻辑
  class MyIntervalJoin extends ProcessJoinFunction[(String, String, Long), (String, String, Long), String] {
    //只是简单的打印一些处理的逻辑
    override def processElement(left: (String, String, Long), right: (String, String, Long), ctx: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect(s"""${left} --------> ${right}""")
    }
  }

}
