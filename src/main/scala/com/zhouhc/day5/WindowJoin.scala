package com.zhouhc.day5


import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

//两条流进行join，不同于interval join，这里是窗口与窗口的join，并且两条流开的窗口大写是一样的
//，窗口join窗口，就是窗口的笛卡尔积
object WindowJoin {
  //main方法
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1)

    //添加流
    val stream1 = env.fromElements(("a", 1000L), ("a", 2000L), ("b", 3000L), ("b", 4000L), ("b", 14000L)).assignAscendingTimestamps(_._2)
    var stream2 = env.fromElements(("a", 3000L), ("a", 4000L), ("b", 8000L), ("b", 9000L),("a", 14000L), ("b", 18000L), ("b", 29000L)).assignAscendingTimestamps(_._2)

    //开始join, wehre().equalTo类似于 on ， 那个窗口中的额元素聚合
    stream1.join(stream2).where(_._1).equalTo(_._1).window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(new MyJoinFunction).print()

    //
    env.execute("WindowJoin")
  }

  class MyJoinFunction extends JoinFunction[(String, Long), (String, Long), String] {

    override def join(first: (String, Long), second: (String, Long)): String = first + "------->" + second

  }
}
