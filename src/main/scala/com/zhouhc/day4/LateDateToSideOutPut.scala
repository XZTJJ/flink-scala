package com.zhouhc.day4

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//将迟到数据 直接重定向到 侧边输出中,  在同一个窗口中，这个程序是允许数据乱序的,
object LateDateToSideOutPut {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //操作stream,这里直接将迟到元素输出到侧边流中去了
    val stream = env.socketTextStream("localhost", 9999, '\n').map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    }).assignAscendingTimestamps(_._2).keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .sideOutputLateData(new OutputTag[(String, Long)]("late-readings")).process(new MyProcessFunction)
    //迟到元素和正常元素都打印
    stream.getSideOutput(new OutputTag[(String, Long)]("late-readings")).print()
    stream.print()

    env.execute("LateDateToSideOutPut")
  }

  //处理正常的元素
  class MyProcessFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(s"""窗口总共有${elements.size}个元素""")
    }
  }
}
