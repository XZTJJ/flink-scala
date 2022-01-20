package com.zhouhc.day4

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//将迟到数据 直接重定向到 侧边输出中，不过这里使用的自定义的方式，而且数据不能乱序,一定要指定事件时间
object LateDateToSideOutPutCus {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //操作stream,这里直接将迟到元素输出到侧边流中去了
    val stream = env.socketTextStream("localhost", 9999, '\n').map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    }).assignAscendingTimestamps(_._2).process(new MyProcessFunction)
    //迟到元素和正常元素都打印
    stream.getSideOutput(new OutputTag[String]("late-readings")).print()
    stream.print()

    env.execute("LateDateToSideOutPutCus")
  }

  //处理正常的元素
  class MyProcessFunction extends ProcessFunction[(String, Long), (String, Long)] {
    //侧边输出的标签
    lazy val lateData = new OutputTag[String]("late-readings")

    //每个元素处理一次
    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(lateData, s"""迟到元素信息为(${value._1},${value._2})""")
      } else {
        out.collect(value)
      }
    }
  }
}
