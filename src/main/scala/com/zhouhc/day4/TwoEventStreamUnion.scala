package com.zhouhc.day4

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//使用两个自定义的数据源，这两个数据源的事件时间需要时升序的
//通过两个stream的 union的就可以观察 watermark 的传播方式了
//watermark 始终传播最小的那个时间戳， watermark一定是单调递增的
object TwoEventStreamUnion {
  def main(args: Array[String]): Unit = {
    //环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义一个输入源
    val stream1 = env.socketTextStream("localhost", 9999, '\n').map(
      //将输入数据拆分长两个部分, 设置水印, 通过将第一个字段去掉, window为5s的固定窗口， process设置full窗口
      line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      }
    ).assignAscendingTimestamps(_._2)
    //定义一个输入源
    val stream2 = env.socketTextStream("localhost", 9998, '\n').map(
      //将输入数据拆分长两个部分, 设置水印, 通过将第一个字段去掉, window为5s的固定窗口， process设置full窗口
      line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      }
    ).assignAscendingTimestamps(_._2)
    //union的 操作
    stream1.union(stream2).keyBy(_._1).process(new MyKeyProccess).print()
    //执行
    env.execute("TwoEventStreamUnion")
  }

  class MyKeyProccess extends KeyedProcessFunction[String, (String, Long), String] {
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect(s"""当前的watermark : ${ctx.timerService().currentWatermark()}""")
    }

  }
}
