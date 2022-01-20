package com.zhouhc.day4

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 这里设置延迟更新数据， 水印延迟5s, 窗口大小为5s，窗口延迟销毁为5s
 */
//a 1
//a 2
//a 3
//a 4
//a 10
//a 1
//a 2
//a 15
//a 1
//注意只有15后才不会更新 0~5之间的窗口
object UpdateLateTime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //添加数据源
    val stream = env.socketTextStream("localhost", 9999, '\n').map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    })
      //添加 5的 水印延迟，设置 水印的时间为第二个字段
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
        override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
      }))
      //分流，开 5s 左右的窗口，同时窗口二外保留5s的数据
      .keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(5)).process(new MyFunction)

    stream.print()
    env.execute("UpdateLateTime")
  }

  //设置自定义的功能
  class MyFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      //获取串口的状态，如果关闭的话，就为false
      val updateValue = context.windowState.getState[Boolean](new ValueStateDescriptor[Boolean]("windowUpdate", Types.of[Boolean]))

      //处于关闭状态
      if (!updateValue.value()) {
        out.collect(s"""窗口关闭了，但是还没有销毁!!!!!, 窗口中现有元素为${elements.size}个""")
        //表示关闭窗口
        updateValue.update(true)
      } else {
        out.collect(s"""延迟数据到来了!窗口中现有元素为${elements.size}个""")
      }
    }
  }
}
