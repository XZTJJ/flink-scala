package com.zhouhc.day3

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.{Date, Timestamp}
import java.time.Duration

//使用事件时间， 统计一个事件时间内窗口元素(5s的滚动窗口)的总量
//如果使用idea的话，需要在Setting 中的 Scala Compiler中的 Maven项目添加如下信息: -target:jvm-1.8
//a 1
//a 2
//a 3
//a 4
//a 5
//a 10
//a 13
//a 15
//a 20
//a 26
//a 36
//因为有 5s 的延迟， 水印时间戳 = 最大的事件时间 - 延迟时间 , 这里的时间窗口为 10s  窗口的开始时间计算逻辑 : timestamp - (timestamp - offset + windowSize) % windowSize
//记住 时间窗口是左闭右开的, 对于事件时间，如果没有冲洗trigger的话，只有 watermark >= 窗口的逻辑时间戳，才会触发窗口的计算
//所以 [0 ,10) 有 5个元素， [10~20)有3个元素 , [20~ 30) 只有2个元素， 36没有触发
object EventTumblingCountElem {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置水印，这里只是提取event时间字段，这里设置了最大的延迟时间为5s ，并且将元祖第二个元素作为时间戳
    val watermarkStrategy: WatermarkStrategy[(String, Long)] = WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5)).withTimestampAssigner(new TimestampAssigner);
    //定义一个函数
    val stream = env.socketTextStream("localhost", 9999, '\n');
    stream.map(
      //将输入数据拆分长两个部分, 设置水印, 通过将第一个字段去掉, window为5s的固定窗口， process设置full窗口
      line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      }
    ).assignTimestampsAndWatermarks(watermarkStrategy).keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new FullProcessWindow).print()

    env.execute("EventTumblingCountElem")
  }


  //全局窗口统计,所有的元素的总数
  class FullProcessWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(s"""${new Timestamp(context.window.getStart)} ~ ${new Timestamp(context.window.getEnd)} 的窗口内元素总数为 : ${elements.size}""")
    }
  }

  //通过设置时间戳字段，只能这么设置
  class TimestampAssigner extends SerializableTimestampAssigner[(String, Long)] {
    override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
  }
}
