package com.zhouhc.day3

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

//自己实现对应的周期性水印处理逻辑， 其实就是  EventTumblingCountElem2 的自己实现， 不过水印生成周期可以通过系统配置的
//每一次周期介绍后，会触发一次水印构建逻辑
object GenPeriodWaterMark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义一个函数
    val stream = env.socketTextStream("localhost", 9999, '\n');
    stream.map(
      //将输入数据拆分长两个部分, 设置水印, 通过将第一个字段去掉, window为5s的固定窗口， process设置full窗口
      line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      }
    ).assignTimestampsAndWatermarks(new MyWaterMarkGen).keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new FullProcessWindow).print()

    env.execute("EventTumblingCountElem")
  }


  //全局窗口统计,所有的元素的总数
  class FullProcessWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(s"""${new Timestamp(context.window.getStart)} ~ ${new Timestamp(context.window.getEnd)} 的窗口内元素总数为 : ${elements.size}""")
    }
  }

  //自定义生成水印的逻辑, flink自带的也是实现这个类的
  class MyWaterMarkGen extends AssignerWithPeriodicWatermarks[(String, Long)] {
    val bound = 5000;
    var MaxTimpstamps = Long.MinValue + bound + 1;

    override def getCurrentWatermark: Watermark = new Watermark(MaxTimpstamps - bound - 1)

    override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
      MaxTimpstamps = math.max(element._2, MaxTimpstamps);
      element._2
    }
  }


}
