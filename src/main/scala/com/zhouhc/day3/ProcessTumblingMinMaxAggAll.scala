package com.zhouhc.day3

import com.zhouhc.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//处理时间的 ，5s滚动窗口的 ，每个窗口温度的最大值和最小值，注意 AggregateFunction 只是保留累加器，非常节省空间
// 增量聚合函数 + 全局窗口函数，  这里的全局处理函数只是为了报一层数据
object ProcessTumblingMinMaxAggAll {

  case class MinMaxCase(id: String, minTemp: Double, maxTemp: Double, windowStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)

    stream.keyBy(_.id).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(new MyAGG, new FullOper).print()

    env.execute("ProcessTumblingMinMaxAggAll")
  }

  /**
   * AggregateFunction需要定义三种类型，一种是输入类型，一种累机器类型，一种是输出类型
   * 输入类型是 : SensorReading
   * 累加器类型: String是传感器id, 两个Double分别表示最小和最大温度
   * 输出类型 ：和传感器类型一致
   */
  class MyAGG extends AggregateFunction[SensorReading, (String, Double, Double), (String, Double, Double)] {
    //初始化累加器，注意最大和最小的温度初始值
    override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue)

    //累积状态，注意记录最大温度和最小问题
    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value.id, math.min(value.temperature, accumulator._2), math.max(value.temperature, accumulator._3))
    }

    //获取结果，累加器的结果就是最终的结果了
    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = (accumulator._1, accumulator._2, accumulator._3)

    //窗口合并
    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = (a._1, math.min(a._2, b._2), math.max(a._3, b._3))
  }

  //全窗口函数，这里的输入类型时，上面的聚合函数的输出类型，MinMaxCase 是最后的输出类型， String为key的类型， TimeWindow是Window的类型
  class FullOper extends ProcessWindowFunction[(String, Double, Double), MinMaxCase, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxCase]): Unit = {
      val value = elements.head;
      out.collect(MinMaxCase(value._1, value._2, value._3, context.window.getStart, context.window.getEnd))
    }
  }
}
